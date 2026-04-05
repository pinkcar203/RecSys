from __future__ import annotations

import asyncio
import os
import json
import time

import redis.asyncio as aioredis
import structlog
from aiokafka import AIOKafkaConsumer
from prometheus_client import Counter, Histogram, start_http_server

from common.db.engine import async_session_factory
from common.db.queries import upsert_user_item_score
from common.logging import setup_logging

log = structlog.get_logger()

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379/0")
TOPIC = "user-events"
GROUP_ID = "score-aggregator"
METRICS_PORT = int(os.getenv("CONSUMER_METRICS_PORT", "8003"))

# Prometheus metrics
CONSUMED_TOTAL = Counter(
    "consumer_messages_consumed_total",
    "Total messages consumed from Kafka",
    ["event_type"],
)
PROCESSING_LATENCY = Histogram(
    "consumer_processing_latency_seconds",
    "Time to process a single message (upsert + cache invalidation)",
    buckets=[0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0],
)
PROCESSING_ERRORS = Counter(
    "consumer_processing_errors_total",
    "Total message processing errors",
)


async def invalidate_cache(redis_client: aioredis.Redis, user_id: str) -> None:
    # Delete cached recommendations for a user. Best-effort -- never crash.
    try:
        await redis_client.delete(f"recs:{user_id}", f"recs_llm:{user_id}")
        log.info("cache_invalidated", user_id=user_id)
    except Exception as exc:
        log.warning("cache_invalidation_failed", user_id=user_id, error=str(exc))


async def process_message(message_value: dict, redis_client: aioredis.Redis) -> None:
    # Process a single Kafka message: upsert score and invalidate cache.
    user_id = message_value["user_id"]
    item_id = message_value["item_id"]
    weight = message_value["weight"]

    start = time.perf_counter()

    async with async_session_factory() as session:
        await upsert_user_item_score(session, user_id, item_id, weight)

    log.info("score_updated", user_id=user_id, item_id=item_id, weight=weight)
    await invalidate_cache(redis_client, user_id)

    PROCESSING_LATENCY.observe(time.perf_counter() - start)
    CONSUMED_TOTAL.labels(event_type=message_value.get("event_type", "unknown")).inc()


def on_partitions_assigned(assigned):
    log.info("partitions_assigned", partitions=[str(tp) for tp in assigned])


def on_partitions_revoked(revoked):
    log.info("partitions_revoked", partitions=[str(tp) for tp in revoked])


async def consume() -> None:
    # Main consumer loop.
    setup_logging()

    # Prometheus metrics HTTP server on a separate port
    start_http_server(METRICS_PORT)
    log.info("metrics_server_started", port=METRICS_PORT)

    log.info("consumer_starting", topic=TOPIC, group_id=GROUP_ID)

    redis_client = aioredis.from_url(REDIS_URL, decode_responses=True)

    consumer = AIOKafkaConsumer(
        TOPIC,
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        group_id=GROUP_ID,
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
        auto_offset_reset="earliest",
        enable_auto_commit=False,
    )

    await consumer.start()
    log.info("consumer_started")

    try:
        async for msg in consumer:
            try:
                await process_message(msg.value, redis_client)
                await consumer.commit()
            except Exception as exc:
                PROCESSING_ERRORS.inc()
                log.error("message_processing_failed", error=str(exc), offset=msg.offset)
    finally:
        await consumer.stop()
        await redis_client.aclose()
        log.info("consumer_stopped")


if __name__ == "__main__":
    asyncio.run(consume())
