from __future__ import annotations

import os
import time
from contextlib import asynccontextmanager

from fastapi import FastAPI, Request
from prometheus_client import Counter, Histogram, make_asgi_app

from common.logging import setup_logging
from common.models.events import EVENT_WEIGHTS, UserEvent, UserEventMessage
from common.kafka.producer import KafkaProducerWrapper

setup_logging()

# Prometheus metrics
EVENTS_RECEIVED = Counter(
    "ingestion_events_received_total",
    "Total events received by the ingestion service",
    ["event_type"],
)
PUBLISH_LATENCY = Histogram(
    "ingestion_publish_latency_seconds",
    "Time to publish an event to Kafka",
    buckets=[0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0],
)


@asynccontextmanager
async def lifespan(app: FastAPI):
    bootstrap_servers = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
    producer = KafkaProducerWrapper(bootstrap_servers=bootstrap_servers)
    await producer.start()
    app.state.kafka_producer = producer
    yield
    await producer.stop()


app = FastAPI(title="Ingestion Service", lifespan=lifespan)

# Prometheus /metrics endpoint
metrics_app = make_asgi_app()
app.mount("/metrics", metrics_app)


@app.post("/events", status_code=202)
async def ingest_event(event: UserEvent, request: Request):
    weight = EVENT_WEIGHTS[event.event_type]
    message = UserEventMessage(
        user_id=event.user_id,
        item_id=event.item_id,
        event_type=event.event_type,
        weight=weight,
        timestamp=event.timestamp,
    )
    producer: KafkaProducerWrapper = request.app.state.kafka_producer

    start = time.perf_counter()
    await producer.send(
        topic="user-events",
        value=message.model_dump(),
        key=event.user_id,
    )
    PUBLISH_LATENCY.observe(time.perf_counter() - start)
    EVENTS_RECEIVED.labels(event_type=event.event_type.value).inc()

    return {"status": "accepted", "event_type": event.event_type.value}


@app.get("/health")
async def health():
    return {"status": "ok", "service": "ingestion"}
