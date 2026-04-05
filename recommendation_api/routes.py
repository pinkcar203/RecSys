from __future__ import annotations

import json
import os
import time

import redis.asyncio as aioredis
import structlog
from fastapi import APIRouter, Request, Query
from prometheus_client import Counter, Histogram
from sqlalchemy import text

from common.db.engine import async_session_factory
from common.models.recommendations import RecommendationItem, RecommendationResponse
from recommendation_api.llm_reranker import rerank

log = structlog.get_logger()

CACHE_TTL = int(os.getenv("CACHE_TTL", "300"))  # 5 minutes
LLM_CACHE_TTL = int(os.getenv("LLM_CACHE_TTL", "120"))  # 2 minutes

# Prometheus metrics
RECS_SERVED = Counter(
    "recs_served_total",
    "Total recommendation requests served",
    ["source"],
)
RECS_LATENCY = Histogram(
    "recs_latency_seconds",
    "Recommendation request latency",
    buckets=[0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0],
)
CACHE_OPS = Counter(
    "recs_cache_operations_total",
    "Cache hit/miss counts",
    ["result"],
)

router = APIRouter()


async def _get_score_based_items(user_id: str, limit: int) -> list[RecommendationItem]:
    # Query PostgreSQL for top-N items by score
    async with async_session_factory() as session:
        result = await session.execute(
            text("""
                SELECT item_id, score
                FROM user_item_scores
                WHERE user_id = :user_id
                ORDER BY score DESC
                LIMIT :limit
            """),
            {"user_id": user_id, "limit": limit},
        )
        rows = result.fetchall()

    return [
        RecommendationItem(item_id=row.item_id, score=row.score, rank=idx + 1)
        for idx, row in enumerate(rows)
    ]


@router.get("/recommendations/{user_id}", response_model=RecommendationResponse)
async def get_recommendations(
    user_id: str,
    request: Request,
    limit: int = Query(default=10, ge=1, le=100),
    explain: bool = Query(default=False),
):
    start = time.perf_counter()
    redis_client: aioredis.Redis = request.app.state.redis

    if explain:
        # LLM path: check LLM cache first
        llm_cache_key = f"recs_llm:{user_id}"
        cached = await redis_client.get(llm_cache_key)
        if cached:
            log.info("llm_cache_hit", user_id=user_id)
            CACHE_OPS.labels(result="hit").inc()
            items_data = json.loads(cached)
            items = [RecommendationItem(**item) for item in items_data]
            RECS_SERVED.labels(source="llm_cache").inc()
            RECS_LATENCY.observe(time.perf_counter() - start)
            return RecommendationResponse(user_id=user_id, items=items, source="llm_cache")

        # Get score-based candidates, then re-rank with LLM
        log.info("llm_cache_miss", user_id=user_id)
        CACHE_OPS.labels(result="miss").inc()
        candidates = await _get_score_based_items(user_id, limit)
        if candidates:
            reranked = await rerank(candidates)
            items_json = json.dumps([item.model_dump() for item in reranked])
            await redis_client.setex(llm_cache_key, LLM_CACHE_TTL, items_json)
            RECS_SERVED.labels(source="llm").inc()
            RECS_LATENCY.observe(time.perf_counter() - start)
            return RecommendationResponse(user_id=user_id, items=reranked, source="llm")
        RECS_SERVED.labels(source="llm").inc()
        RECS_LATENCY.observe(time.perf_counter() - start)
        return RecommendationResponse(user_id=user_id, items=[], source="llm")

    else:
        # Score-based path: check score cache
        cache_key = f"recs:{user_id}"
        cached = await redis_client.get(cache_key)
        if cached:
            log.info("cache_hit", user_id=user_id)
            CACHE_OPS.labels(result="hit").inc()
            items_data = json.loads(cached)
            items = [RecommendationItem(**item) for item in items_data]
            RECS_SERVED.labels(source="cache").inc()
            RECS_LATENCY.observe(time.perf_counter() - start)
            return RecommendationResponse(user_id=user_id, items=items, source="cache")

        log.info("cache_miss", user_id=user_id)
        CACHE_OPS.labels(result="miss").inc()
        items = await _get_score_based_items(user_id, limit)

        if items:
            items_json = json.dumps([item.model_dump() for item in items])
            await redis_client.setex(cache_key, CACHE_TTL, items_json)
            log.info("cache_set", user_id=user_id, ttl=CACHE_TTL, items_count=len(items))

        RECS_SERVED.labels(source="score").inc()
        RECS_LATENCY.observe(time.perf_counter() - start)
        return RecommendationResponse(user_id=user_id, items=items, source="score")
