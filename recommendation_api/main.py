from __future__ import annotations

import os
from contextlib import asynccontextmanager

import redis.asyncio as aioredis
from fastapi import FastAPI
from prometheus_client import make_asgi_app

from common.logging import setup_logging
from recommendation_api.middleware import TimingMiddleware
from recommendation_api.routes import router

setup_logging()

REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379/0")


@asynccontextmanager
async def lifespan(app: FastAPI):
    app.state.redis = aioredis.from_url(REDIS_URL, decode_responses=True)
    yield
    await app.state.redis.aclose()


app = FastAPI(title="Recommendation API", lifespan=lifespan)
app.add_middleware(TimingMiddleware)
app.include_router(router)

# Mount Prometheus /metrics endpoint
metrics_app = make_asgi_app()
app.mount("/metrics", metrics_app)


@app.get("/health")
async def health():
    return {"status": "ok", "service": "recommendation"}
