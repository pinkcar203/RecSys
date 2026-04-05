from __future__ import annotations

import json
import pytest
from unittest.mock import AsyncMock, patch, MagicMock
from httpx import AsyncClient, ASGITransport

from recommendation_api.main import app


@pytest.fixture
def mock_redis():
    r = AsyncMock()
    r.get = AsyncMock(return_value=None)
    r.setex = AsyncMock()
    r.aclose = AsyncMock()
    return r


@pytest.fixture(autouse=True)
def setup_redis(mock_redis):
    app.state.redis = mock_redis


# Test 1: Health check
@pytest.mark.asyncio
async def test_health_check():
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        response = await client.get("/health")
    assert response.status_code == 200
    assert response.json() == {"status": "ok", "service": "recommendation"}


# Test 2: Cache hit  returns cached data with source="cache"
@pytest.mark.asyncio
async def test_cache_hit(mock_redis):
    cached_items = [{"item_id": "i1", "score": 5.0, "rank": 1, "explanation": None}]
    mock_redis.get = AsyncMock(return_value=json.dumps(cached_items))

    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        response = await client.get("/recommendations/u1")

    assert response.status_code == 200
    data = response.json()
    assert data["user_id"] == "u1"
    assert data["source"] == "cache"
    assert len(data["items"]) == 1
    assert data["items"][0]["item_id"] == "i1"
    # Should NOT have queried DB or called setex
    mock_redis.setex.assert_not_awaited()


# Test 3: Cache miss  queries DB, caches result, returns source="score"
@pytest.mark.asyncio
async def test_cache_miss_queries_db(mock_redis):
    mock_redis.get = AsyncMock(return_value=None)

    # Mock the DB session to return some rows
    mock_row_1 = MagicMock()
    mock_row_1.item_id = "i1"
    mock_row_1.score = 5.0
    mock_row_2 = MagicMock()
    mock_row_2.item_id = "i2"
    mock_row_2.score = 3.0

    mock_result = MagicMock()
    mock_result.fetchall.return_value = [mock_row_1, mock_row_2]

    mock_session = AsyncMock()
    mock_session.execute = AsyncMock(return_value=mock_result)
    mock_session.__aenter__ = AsyncMock(return_value=mock_session)
    mock_session.__aexit__ = AsyncMock(return_value=False)

    with patch("recommendation_api.routes.async_session_factory", return_value=mock_session):
        transport = ASGITransport(app=app)
        async with AsyncClient(transport=transport, base_url="http://test") as client:
            response = await client.get("/recommendations/u1?limit=5")

    assert response.status_code == 200
    data = response.json()
    assert data["user_id"] == "u1"
    assert data["source"] == "score"
    assert len(data["items"]) == 2
    assert data["items"][0]["item_id"] == "i1"
    assert data["items"][0]["rank"] == 1
    assert data["items"][1]["item_id"] == "i2"
    assert data["items"][1]["rank"] == 2
    # Should have cached the result
    mock_redis.setex.assert_awaited_once()


# Test 4: Empty user  returns empty items list
@pytest.mark.asyncio
async def test_empty_user_returns_empty_list(mock_redis):
    mock_redis.get = AsyncMock(return_value=None)

    mock_result = MagicMock()
    mock_result.fetchall.return_value = []

    mock_session = AsyncMock()
    mock_session.execute = AsyncMock(return_value=mock_result)
    mock_session.__aenter__ = AsyncMock(return_value=mock_session)
    mock_session.__aexit__ = AsyncMock(return_value=False)

    with patch("recommendation_api.routes.async_session_factory", return_value=mock_session):
        transport = ASGITransport(app=app)
        async with AsyncClient(transport=transport, base_url="http://test") as client:
            response = await client.get("/recommendations/unknown_user")

    assert response.status_code == 200
    data = response.json()
    assert data["user_id"] == "unknown_user"
    assert data["items"] == []
    assert data["source"] == "score"
    # Should NOT cache empty results
    mock_redis.setex.assert_not_awaited()


# Test 5: X-Response-Time header is present
@pytest.mark.asyncio
async def test_response_time_header():
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        response = await client.get("/health")
    assert "x-response-time" in response.headers
    assert response.headers["x-response-time"].endswith("ms")
