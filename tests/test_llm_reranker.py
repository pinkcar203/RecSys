from __future__ import annotations

import json
import pytest
from unittest.mock import AsyncMock, MagicMock, patch
from httpx import AsyncClient, ASGITransport

from common.models.recommendations import RecommendationItem
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


def _mock_db_session(rows):
    """Helper to create a mock DB session returning given rows."""
    mock_result = MagicMock()
    mock_result.fetchall.return_value = rows
    mock_session = AsyncMock()
    mock_session.execute = AsyncMock(return_value=mock_result)
    mock_session.__aenter__ = AsyncMock(return_value=mock_session)
    mock_session.__aexit__ = AsyncMock(return_value=False)
    return mock_session


def _make_rows(*items):
    """Create mock DB rows: items are (item_id, score) tuples."""
    rows = []
    for item_id, score in items:
        row = MagicMock()
        row.item_id = item_id
        row.score = score
        rows.append(row)
    return rows


# Test 1: explain=false returns score-based (no LLM)
@pytest.mark.asyncio
async def test_explain_false_no_llm(mock_redis):
    mock_redis.get = AsyncMock(return_value=None)
    rows = _make_rows(("i1", 5.0), ("i2", 3.0))
    mock_session = _mock_db_session(rows)

    with patch("recommendation_api.routes.async_session_factory", return_value=mock_session):
        transport = ASGITransport(app=app)
        async with AsyncClient(transport=transport, base_url="http://test") as client:
            response = await client.get("/recommendations/u1?limit=5&explain=false")

    data = response.json()
    assert data["source"] == "score"
    assert data["items"][0]["item_id"] == "i1"


# Test 2: explain=true calls LLM reranker
@pytest.mark.asyncio
async def test_explain_true_calls_reranker(mock_redis):
    mock_redis.get = AsyncMock(return_value=None)
    rows = _make_rows(("i1", 5.0), ("i2", 3.0))
    mock_session = _mock_db_session(rows)

    reranked = [
        RecommendationItem(item_id="i2", score=3.0, rank=1, explanation="Highly relevant"),
        RecommendationItem(item_id="i1", score=5.0, rank=2, explanation="Also good"),
    ]

    with patch("recommendation_api.routes.async_session_factory", return_value=mock_session):
        with patch("recommendation_api.routes.rerank", new_callable=AsyncMock, return_value=reranked):
            transport = ASGITransport(app=app)
            async with AsyncClient(transport=transport, base_url="http://test") as client:
                response = await client.get("/recommendations/u1?limit=5&explain=true")

    data = response.json()
    assert data["source"] == "llm"
    assert data["items"][0]["item_id"] == "i2"
    assert data["items"][0]["explanation"] == "Highly relevant"
    # Should cache LLM results
    mock_redis.setex.assert_awaited_once()


# Test 3: explain=true with LLM cache hit
@pytest.mark.asyncio
async def test_explain_true_llm_cache_hit(mock_redis):
    cached = [{"item_id": "i2", "score": 3.0, "rank": 1, "explanation": "Cached explanation"}]
    mock_redis.get = AsyncMock(return_value=json.dumps(cached))

    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        response = await client.get("/recommendations/u1?explain=true")

    data = response.json()
    assert data["source"] == "llm_cache"
    assert data["items"][0]["explanation"] == "Cached explanation"


# Test 4: LLM timeout falls back to score-based
@pytest.mark.asyncio
async def test_llm_timeout_fallback(mock_redis):
    mock_redis.get = AsyncMock(return_value=None)
    rows = _make_rows(("i1", 5.0), ("i2", 3.0))
    mock_session = _mock_db_session(rows)

    # rerank returns original candidates on timeout (that's what it does internally)
    async def fake_rerank(candidates, **kwargs):
        return candidates  # fallback behavior

    with patch("recommendation_api.routes.async_session_factory", return_value=mock_session):
        with patch("recommendation_api.routes.rerank", side_effect=fake_rerank):
            transport = ASGITransport(app=app)
            async with AsyncClient(transport=transport, base_url="http://test") as client:
                response = await client.get("/recommendations/u1?explain=true")

    data = response.json()
    assert data["source"] == "llm"
    assert data["items"][0]["item_id"] == "i1"  # original order preserved
    assert data["items"][0]["explanation"] is None


# Test 5: rerank function handles timeout
@pytest.mark.asyncio
async def test_rerank_timeout():
    import asyncio
    from recommendation_api.llm_reranker import rerank

    candidates = [
        RecommendationItem(item_id="i1", score=5.0, rank=1),
        RecommendationItem(item_id="i2", score=3.0, rank=2),
    ]

    async def slow_groq(*args, **kwargs):
        await asyncio.sleep(10)

    with patch("recommendation_api.llm_reranker.GROQ_API_KEY", "test-key"):
        with patch("recommendation_api.llm_reranker._call_groq", side_effect=slow_groq):
            with patch("recommendation_api.llm_reranker.LLM_TIMEOUT", 0.01):
                result = await rerank(candidates)

    # Should return original candidates on timeout
    assert len(result) == 2
    assert result[0].item_id == "i1"
    assert result[0].explanation is None


# Test 6: rerank returns original on API error
@pytest.mark.asyncio
async def test_rerank_api_error():
    from recommendation_api.llm_reranker import rerank

    candidates = [
        RecommendationItem(item_id="i1", score=5.0, rank=1),
    ]

    with patch("recommendation_api.llm_reranker.GROQ_API_KEY", "test-key"):
        with patch("recommendation_api.llm_reranker._call_groq", side_effect=Exception("API Error")):
            result = await rerank(candidates)

    assert len(result) == 1
    assert result[0].item_id == "i1"
