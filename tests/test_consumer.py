from __future__ import annotations

import pytest
from unittest.mock import AsyncMock, MagicMock, patch


# Test 1: process_message calls upsert and cache invalidation
@pytest.mark.asyncio
async def test_process_message_upserts_score():
    """Verify process_message calls upsert_user_item_score with correct args."""
    mock_session = AsyncMock()
    mock_session.__aenter__ = AsyncMock(return_value=mock_session)
    mock_session.__aexit__ = AsyncMock(return_value=False)

    mock_redis = AsyncMock()
    mock_redis.delete = AsyncMock()

    message = {"user_id": "u1", "item_id": "i1", "weight": 1.0}

    with patch("consumer_service.main.async_session_factory", return_value=mock_session):
        with patch("consumer_service.main.upsert_user_item_score", new_callable=AsyncMock) as mock_upsert:
            from consumer_service.main import process_message
            await process_message(message, mock_redis)

            mock_upsert.assert_awaited_once_with(mock_session, "u1", "i1", 1.0)


# Test 2: cache invalidation deletes correct keys
@pytest.mark.asyncio
async def test_invalidate_cache_deletes_keys():
    """Verify both recs: and recs_llm: keys are deleted."""
    mock_redis = AsyncMock()
    mock_redis.delete = AsyncMock()

    from consumer_service.main import invalidate_cache
    await invalidate_cache(mock_redis, "u1")

    mock_redis.delete.assert_awaited_once_with("recs:u1", "recs_llm:u1")


# Test 3: cache invalidation failure doesn't crash
@pytest.mark.asyncio
async def test_invalidate_cache_handles_redis_failure():
    """Verify cache invalidation logs warning but doesn't raise on Redis failure."""
    mock_redis = AsyncMock()
    mock_redis.delete = AsyncMock(side_effect=ConnectionError("Redis down"))

    from consumer_service.main import invalidate_cache
    # Should not raise
    await invalidate_cache(mock_redis, "u1")


# Test 4: process_message with purchase weight
@pytest.mark.asyncio
async def test_process_message_purchase_weight():
    """Verify process_message passes correct weight for purchase events."""
    mock_session = AsyncMock()
    mock_session.__aenter__ = AsyncMock(return_value=mock_session)
    mock_session.__aexit__ = AsyncMock(return_value=False)

    mock_redis = AsyncMock()
    mock_redis.delete = AsyncMock()

    message = {"user_id": "u2", "item_id": "i2", "weight": 3.0}

    with patch("consumer_service.main.async_session_factory", return_value=mock_session):
        with patch("consumer_service.main.upsert_user_item_score", new_callable=AsyncMock) as mock_upsert:
            from consumer_service.main import process_message
            await process_message(message, mock_redis)

            mock_upsert.assert_awaited_once_with(mock_session, "u2", "i2", 3.0)
