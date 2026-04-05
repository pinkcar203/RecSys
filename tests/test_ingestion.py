from __future__ import annotations

import pytest
from unittest.mock import AsyncMock
from httpx import AsyncClient, ASGITransport

from ingestion_service.main import app


@pytest.fixture
def mock_producer():
    producer = AsyncMock()
    producer.send = AsyncMock()
    return producer


# Health check

@pytest.mark.asyncio
async def test_health_check():
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        response = await client.get("/health")

    assert response.status_code == 200
    assert response.json() == {"status": "ok", "service": "ingestion"}


# Valid events

@pytest.mark.asyncio
async def test_post_valid_click_event(mock_producer):
    app.state.kafka_producer = mock_producer

    payload = {
        "user_id": "user-1",
        "item_id": "item-1",
        "event_type": "click",
        "timestamp": "2026-04-03T12:00:00Z",
    }

    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        response = await client.post("/events", json=payload)

    assert response.status_code == 202
    mock_producer.send.assert_awaited_once()
    call_kwargs = mock_producer.send.call_args.kwargs
    assert call_kwargs["topic"] == "user-events"
    assert call_kwargs["value"]["user_id"] == "user-1"
    assert call_kwargs["value"]["item_id"] == "item-1"
    assert call_kwargs["value"]["event_type"] == "click"
    assert call_kwargs["value"]["weight"] == 1.0


@pytest.mark.asyncio
async def test_post_valid_purchase_event(mock_producer):
    app.state.kafka_producer = mock_producer

    payload = {
        "user_id": "user-2",
        "item_id": "item-2",
        "event_type": "purchase",
        "timestamp": "2026-04-03T12:00:00Z",
    }

    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        response = await client.post("/events", json=payload)

    assert response.status_code == 202
    call_kwargs = mock_producer.send.call_args.kwargs
    assert call_kwargs["value"]["weight"] == 3.0
    assert call_kwargs["value"]["event_type"] == "purchase"


@pytest.mark.asyncio
async def test_post_valid_view_event(mock_producer):
    app.state.kafka_producer = mock_producer

    payload = {
        "user_id": "user-3",
        "item_id": "item-3",
        "event_type": "view",
        "timestamp": "2026-04-03T12:00:00Z",
    }

    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        response = await client.post("/events", json=payload)

    assert response.status_code == 202
    call_kwargs = mock_producer.send.call_args.kwargs
    assert call_kwargs["value"]["weight"] == 0.5
    assert call_kwargs["value"]["event_type"] == "view"


# Invalid events

@pytest.mark.asyncio
async def test_post_invalid_event_missing_fields(mock_producer):
    app.state.kafka_producer = mock_producer

    payload = {
        "item_id": "item-1",
        "event_type": "click",
        "timestamp": "2026-04-03T12:00:00Z",
    }

    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        response = await client.post("/events", json=payload)

    assert response.status_code == 422
    mock_producer.send.assert_not_awaited()


@pytest.mark.asyncio
async def test_post_invalid_event_bad_type(mock_producer):
    app.state.kafka_producer = mock_producer

    payload = {
        "user_id": "user-1",
        "item_id": "item-1",
        "event_type": "invalid",
        "timestamp": "2026-04-03T12:00:00Z",
    }

    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        response = await client.post("/events", json=payload)

    assert response.status_code == 422
    mock_producer.send.assert_not_awaited()
