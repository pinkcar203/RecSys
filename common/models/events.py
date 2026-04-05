from __future__ import annotations

from datetime import datetime
from enum import Enum
from typing import Dict
from pydantic import BaseModel, Field

class EventType(str, Enum):
    CLICK = "click"
    VIEW = "view"
    PURCHASE = "purchase"

EVENT_WEIGHTS: Dict[EventType, float] = {
    EventType.CLICK: 1.0,
    EventType.VIEW: 0.5,
    EventType.PURCHASE: 3.0,
}

class UserEvent(BaseModel):
    user_id: str = Field(..., min_length=1, description="Unique user identifier")
    item_id: str = Field(..., min_length=1, description="Unique item identifier")
    event_type: EventType
    timestamp: datetime

class UserEventMessage(BaseModel):
    """Kafka message payload - includes the weight for consumer convenience."""
    user_id: str
    item_id: str
    event_type: EventType
    weight: float
    timestamp: datetime
