from __future__ import annotations

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession


async def upsert_user_item_score(
    session: AsyncSession,
    user_id: str,
    item_id: str,
    weight: float,
) -> None:
    # Upsert a user-item score using PostgreSQL INSERT ON CONFLICT.
    query = text("""
        INSERT INTO user_item_scores (user_id, item_id, score, last_updated)
        VALUES (:user_id, :item_id, :weight, CURRENT_TIMESTAMP)
        ON CONFLICT (user_id, item_id)
        DO UPDATE SET
            score = user_item_scores.score + :weight,
            last_updated = CURRENT_TIMESTAMP
    """)
    await session.execute(
        query,
        {
            "user_id": user_id,
            "item_id": item_id,
            "weight": weight,
        },
    )
    await session.commit()
