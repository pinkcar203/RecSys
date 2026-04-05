"""Create user_item_scores table

Revision ID: 001
Revises:
Create Date: 2026-04-03
"""
from __future__ import annotations

from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa

revision: str = "001"
down_revision: Union[str, None] = None
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.create_table(
        "user_item_scores",
        sa.Column("user_id", sa.String(), nullable=False),
        sa.Column("item_id", sa.String(), nullable=False),
        sa.Column("score", sa.Float(), nullable=False, server_default="0.0"),
        sa.Column("last_updated", sa.DateTime(), nullable=False),
        sa.PrimaryKeyConstraint("user_id", "item_id"),
    )
    op.create_index("ix_user_item_scores_user_id", "user_item_scores", ["user_id"])


def downgrade() -> None:
    op.drop_index("ix_user_item_scores_user_id")
    op.drop_table("user_item_scores")
