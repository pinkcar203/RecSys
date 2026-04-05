from datetime import datetime
from sqlalchemy import Column, String, Float, DateTime, PrimaryKeyConstraint
from sqlalchemy.orm import DeclarativeBase

class Base(DeclarativeBase):
    pass

class UserItemScore(Base):
    __tablename__ = "user_item_scores"

    user_id = Column(String, nullable=False)
    item_id = Column(String, nullable=False)
    score = Column(Float, nullable=False, default=0.0)
    last_updated = Column(DateTime, nullable=False, default=datetime.utcnow)

    __table_args__ = (
        PrimaryKeyConstraint("user_id", "item_id"),
    )

    def __repr__(self):
        return f"<UserItemScore(user_id={self.user_id}, item_id={self.item_id}, score={self.score})>"
