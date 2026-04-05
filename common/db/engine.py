import os
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

DATABASE_URL = os.getenv("DATABASE_URL", "postgresql+asyncpg://recsys:recsys@localhost:5433/recsys")

engine = create_async_engine(DATABASE_URL, echo=False, pool_size=5, max_overflow=10)
async_session_factory = async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)

async def get_async_session():
    """Async context manager for database sessions."""
    async with async_session_factory() as session:
        yield session
