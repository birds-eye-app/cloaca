import os

from sqlalchemy.ext.asyncio import AsyncEngine, create_async_engine

_engine: AsyncEngine | None = None


def get_engine() -> AsyncEngine:
    global _engine
    if _engine is None:
        url = os.environ.get("PIPER_POSTGRES_DB_URL") or os.environ["DATABASE_URL"]
        # Render uses postgres:// but SQLAlchemy needs postgresql+asyncpg://
        if url.startswith("postgres://"):
            url = url.replace("postgres://", "postgresql+asyncpg://", 1)
        elif url.startswith("postgresql://"):
            url = url.replace("postgresql://", "postgresql+asyncpg://", 1)
        _engine = create_async_engine(url, pool_size=5, max_overflow=0)
    return _engine


async def close_engine():
    global _engine
    if _engine is not None:
        await _engine.dispose()
        _engine = None
