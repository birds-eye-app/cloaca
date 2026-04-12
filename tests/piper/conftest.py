import os
from pathlib import Path

import pytest
import pytest_asyncio
from sqlalchemy import text
from sqlalchemy.ext.asyncio import create_async_engine

# Set a dummy eBird API key so the phoebe client can be imported.
# All actual API calls are mocked in tests.
os.environ.setdefault("EBIRD_API_KEY", "test-dummy-key")

_SCHEMA_SQL = (
    Path(__file__).resolve().parent.parent.parent
    / "src"
    / "cloaca"
    / "piper"
    / "sql"
    / "schema.sql"
).read_text()


@pytest.fixture(scope="session")
def pg_url():
    """Provide a Postgres URL for the test session.

    By default, starts a Postgres container via testcontainers.
    Set TEST_DATABASE_URL to skip the container and use an existing DB
    (e.g. the docker-compose one).
    """
    if url := os.environ.get("TEST_DATABASE_URL"):
        # Normalize to asyncpg driver
        url = url.replace("postgresql://", "postgresql+asyncpg://")
        url = url.replace("postgresql+psycopg2://", "postgresql+asyncpg://")
        yield url
        return

    from testcontainers.postgres import PostgresContainer

    with PostgresContainer("postgres:16-alpine") as pg:
        url = pg.get_connection_url()
        url = url.replace("postgresql+psycopg2://", "postgresql+asyncpg://")
        yield url


@pytest_asyncio.fixture(autouse=True)
async def state_db(pg_url, monkeypatch):
    """Provide a fresh Postgres schema for each test."""
    from cloaca.piper import db_pool

    # Point db_pool at the test Postgres
    monkeypatch.setenv("DATABASE_URL", pg_url)
    # Reset any cached engine from a prior test
    await db_pool.close_engine()

    engine = create_async_engine(pg_url)

    # Drop and recreate tables for full isolation
    async with engine.begin() as conn:
        for table in [
            "birdcast_post_log",
            "pending_provisional_lifers",
            "hotspot_all_time_species",
            "backfill_status",
            "hotspot_year_species",
        ]:
            await conn.execute(text(f"DROP TABLE IF EXISTS {table} CASCADE"))
        for statement in _SCHEMA_SQL.split(";"):
            statement = statement.strip()
            if statement:
                await conn.execute(text(statement))

    await engine.dispose()

    yield

    await db_pool.close_engine()
