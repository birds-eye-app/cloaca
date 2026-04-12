"""One-time script to migrate piper state from DuckDB to Postgres.

Usage:
    DATABASE_URL=postgresql://piper:piper@localhost:5432/piper_state \
        uv run python scripts/migrate_duckdb_to_postgres.py /tmp/piper_state.db
"""

import asyncio
import sys

import duckdb
from sqlalchemy import text
from sqlalchemy.ext.asyncio import create_async_engine


async def migrate(duckdb_path: str, pg_url: str):
    con = duckdb.connect(duckdb_path, read_only=True)
    engine = create_async_engine(pg_url)

    async with engine.begin() as conn:
        # --- hotspot_year_species ---
        rows = con.execute(
            "SELECT hotspot_id, year, species_code, common_name, scientific_name, "
            "first_obs_date, observer_name, checklist_id, created_at "
            "FROM hotspot_year_species"
        ).fetchall()
        for r in rows:
            await conn.execute(
                text(
                    "INSERT INTO hotspot_year_species "
                    "(hotspot_id, year, species_code, common_name, scientific_name, "
                    "first_obs_date, observer_name, checklist_id, created_at) "
                    "VALUES (:a,:b,:c,:d,:e,:f,:g,:h,:i) "
                    "ON CONFLICT DO NOTHING"
                ),
                {
                    "a": r[0],
                    "b": r[1],
                    "c": r[2],
                    "d": r[3],
                    "e": r[4],
                    "f": r[5],
                    "g": r[6],
                    "h": r[7],
                    "i": r[8],
                },
            )
        print(f"  hotspot_year_species: {len(rows)} rows")

        # --- hotspot_all_time_species ---
        rows = con.execute(
            "SELECT hotspot_id, species_code FROM hotspot_all_time_species"
        ).fetchall()
        for r in rows:
            await conn.execute(
                text(
                    "INSERT INTO hotspot_all_time_species (hotspot_id, species_code) "
                    "VALUES (:a,:b) ON CONFLICT DO NOTHING"
                ),
                {"a": r[0], "b": r[1]},
            )
        print(f"  hotspot_all_time_species: {len(rows)} rows")

        # --- backfill_status ---
        rows = con.execute(
            "SELECT hotspot_id, year, completed_at, species_count FROM backfill_status"
        ).fetchall()
        for r in rows:
            await conn.execute(
                text(
                    "INSERT INTO backfill_status "
                    "(hotspot_id, year, completed_at, species_count) "
                    "VALUES (:a,:b,:c,:d) ON CONFLICT DO NOTHING"
                ),
                {"a": r[0], "b": r[1], "c": r[2], "d": r[3]},
            )
        print(f"  backfill_status: {len(rows)} rows")

        # --- pending_provisional_lifers ---
        # The DuckDB schema may have "checklist_id" (old) or "sub_id" (new)
        duck_cols = [
            c[0] for c in con.execute("DESCRIBE pending_provisional_lifers").fetchall()
        ]
        sub_id_col = "sub_id" if "sub_id" in duck_cols else "checklist_id"
        rows = con.execute(
            "SELECT hotspot_id, species_code, common_name, scientific_name, "
            f"obs_date, observer_name, {sub_id_col}, lifer_type, year, created_at "
            "FROM pending_provisional_lifers"
        ).fetchall()
        for r in rows:
            await conn.execute(
                text(
                    "INSERT INTO pending_provisional_lifers "
                    "(hotspot_id, species_code, common_name, scientific_name, "
                    "obs_date, observer_name, sub_id, lifer_type, year, created_at) "
                    "VALUES (:a,:b,:c,:d,:e,:f,:g,:h,:i,:j) "
                    "ON CONFLICT DO NOTHING"
                ),
                {
                    "a": r[0],
                    "b": r[1],
                    "c": r[2],
                    "d": r[3],
                    "e": r[4],
                    "f": r[5],
                    "g": r[6],
                    "h": r[7],
                    "i": r[8],
                    "j": r[9],
                },
            )
        print(f"  pending_provisional_lifers: {len(rows)} rows")

    await engine.dispose()
    con.close()
    print("\nMigration complete!")


if __name__ == "__main__":
    import os

    if len(sys.argv) < 2:
        print(f"Usage: {sys.argv[0]} <path-to-piper_state.db>")
        sys.exit(1)

    duckdb_path = sys.argv[1]
    pg_url = os.environ["DATABASE_URL"]
    # Normalize for asyncpg
    if pg_url.startswith("postgres://"):
        pg_url = pg_url.replace("postgres://", "postgresql+asyncpg://", 1)
    elif pg_url.startswith("postgresql://"):
        pg_url = pg_url.replace("postgresql://", "postgresql+asyncpg://", 1)

    print(f"Migrating from {duckdb_path} to Postgres...")
    asyncio.run(migrate(duckdb_path, pg_url))
