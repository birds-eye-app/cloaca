"""
Build the ebd_nyc table from the full EBD source database.

Reads from the external drive source DB and writes an optimized,
NYC-only table to a local output DB.

Usage:
    uv run python src/cloaca/swan_lake/scripts/build_ebd_nyc.py \
        --source /Volumes/lacie_disk/ebd_relJan-2026.db \
        --output src/cloaca/swan_lake/dbs/ebd_nyc.db
"""

import argparse
import os
import subprocess
import sys
import tempfile
import threading
import time

NYC_COUNTIES = ("US-NY-061", "US-NY-047", "US-NY-081", "US-NY-005", "US-NY-085")

# fmt: off
SQL_TEMPLATE = """
ATTACH '{source}' AS src (READ_ONLY);
SET enable_progress_bar = true;

CREATE TYPE IF NOT EXISTS category_t      AS ENUM ('species','issf','spuh','slash','hybrid','form','domestic','intergrade');
CREATE TYPE IF NOT EXISTS exotic_code_t   AS ENUM ('N','X','P');
CREATE TYPE IF NOT EXISTS locality_type_t AS ENUM ('C','H','P','PC','S','T');
CREATE TYPE IF NOT EXISTS protocol_t      AS ENUM ('Area','Banding','Breeding Bird Atlas','Historical','Incidental','My Yard Counts','Nocturnal Flight Call Count','Random','Stationary','Stationary (2 band, 100m)','Stationary (2 band, 25m)','Stationary (2 band, 30m)','Stationary (2 band, 50m)','Stationary (2 band, 75m)','Stationary (3 band, 30m+100m)','Stationary (Directional)','Traveling','Traveling (2 band, 25m)','Traveling - Property Specific','eBird Pelagic Protocol');

CREATE TABLE ebd_nyc AS
SELECT
    category::category_t                                    AS category,
    common_name,
    scientific_name,
    subspecies_common_name,
    subspecies_scientific_name,
    exotic_code::exotic_code_t                              AS exotic_code,
    observation_count,
    country_code,
    state_code,
    county_code,
    locality,
    locality_id,
    locality_type::locality_type_t                          AS locality_type,
    latitude::FLOAT                                         AS latitude,
    longitude::FLOAT                                        AS longitude,
    observation_date,
    sampling_event_identifier,
    protocol_name::protocol_t                               AS protocol_name,
    duration_minutes::INTEGER                               AS duration_minutes,
    effort_distance_km::FLOAT                               AS effort_distance_km,
    number_observers::SMALLINT                              AS number_observers,
    all_species_reported,
    group_identifier,
    approved,
    reviewed
FROM src.ebd_full
WHERE county_code IN ('US-NY-061','US-NY-047','US-NY-081','US-NY-005','US-NY-085')
{order_by}{limit};

SELECT COUNT(*) AS row_count FROM ebd_nyc;
"""
# fmt: on


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--source", default="/Volumes/lacie_disk/ebd_relJan-2026.db")
    parser.add_argument("--output", default="src/cloaca/swan_lake/dbs/ebd_nyc.db")
    parser.add_argument("--limit", type=int, default=None, help="Limit rows for testing (skips ORDER BY)")
    args = parser.parse_args()

    if not os.path.exists(args.source):
        print(f"Source DB not found: {args.source}")
        sys.exit(1)

    if os.path.exists(args.output):
        print(f"Output DB already exists: {args.output}")
        print("Delete it first if you want to rebuild.")
        sys.exit(1)

    order_by = "" if args.limit else "ORDER BY locality_id, observation_date\n"
    limit = f"LIMIT {args.limit}" if args.limit else ""

    sql = SQL_TEMPLATE.format(source=args.source, order_by=order_by, limit=limit)

    print(f"Source: {args.source}")
    print(f"Output: {args.output}")
    print(f"Filter: {NYC_COUNTIES}")
    if args.limit:
        print(f"Limit:  {args.limit:,} rows (test mode — ORDER BY skipped)")
    print()

    with tempfile.NamedTemporaryFile(mode="w", suffix=".sql", delete=False) as f:
        f.write(sql)
        sql_file = f.name

    def heartbeat(stop: threading.Event):
        while not stop.wait(30):
            elapsed = time.time() - start_time
            print(f"  still running... {elapsed/60:.1f} min elapsed", flush=True)

    print("Starting extraction...")
    sys.stdout.flush()
    start_time = time.time()
    stop_event = threading.Event()
    watcher = threading.Thread(target=heartbeat, args=(stop_event,), daemon=True)
    watcher.start()
    try:
        result = subprocess.run(
            ["duckdb", args.output, "-f", sql_file],
            check=False,
        )
    finally:
        stop_event.set()
        os.unlink(sql_file)

    if result.returncode != 0:
        print("Extraction failed.")
        if os.path.exists(args.output):
            os.unlink(args.output)
        sys.exit(1)

    elapsed = time.time() - start_time
    db_size_mb = os.path.getsize(args.output) / 1024 / 1024
    print(f"\nDone in {elapsed/60:.1f} minutes")
    print(f"DB size: {db_size_mb:.1f} MB")
