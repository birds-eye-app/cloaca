# /// script
# requires-python = ">=3.12"
# dependencies = [
#   "duckdb>=1.3",
#   "python-dotenv>=1.0",
# ]
# ///
"""Benchmark identical analytic queries against three R2 dataset shapes:

    unpartitioned   r2://<bucket>/release=<rel>/*.parquet
                    (input-order Parquet chunks, naturally sorted by date)
    partitioned     r2://<bucket>/partitioned-by-state/release=<rel>/
                    (Hive-partitioned by country_code + state_code, locality-sorted)
    native          r2://<bucket>/duckdb/<rel>.duckdb
                    (single .duckdb file, rows clustered by location + date)

For each (dataset, query) pair we run the query twice — once cold, once warm —
and report wallclock + reported "rows" + a short result preview. Times are
captured via DuckDB's built-in `.timer on` so we measure the engine, not the
shell. R2 download bandwidth dominates everything except the local-NYC-DB
sanity baselines. Ad-hoc tweaks: adjust `QUERIES` to add new probes.

Run:
  uv run src/cloaca/swan_lake/scripts/bench_partitions.py [--datasets unpartitioned,partitioned,native] [--queries q1,q2,...]
"""

from __future__ import annotations

import argparse
import os
import sys
import time
from pathlib import Path

import duckdb
from dotenv import load_dotenv


def derive_release() -> str:
    base = Path(os.environ.get("EBD_TAR_URL", "").rsplit("/", 1)[-1]).stem
    if not base:
        raise RuntimeError("Cannot derive release from EBD_TAR_URL")
    return base


def configure(con):
    con.execute("INSTALL httpfs")
    con.execute("LOAD httpfs")
    # Long timeouts + retries — slow / contended residential R2 connections
    # otherwise trip 30s defaults frequently in long benchmark runs.
    con.execute("SET http_timeout = 300000")
    con.execute("SET http_retries = 5")
    con.execute("SET http_retry_wait_ms = 2000")
    con.execute(
        """
        CREATE OR REPLACE SECRET ebd_r2 (
            TYPE r2,
            KEY_ID $key,
            SECRET $secret,
            ACCOUNT_ID $account
        )
        """,
        {
            "key": os.environ["R2_ACCESS_KEY_ID"],
            "secret": os.environ["R2_SECRET_ACCESS_KEY"],
            "account": os.environ["R2_ACCOUNT_ID"],
        },
    )
    con.execute("PRAGMA threads=8")


# Each query is parameterized on a SOURCE expression that returns a relation
# named `ebd`. The dataset bindings below substitute that.
QUERIES: dict[str, str] = {
    "q1_state_aggregate": """
        SELECT locality, COUNT(DISTINCT scientific_name) AS species_count
        FROM {SOURCE}
        WHERE country_code = 'US' AND state_code = 'US-CA'
          AND all_species_reported = TRUE
          AND approved = TRUE
          AND category NOT IN ('slash', 'spuh')
          AND (exotic_code IS NULL OR exotic_code = 'N')
        GROUP BY locality
        ORDER BY species_count DESC
        LIMIT 10
    """,
    "q2_hotspot_point": """
        SELECT
            COUNT(*) AS rows,
            COUNT(DISTINCT scientific_name) AS species,
            COUNT(DISTINCT sampling_event_identifier) AS checklists
        FROM {SOURCE}
        WHERE locality_id = 'L99381'
    """,
    "q3_state_recent": """
        SELECT common_name, COUNT(*) AS sightings
        FROM {SOURCE}
        WHERE country_code = 'US' AND state_code = 'US-NY'
          AND observation_date >= DATE '2024-01-01'
          AND approved = TRUE
        GROUP BY common_name
        ORDER BY sightings DESC
        LIMIT 10
    """,
    "q4_global_common_name": """
        SELECT country_code, COUNT(*) AS sightings
        FROM {SOURCE}
        WHERE common_name = 'Bald Eagle'
          AND approved = TRUE
        GROUP BY country_code
        ORDER BY sightings DESC
        LIMIT 15
    """,
    "q5_global_pattern_scan": """
        -- Forces a full scan; partitioning shouldn't help, only file format
        SELECT COUNT(DISTINCT locality_id) AS owly_localities
        FROM {SOURCE}
        WHERE common_name ILIKE '%Owl%'
          AND approved = TRUE
    """,
    "q6_state_species": """
        -- Combines region-partition pruning with a species filter; tests
        -- whether sort-within-partition on locality helps zone-map prune.
        SELECT locality_id, locality, COUNT(*) AS sightings
        FROM {SOURCE}
        WHERE country_code = 'US' AND state_code = 'US-NY'
          AND common_name = 'Eastern Phoebe'
          AND approved = TRUE
        GROUP BY 1, 2
        ORDER BY sightings DESC
        LIMIT 10
    """,
}


def source_for(dataset: str, bucket: str, release: str) -> str:
    if dataset == "unpartitioned":
        return f"read_parquet('r2://{bucket}/release={release}/*.parquet')"
    if dataset == "partitioned":
        return (
            f"read_parquet('r2://{bucket}/partitioned-by-state/release={release}/"
            f"**/*.parquet', hive_partitioning=true)"
        )
    if dataset == "native":
        # Attaches as ephemeral schema 'native_db'
        return "native_db.ebd"
    raise ValueError(dataset)


def setup_native(con, bucket: str, release: str):
    con.execute(
        f"ATTACH 'r2://{bucket}/duckdb/{release}.duckdb' AS native_db (READ_ONLY)"
    )


def run_query(con, sql: str, label: str):
    t0 = time.monotonic()
    rows = con.execute(sql).fetchall()
    elapsed = time.monotonic() - t0
    return elapsed, rows


def main():
    load_dotenv()
    p = argparse.ArgumentParser()
    p.add_argument("--release", default=None)
    p.add_argument(
        "--datasets",
        default="unpartitioned,partitioned,native",
        help="Comma-separated subset of {unpartitioned, partitioned, native}",
    )
    p.add_argument(
        "--queries",
        default=",".join(QUERIES.keys()),
        help="Comma-separated subset of QUERIES",
    )
    args = p.parse_args()

    bucket = os.environ["R2_BUCKET"]
    release = args.release or derive_release()
    datasets = [d.strip() for d in args.datasets.split(",") if d.strip()]
    queries = [q.strip() for q in args.queries.split(",") if q.strip()]

    print(f"Release: {release}")
    print(f"Datasets: {datasets}")
    print(f"Queries: {queries}")
    print()

    results: list[tuple[str, str, str, float, int]] = []
    for ds in datasets:
        print(f"=== dataset: {ds} ===")
        con = duckdb.connect()
        configure(con)
        if ds == "native":
            try:
                setup_native(con, bucket, release)
            except Exception as e:
                print(f"  (native dataset not available: {e})\n")
                continue
        src = source_for(ds, bucket, release)
        for qname in queries:
            sql = QUERIES[qname].format(SOURCE=src)
            for trial in ("cold", "warm"):
                try:
                    elapsed, rows = run_query(con, sql, f"{ds}/{qname}/{trial}")
                except Exception as e:
                    print(f"  [{qname}/{trial}] ERROR: {e}")
                    results.append((ds, qname, trial, -1, 0))
                    continue
                preview = ", ".join(str(r) for r in rows[:2]) if rows else "(empty)"
                print(
                    f"  [{qname}/{trial}] {elapsed:7.2f}s  "
                    f"rows={len(rows)}  preview={preview[:120]}"
                )
                results.append((ds, qname, trial, elapsed, len(rows)))
        con.close()
        print()

    # Summary
    print("=== summary ===")
    print(f"{'dataset':<14} {'query':<28} {'cold':>8} {'warm':>8}")
    seen: dict[tuple[str, str], dict[str, float]] = {}
    for ds, q, trial, elapsed, _ in results:
        seen.setdefault((ds, q), {})[trial] = elapsed
    for (ds, q), trials in seen.items():
        c = trials.get("cold", -1)
        w = trials.get("warm", -1)
        print(f"{ds:<14} {q:<28} {c:>7.2f}s {w:>7.2f}s")


if __name__ == "__main__":
    sys.exit(main() or 0)
