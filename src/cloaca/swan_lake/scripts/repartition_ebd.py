# /// script
# requires-python = ">=3.12"
# dependencies = [
#   "duckdb>=1.3",
#   "boto3>=1.34",
#   "python-dotenv>=1.0",
# ]
# ///
"""Repartition / repackage an existing R2 Parquet release for faster region queries.

Two output modes:

  partitioned-parquet (default)
    Reads r2://<bucket>/release=<rel>/*.parquet and writes partitioned output
    to r2://<bucket>/partitioned-by-state/release=<rel>/country_code=X/state_code=Y/.
    Uses DuckDB's `COPY ... PARTITION_BY` directly against R2 (no local staging
    required). For region-bound queries the partition prefix prunes everything
    outside the matching state.

  native-duckdb
    Builds a single .duckdb file with rows clustered by sort order on the
    common-filter columns (country_code, state_code, locality_id,
    observation_date), then uploads to r2://<bucket>/duckdb/<rel>.duckdb.
    DuckDB's per-row-group zone maps prune scans on those columns; explicit
    indexes are deliberately NOT used (DuckDB's docs warn they add storage
    cost without speeding up OLAP filter scans —
    https://duckdb.org/docs/guides/performance/indexing).

Inputs come from environment variables (never command-line):
  EBD_TAR_URL              — used only to derive the release name
  R2_ACCOUNT_ID, R2_BUCKET, R2_ACCESS_KEY_ID, R2_SECRET_ACCESS_KEY
  EBD_LOCAL_WORK           — optional local work dir for native-duckdb mode
                             (defaults to /tmp/ebd-repartition)

Run:
  uv run src/cloaca/swan_lake/scripts/repartition_ebd.py partitioned-parquet
  uv run src/cloaca/swan_lake/scripts/repartition_ebd.py native-duckdb
"""

from __future__ import annotations

import argparse
import os
import sys
import time
from pathlib import Path

import boto3
import boto3.s3.transfer as s3_transfer
import duckdb
from dotenv import load_dotenv


def derive_release() -> str:
    url = os.environ.get("EBD_TAR_URL", "")
    base = Path(url.rsplit("/", 1)[-1]).stem
    if not base:
        raise RuntimeError(
            "Cannot derive release name. Set EBD_TAR_URL or pass --release."
        )
    return base


def configure_duckdb(con: duckdb.DuckDBPyConnection):
    """Install/load httpfs + create R2 secret on the given connection."""
    con.execute("INSTALL httpfs")
    con.execute("LOAD httpfs")
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


def make_s3_client():
    return boto3.client(
        "s3",
        endpoint_url=f"https://{os.environ['R2_ACCOUNT_ID']}.r2.cloudflarestorage.com",
        aws_access_key_id=os.environ["R2_ACCESS_KEY_ID"],
        aws_secret_access_key=os.environ["R2_SECRET_ACCESS_KEY"],
        region_name="auto",
    )


def cmd_partitioned_parquet(args):
    bucket = os.environ["R2_BUCKET"]
    release = args.release or derive_release()
    src = f"r2://{bucket}/release={release}/*.parquet"
    dst = f"r2://{bucket}/partitioned-by-state/release={release}/"

    print(f"Source: {src}")
    print(f"Destination: {dst}")
    print("Partition keys: country_code, state_code")
    print("Compression: ZSTD")
    print()

    con = duckdb.connect()
    configure_duckdb(con)
    # Surface progress and bound memory so DuckDB doesn't OOM on the partition
    # buffers (one open writer per (country_code, state_code) pair, plus the
    # internal row-buffer per writer).
    con.execute("PRAGMA memory_limit='12GB'")
    con.execute("PRAGMA threads=8")
    con.execute("PRAGMA enable_progress_bar=true")

    t0 = time.monotonic()
    con.execute(f"""
        COPY (
            SELECT *
            FROM read_parquet('{src}')
            ORDER BY country_code, state_code, locality_id, observation_date
        )
        TO '{dst}'
        (FORMAT PARQUET,
         PARTITION_BY (country_code, state_code),
         COMPRESSION ZSTD,
         OVERWRITE_OR_IGNORE)
    """)
    elapsed = time.monotonic() - t0
    print(f"\nDone in {elapsed / 60:.1f} min")


def cmd_native_duckdb(args):
    bucket = os.environ["R2_BUCKET"]
    release = args.release or derive_release()
    src = f"r2://{bucket}/release={release}/*.parquet"
    work_dir = Path(os.environ.get("EBD_LOCAL_WORK", "/tmp/ebd-repartition"))
    work_dir.mkdir(parents=True, exist_ok=True)
    local_db = work_dir / f"{release}.duckdb"
    if local_db.exists():
        print(f"Removing existing {local_db}")
        local_db.unlink()
    raw_key = f"duckdb/{release}.duckdb"

    print(f"Source:        {src}")
    print(f"Local build:   {local_db}")
    print(f"R2 destination: r2://{bucket}/{raw_key}")
    print()

    con = duckdb.connect(str(local_db))
    configure_duckdb(con)
    con.execute("PRAGMA memory_limit='12GB'")
    con.execute("PRAGMA threads=8")
    con.execute("PRAGMA enable_progress_bar=true")

    print("Phase 1: CREATE TABLE ebd AS SELECT ... ORDER BY (clustering)")
    print("  sort order: (country_code, state_code, locality_id, observation_date)")
    print("  this clusters rows so DuckDB's per-row-group zone maps prune")
    print("  filters on those columns at scan time without explicit indexes.")
    t0 = time.monotonic()
    con.execute(f"""
        CREATE TABLE ebd AS
        SELECT * FROM read_parquet('{src}')
        ORDER BY country_code, state_code, locality_id, observation_date
    """)
    print(f"  load + sort done in {(time.monotonic() - t0) / 60:.1f} min")

    print("Phase 2: ANALYZE for query planner stats")
    t2 = time.monotonic()
    con.execute("ANALYZE")
    print(f"  done in {(time.monotonic() - t2) / 60:.1f} min")

    con.close()
    db_size = local_db.stat().st_size
    print(f"Local file: {db_size / 1e9:.2f} GB")

    print(f"Phase 3: upload to r2://{bucket}/{raw_key}")
    s3 = make_s3_client()
    config = s3_transfer.TransferConfig(
        multipart_threshold=64 * 1024 * 1024,
        multipart_chunksize=64 * 1024 * 1024,
        max_concurrency=4,
        use_threads=True,
    )
    uploaded = [0]

    def cb(n):
        uploaded[0] += n
        if uploaded[0] // (1024 * 1024 * 1024) > (uploaded[0] - n) // (
            1024 * 1024 * 1024
        ):
            print(f"    uploaded {uploaded[0] / 1e9:.1f}/{db_size / 1e9:.1f} GB")

    t3 = time.monotonic()
    s3.upload_file(str(local_db), bucket, raw_key, Config=config, Callback=cb)
    print(f"  upload done in {(time.monotonic() - t3) / 60:.1f} min")

    if args.cleanup_local:
        local_db.unlink()
        print(f"Removed {local_db}")


def main():
    load_dotenv()
    p = argparse.ArgumentParser()
    p.add_argument(
        "--release",
        default=None,
        help="Release name (default: derived from EBD_TAR_URL)",
    )
    sub = p.add_subparsers(dest="cmd", required=True)

    pp = sub.add_parser("partitioned-parquet")
    pp.set_defaults(func=cmd_partitioned_parquet)

    nd = sub.add_parser("native-duckdb")
    nd.add_argument(
        "--cleanup-local",
        action="store_true",
        help="Delete the local .duckdb after upload",
    )
    nd.set_defaults(func=cmd_native_duckdb)

    args = p.parse_args()
    args.func(args)


if __name__ == "__main__":
    sys.exit(main() or 0)
