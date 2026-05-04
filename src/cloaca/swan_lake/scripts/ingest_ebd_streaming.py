# /// script
# requires-python = ">=3.12"
# dependencies = [
#   "duckdb>=1.3",
#   "pyarrow>=17",
#   "boto3>=1.34",
#   "rich>=13",
#   "python-dotenv>=1.0",
# ]
# ///
"""Stream-ingest the eBird EBD tar from a URL.

One pass: HTTP stream -> tar -> gunzip -> TSV parse -> fan out to two sinks:
  1. Rolling Parquet chunks uploaded to Cloudflare R2 (full dataset).
  2. NYC-county-filtered rows appended into a local DuckDB (Render-bound subset).

Run with:  uv run src/cloaca/swan_lake/scripts/ingest_ebd_streaming.py --url ...
"""

from __future__ import annotations

import argparse
import gzip
import os
import queue
import signal
import sys
import tarfile
import tempfile
import threading
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional
from urllib.request import Request, urlopen

import boto3
import duckdb
import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.csv as pa_csv
import pyarrow.parquet as pq
from dotenv import load_dotenv
from rich.console import Console
from rich.live import Live
from rich.panel import Panel
from rich.table import Table


NYC_COUNTIES = ["US-NY-061", "US-NY-047", "US-NY-081", "US-NY-005", "US-NY-085"]

# (raw TSV column name, snake_case name, arrow type). Order = output schema order.
# GLOBAL UNIQUE IDENTIFIER intentionally dropped (long string per row, never queried).
EBD_COLUMNS: list[tuple[str, str, pa.DataType]] = [
    ("CATEGORY", "category", pa.string()),
    ("COMMON NAME", "common_name", pa.string()),
    ("SCIENTIFIC NAME", "scientific_name", pa.string()),
    ("SUBSPECIES COMMON NAME", "subspecies_common_name", pa.string()),
    ("SUBSPECIES SCIENTIFIC NAME", "subspecies_scientific_name", pa.string()),
    ("EXOTIC CODE", "exotic_code", pa.string()),
    ("OBSERVATION COUNT", "observation_count", pa.string()),  # may be 'X'
    ("COUNTRY CODE", "country_code", pa.string()),
    ("STATE CODE", "state_code", pa.string()),
    ("COUNTY CODE", "county_code", pa.string()),
    ("LOCALITY", "locality", pa.string()),
    ("LOCALITY ID", "locality_id", pa.string()),
    ("LOCALITY TYPE", "locality_type", pa.string()),
    ("LATITUDE", "latitude", pa.float32()),
    ("LONGITUDE", "longitude", pa.float32()),
    ("OBSERVATION DATE", "observation_date", pa.date32()),
    ("SAMPLING EVENT IDENTIFIER", "sampling_event_identifier", pa.string()),
    ("PROTOCOL NAME", "protocol_name", pa.string()),
    ("DURATION MINUTES", "duration_minutes", pa.int32()),
    ("EFFORT DISTANCE KM", "effort_distance_km", pa.float32()),
    ("NUMBER OBSERVERS", "number_observers", pa.int16()),
    ("ALL SPECIES REPORTED", "all_species_reported", pa.bool_()),
    ("GROUP IDENTIFIER", "group_identifier", pa.string()),
    ("APPROVED", "approved", pa.bool_()),
    ("REVIEWED", "reviewed", pa.bool_()),
]

INCLUDE_COLS = [c[0] for c in EBD_COLUMNS]
SNAKE_NAMES = [c[1] for c in EBD_COLUMNS]
ARROW_TYPES = {c[0]: c[2] for c in EBD_COLUMNS}
ARROW_SCHEMA = pa.schema([(c[1], c[2]) for c in EBD_COLUMNS])


# === Stats =====================================================================


@dataclass
class Stats:
    started_at: float = field(default_factory=time.monotonic)
    total_bytes: int = 0
    bytes_downloaded: int = 0
    decompressed_bytes: int = 0
    rows_parsed: int = 0
    nyc_rows_kept: int = 0
    chunks_started: int = 1
    chunks_uploaded: int = 0
    bytes_uploaded: int = 0
    current_chunk_rows: int = 0
    current_chunk_bytes: int = 0
    last_uploaded_key: str = ""
    invalid_rows: int = 0
    status: str = "starting"
    lock: threading.Lock = field(default_factory=threading.Lock)

    def snapshot(self) -> dict:
        with self.lock:
            d = {
                "elapsed": time.monotonic() - self.started_at,
                "total_bytes": self.total_bytes,
                "bytes_downloaded": self.bytes_downloaded,
                "decompressed_bytes": self.decompressed_bytes,
                "rows_parsed": self.rows_parsed,
                "nyc_rows_kept": self.nyc_rows_kept,
                "chunks_started": self.chunks_started,
                "chunks_uploaded": self.chunks_uploaded,
                "bytes_uploaded": self.bytes_uploaded,
                "current_chunk_rows": self.current_chunk_rows,
                "current_chunk_bytes": self.current_chunk_bytes,
                "last_uploaded_key": self.last_uploaded_key,
                "invalid_rows": self.invalid_rows,
                "status": self.status,
            }
        e = d["elapsed"] or 1e-9
        d["dl_avg_mbps"] = d["bytes_downloaded"] / 1e6 / e
        d["decomp_avg_mbps"] = d["decompressed_bytes"] / 1e6 / e
        d["rows_per_s"] = d["rows_parsed"] / e
        d["decomp_ratio"] = (
            d["decompressed_bytes"] / d["bytes_downloaded"]
            if d["bytes_downloaded"]
            else 0
        )
        d["nyc_hit_rate"] = (
            d["nyc_rows_kept"] / d["rows_parsed"] if d["rows_parsed"] else 0
        )
        d["dl_eta_sec"] = (
            (d["total_bytes"] - d["bytes_downloaded"]) / 1e6 / d["dl_avg_mbps"]
            if d["total_bytes"] > 0 and d["dl_avg_mbps"] > 0
            else None
        )
        return d


def fmt_bytes(n: float) -> str:
    for unit in ["B", "KB", "MB", "GB", "TB"]:
        if abs(n) < 1024:
            return f"{n:.2f} {unit}"
        n /= 1024
    return f"{n:.2f} PB"


def fmt_duration(s: Optional[float]) -> str:
    if s is None:
        return "—"
    s = int(s)
    h, r = divmod(s, 3600)
    m, s = divmod(r, 60)
    if h:
        return f"{h}h{m:02d}m{s:02d}s"
    if m:
        return f"{m}m{s:02d}s"
    return f"{s}s"


# === I/O wrappers ==============================================================


class TrackingStream:
    def __init__(self, raw, stats: Stats):
        self._raw = raw
        self._stats = stats
        self._closed = False

    def read(self, n: int = -1) -> bytes:
        data = self._raw.read(n) if n != -1 else self._raw.read()
        if data:
            with self._stats.lock:
                self._stats.bytes_downloaded += len(data)
        return data

    def readable(self) -> bool:
        return True

    def writable(self) -> bool:
        return False

    def seekable(self) -> bool:
        return False

    @property
    def closed(self) -> bool:
        return self._closed

    def close(self):
        if self._closed:
            return
        self._closed = True
        try:
            self._raw.close()
        except Exception:
            pass


class DecompressTracker:
    def __init__(self, gz, stats: Stats):
        self._gz = gz
        self._stats = stats
        self._closed = False

    def read(self, n: int = -1) -> bytes:
        data = self._gz.read(n)
        if data:
            with self._stats.lock:
                self._stats.decompressed_bytes += len(data)
        return data

    def readable(self) -> bool:
        return True

    def writable(self) -> bool:
        return False

    def seekable(self) -> bool:
        return False

    @property
    def closed(self) -> bool:
        return self._closed

    def close(self):
        if self._closed:
            return
        self._closed = True
        try:
            self._gz.close()
        except Exception:
            pass


# === Live monitor ==============================================================


def render_panel(stats: Stats, args) -> Panel:
    s = stats.snapshot()
    t = Table.grid(padding=(0, 2))
    t.add_column(style="cyan", no_wrap=True)
    t.add_column()

    t.add_row("Status", f"[bold]{s['status']}[/]")
    t.add_row("Elapsed", fmt_duration(s["elapsed"]))
    t.add_row("", "")

    if s["total_bytes"] > 0:
        pct = 100 * s["bytes_downloaded"] / s["total_bytes"]
        eta = fmt_duration(s["dl_eta_sec"])
        t.add_row(
            "Downloaded",
            f"{fmt_bytes(s['bytes_downloaded'])} / {fmt_bytes(s['total_bytes'])} "
            f"({pct:.2f}%) · {s['dl_avg_mbps']:.1f} MB/s avg · ETA {eta}",
        )
    else:
        t.add_row(
            "Downloaded",
            f"{fmt_bytes(s['bytes_downloaded'])} · {s['dl_avg_mbps']:.1f} MB/s avg",
        )

    t.add_row(
        "Decompressed",
        f"{fmt_bytes(s['decompressed_bytes'])} · {s['decomp_avg_mbps']:.1f} MB/s · "
        f"ratio {s['decomp_ratio']:.2f}×",
    )
    t.add_row(
        "Rows parsed",
        f"{s['rows_parsed']:,} · {s['rows_per_s']:,.0f} rows/s"
        + (f" · skipped {s['invalid_rows']:,}" if s["invalid_rows"] else ""),
    )
    t.add_row(
        "NYC kept",
        f"{s['nyc_rows_kept']:,} ({s['nyc_hit_rate'] * 100:.4f}%)",
    )
    t.add_row(
        "Current chunk",
        f"#{s['chunks_started']} · {s['current_chunk_rows']:,} rows · "
        f"~{fmt_bytes(s['current_chunk_bytes'])} arrow",
    )
    t.add_row(
        "R2 uploaded",
        f"{s['chunks_uploaded']} chunk(s) · {fmt_bytes(s['bytes_uploaded'])}",
    )
    if s["last_uploaded_key"]:
        t.add_row("Last upload", s["last_uploaded_key"])

    return Panel(t, title="[bold]EBD streaming ingest[/]", border_style="blue")


def monitor_loop(stats: Stats, args, stop_event: threading.Event, console: Console):
    if console.is_terminal:
        with Live(
            render_panel(stats, args), console=console, refresh_per_second=4
        ) as live:
            while not stop_event.is_set():
                live.update(render_panel(stats, args))
                stop_event.wait(0.25)
            live.update(render_panel(stats, args))
        return
    while not stop_event.is_set():
        stop_event.wait(5)
        s = stats.snapshot()
        print(
            f"[{fmt_duration(s['elapsed'])}] "
            f"dl={fmt_bytes(s['bytes_downloaded'])}/{fmt_bytes(s['total_bytes'])} "
            f"({s['dl_avg_mbps']:.1f} MB/s) | "
            f"rows={s['rows_parsed']:,} ({s['rows_per_s']:,.0f}/s) | "
            f"nyc={s['nyc_rows_kept']:,} | "
            f"chunks {s['chunks_uploaded']}/{s['chunks_started']}",
            flush=True,
        )


# === R2 sink ===================================================================


def upload_worker(q: queue.Queue, s3, bucket: str, stats: Stats):
    while True:
        item = q.get()
        if item is None:
            q.task_done()
            return
        local_path, key = item
        try:
            sz = local_path.stat().st_size
            s3.upload_file(str(local_path), bucket, key)
            with stats.lock:
                stats.chunks_uploaded += 1
                stats.bytes_uploaded += sz
                stats.last_uploaded_key = key
            local_path.unlink(missing_ok=True)
        except Exception as e:
            print(f"upload failed for {key}: {e}", file=sys.stderr)
        finally:
            q.task_done()


# === DuckDB sink ===============================================================


CREATE_NYC_TABLE = """
CREATE TABLE ebd_nyc (
    category VARCHAR,
    common_name VARCHAR,
    scientific_name VARCHAR,
    subspecies_common_name VARCHAR,
    subspecies_scientific_name VARCHAR,
    exotic_code VARCHAR,
    observation_count VARCHAR,
    country_code VARCHAR,
    state_code VARCHAR,
    county_code VARCHAR,
    locality VARCHAR,
    locality_id VARCHAR,
    locality_type VARCHAR,
    latitude FLOAT,
    longitude FLOAT,
    observation_date DATE,
    sampling_event_identifier VARCHAR,
    protocol_name VARCHAR,
    duration_minutes INTEGER,
    effort_distance_km FLOAT,
    number_observers SMALLINT,
    all_species_reported BOOLEAN,
    group_identifier VARCHAR,
    approved BOOLEAN,
    reviewed BOOLEAN
)
"""


# === Main =====================================================================


def parse_size(s: str) -> int:
    s = s.strip().upper()
    if s.endswith("K"):
        return int(float(s[:-1]) * 1024)
    if s.endswith("M"):
        return int(float(s[:-1]) * 1024 * 1024)
    if s.endswith("G"):
        return int(float(s[:-1]) * 1024 * 1024 * 1024)
    return int(s)


def main() -> int:
    p = argparse.ArgumentParser()
    p.add_argument("--url", required=True, help="EBD .tar URL")
    p.add_argument(
        "--release", default=None, help="Release name for R2 prefix; defaults from URL"
    )
    p.add_argument(
        "--prefix-suffix",
        default="",
        help="Suffix appended to release prefix (e.g. -smoketest)",
    )
    p.add_argument("--nyc-db", default="src/cloaca/swan_lake/dbs/ebd_nyc_smoketest.db")
    p.add_argument(
        "--max-bytes",
        default="0",
        help="Stop after N decompressed bytes (0=no limit). Suffixes K/M/G.",
    )
    p.add_argument("--max-rows", type=int, default=0)
    p.add_argument("--max-chunks", type=int, default=0)
    p.add_argument(
        "--chunk-bytes",
        default="500M",
        help="Rotate Parquet chunk after ~N arrow bytes accumulated.",
    )
    p.add_argument("--block-bytes", default="64M", help="CSV reader block size.")
    p.add_argument("--workdir", default=None)
    p.add_argument(
        "--dry-run-r2",
        action="store_true",
        help="Skip R2 upload (write Parquet to workdir only)",
    )
    args = p.parse_args()

    args.max_bytes = parse_size(args.max_bytes)
    args.chunk_bytes = parse_size(args.chunk_bytes)
    args.block_bytes = parse_size(args.block_bytes)

    load_dotenv()
    account_id = os.environ["R2_ACCOUNT_ID"]
    bucket = os.environ["R2_BUCKET"]
    access_key_id = os.environ["R2_ACCESS_KEY_ID"]
    secret_key = os.environ["R2_SECRET_ACCESS_KEY"]

    release = args.release or Path(args.url.rsplit("/", 1)[-1]).stem
    prefix = f"release={release}{args.prefix_suffix}"

    workdir = (
        Path(args.workdir)
        if args.workdir
        else Path(tempfile.mkdtemp(prefix="ebd-ingest-"))
    )
    workdir.mkdir(parents=True, exist_ok=True)

    nyc_db_path = Path(args.nyc_db)
    nyc_db_path.parent.mkdir(parents=True, exist_ok=True)
    if nyc_db_path.exists():
        nyc_db_path.unlink()

    console = Console()
    stats = Stats()
    stop_event = threading.Event()

    def _sig(_signum, _frame):
        with stats.lock:
            stats.status = "stopping (signal)"
        stop_event.set()

    signal.signal(signal.SIGINT, _sig)
    signal.signal(signal.SIGTERM, _sig)

    s3 = None
    worker = None
    upload_q: queue.Queue = queue.Queue(maxsize=4)
    if not args.dry_run_r2:
        s3 = boto3.client(
            "s3",
            endpoint_url=f"https://{account_id}.r2.cloudflarestorage.com",
            aws_access_key_id=access_key_id,
            aws_secret_access_key=secret_key,
            region_name="auto",
        )
        worker = threading.Thread(
            target=upload_worker,
            args=(upload_q, s3, bucket, stats),
            name="r2-uploader",
            daemon=True,
        )
        worker.start()

    monitor = threading.Thread(
        target=monitor_loop,
        args=(stats, args, stop_event, console),
        name="monitor",
        daemon=True,
    )
    monitor.start()

    con = duckdb.connect(str(nyc_db_path))
    con.execute(CREATE_NYC_TABLE)

    try:
        with urlopen(Request(args.url, method="HEAD"), timeout=30) as r:
            cl = r.headers.get("Content-Length")
            if cl:
                with stats.lock:
                    stats.total_bytes = int(cl)
    except Exception as e:
        print(f"HEAD failed (non-fatal): {e}", file=sys.stderr)

    with stats.lock:
        stats.status = "downloading"

    chunk_writer: Optional[pq.ParquetWriter] = None
    chunk_path: Optional[Path] = None
    chunk_idx = 1
    chunk_arrow_bytes = 0
    nyc_value_set = pa.array(NYC_COUNTIES)

    def open_new_chunk():
        nonlocal chunk_writer, chunk_path, chunk_arrow_bytes
        chunk_path = workdir / f"chunk_{chunk_idx:05d}.parquet"
        chunk_writer = pq.ParquetWriter(
            chunk_path, schema=ARROW_SCHEMA, compression="zstd", compression_level=6
        )
        chunk_arrow_bytes = 0
        with stats.lock:
            stats.chunks_started = chunk_idx
            stats.current_chunk_rows = 0
            stats.current_chunk_bytes = 0

    def finalize_chunk():
        nonlocal chunk_writer, chunk_path, chunk_idx
        if chunk_writer is None or chunk_path is None:
            return
        chunk_writer.close()
        chunk_writer = None
        sz = chunk_path.stat().st_size
        key = f"{prefix}/{chunk_path.name}"
        if not args.dry_run_r2:
            upload_q.put((chunk_path, key))
        else:
            with stats.lock:
                stats.chunks_uploaded += 1
                stats.bytes_uploaded += sz
                stats.last_uploaded_key = f"(dry-run) {chunk_path}"
        chunk_idx += 1

    open_new_chunk()

    raw = urlopen(args.url, timeout=300)
    tracked_raw = TrackingStream(raw, stats)
    tar = tarfile.open(fileobj=tracked_raw, mode="r|")

    inner = None
    for member in tar:
        if member.name.endswith(".txt.gz"):
            inner = tar.extractfile(member)
            break
    if inner is None:
        raise RuntimeError("No .txt.gz member in tar")

    decomp = DecompressTracker(gzip.GzipFile(fileobj=inner, mode="rb"), stats)

    def _on_invalid(row):
        with stats.lock:
            stats.invalid_rows += 1
        return "skip"

    read_options = pa_csv.ReadOptions(block_size=args.block_bytes)
    parse_options = pa_csv.ParseOptions(
        delimiter="\t", quote_char=False, invalid_row_handler=_on_invalid
    )
    convert_options = pa_csv.ConvertOptions(
        column_types=ARROW_TYPES,
        include_columns=INCLUDE_COLS,
        strings_can_be_null=True,
        null_values=[""],
        true_values=["1"],
        false_values=["0"],
    )

    with stats.lock:
        stats.status = "streaming"

    try:
        reader = pa_csv.open_csv(
            decomp,
            read_options=read_options,
            parse_options=parse_options,
            convert_options=convert_options,
        )

        for batch in reader:
            if stop_event.is_set():
                break

            batch = batch.rename_columns(SNAKE_NAMES)
            n_rows = batch.num_rows

            with stats.lock:
                stats.rows_parsed += n_rows

            mask = pc.is_in(batch.column("county_code"), value_set=nyc_value_set)
            nyc = batch.filter(mask)
            if nyc.num_rows > 0:
                con.register("nyc_arrow_view", pa.Table.from_batches([nyc]))
                con.execute("INSERT INTO ebd_nyc SELECT * FROM nyc_arrow_view")
                con.unregister("nyc_arrow_view")
                with stats.lock:
                    stats.nyc_rows_kept += nyc.num_rows

            chunk_writer.write_batch(batch)
            chunk_arrow_bytes += batch.nbytes

            with stats.lock:
                stats.current_chunk_rows += n_rows
                stats.current_chunk_bytes = chunk_arrow_bytes

            if chunk_arrow_bytes >= args.chunk_bytes:
                finalize_chunk()
                open_new_chunk()

            with stats.lock:
                done = (
                    (args.max_bytes and stats.decompressed_bytes >= args.max_bytes)
                    or (args.max_rows and stats.rows_parsed >= args.max_rows)
                    or (args.max_chunks and stats.chunks_uploaded >= args.max_chunks)
                )
            if done:
                stop_event.set()
    finally:
        with stats.lock:
            stats.status = "finalizing"
        finalize_chunk()
        try:
            tar.close()
        except Exception:
            pass
        tracked_raw.close()

        if worker is not None:
            upload_q.put(None)
            upload_q.join()

        con.close()

        with stats.lock:
            stats.status = "done"
        stop_event.set()
        monitor.join(timeout=2)

    s = stats.snapshot()
    console.print(
        f"\n[bold green]Done[/]: {s['rows_parsed']:,} rows "
        f"({s['nyc_rows_kept']:,} NYC) · "
        f"{s['chunks_uploaded']} chunks ({fmt_bytes(s['bytes_uploaded'])}) · "
        f"{fmt_duration(s['elapsed'])}"
    )
    console.print(f"NYC DB: [cyan]{nyc_db_path}[/]")
    console.print(f"R2 prefix: [cyan]r2://{bucket}/{prefix}/[/]")
    if args.dry_run_r2:
        console.print(f"(dry-run) Parquet files retained in [cyan]{workdir}[/]")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
