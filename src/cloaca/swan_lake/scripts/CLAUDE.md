# EBD streaming ingest

This directory holds scripts that turn the monthly eBird Basic Dataset (EBD)
release into the two databases the cloaca app cares about. The historical
manual flow is `parse_ebd.sh` (download `.tar` to disk, stream into a giant
DuckDB) followed by `build_ebd_nyc.sh` (filter the 5 NYC counties out of the
full DB into the small one Piper queries on Render).

`ingest_ebd_streaming.py` is the in-progress replacement: a single-pass
pipeline that streams the tar straight from eBird's URL and produces both the
NYC DuckDB and a remote Parquet copy of the full dataset, without ever
materializing the 100+ GB intermediate `ebd_full.db`. As of this commit the
script is a working **proof of concept** — the smoke test passes
end-to-end but it has not been wired into a schedule, the Render swap step
isn't built, and several optimization decisions are deliberately deferred.

## Architecture

```
URL (HTTPS, public)
  │  urlopen, ~23 MB/s per-IP cap
  ▼
TrackingStream                ← bytes_downloaded counter
  │
  ▼
tarfile (mode='r|')           ← streaming, no random access
  │  extracts inner ebd_relMMM-YYYY.txt.gz
  ▼
gzip.GzipFile + DecompressTracker  ← decompressed_bytes counter
  │
  ▼
pyarrow.csv.open_csv          ← TSV parser, batched RecordBatches
  │
  ├──► NYC filter (county_code in 5 NYC FIPS codes)
  │       └──► DuckDB Appender → src/cloaca/swan_lake/dbs/ebd_nyc.db
  │
  └──► Rolling Parquet writer (zstd, level 6)
          └──► boto3 multipart upload → r2://ebd/release=<rel>/chunk_NNNNN.parquet
                                          (uploaded async by worker thread)
```

A `rich.live.Live` panel renders progress 4×/sec in a TTY (download MB/s,
decompress ratio, rows/sec, NYC hit rate, in-flight chunk size, R2 upload
totals, ETA). Outside a TTY the same numbers print every 5s as plain log
lines. SIGINT flushes the open chunk to R2 and closes the DuckDB cleanly
before exit.

## Why this shape

### Why stream incrementally, not build the full DB first

The locally-built `ebd_relJan-2026.db` is **138 GB** with no compression
tuning. Even after optimization (ENUMs, FLOAT lat/lon, dropping
`global_unique_identifier`, ZSTD-encoded Parquet) the realistic optimized
size is 25–60 GB — still too big to materialize on a default GitHub Actions
runner (~30 GB free after cleanup).

DuckDB's `COPY ... TO 's3://...' (FORMAT PARQUET, PARTITION_BY year)` looks
ideal but partitions force the engine to spill the whole input to its
`temp_directory` before flushing — defeats the small-disk goal.

The fix is a single in-memory fan-out: each Arrow `RecordBatch` is filtered
to NYC and appended to the local DuckDB, *and* written to a rolling Parquet
chunk on disk. When a chunk hits its arrow-bytes threshold, an upload thread
ships it to R2 and the chunk file is deleted. Total disk needed = one
in-flight chunk (~500 MB) + ebd_nyc.db (~300 MB) + DuckDB temp (a few GB).

### Why two outputs

- **NYC subset on Render**: small (~300 MB), cheap, what Piper actually
  queries today. Gets scp'd onto the cloaca service's 1 GB persistent disk
  at `/var/data/ebd_nyc.db`.
- **Full Parquet on R2**: so questions about birds outside the 5 boroughs
  can still be answered. R2 is $0.015/GB-month with $0 egress and queryable
  directly via DuckDB's `httpfs` extension — no download step from a
  laptop or from Piper.

### Why R2 over alternatives

R2 won on cost (~$0.50/mo for ~30 GB of optimized Parquet) and on the
zero-egress story (DuckDB queries from anywhere are free). Other options
considered:

- **MotherDuck**: clean fit but vendor-locked.
- **Render disk on a separate service**: $5+/mo just for storage.
- **GitHub Releases / LFS**: 2 GB per file limit kills it.
- **Local-only on lacie disk**: works for now but doesn't survive without
  the laptop.

## What's been validated

A 300 MB smoke test against `ebd_relMar-2026` confirmed:

- Streaming download from the URL: works at ~23 MB/s single-connection
  (well above the conservative "10 MB/s" estimate the planning was based on).
- Tar member streaming + gzip decompression on the URL stream: works.
- pyarrow CSV parser: works with the eBird TSV (`delimiter='\t'`,
  `quote_char=False`, `column_types` dict, `include_columns` to drop
  `global_unique_identifier`). The `invalid_row_handler='skip'` is wired
  for resilience but didn't fire in the smoke test.
- NYC filter via `pc.is_in`: works; ~0.91% hit rate on the first 770K rows.
- DuckDB Appender via `register(arrow_table) → INSERT INTO`: works; types
  match (FLOAT, DATE, BOOLEAN, etc.).
- Rolling Parquet (ZSTD level 6) → R2 multipart upload via boto3: works.
- Round-trip query: DuckDB `httpfs` reading
  `r2://ebd/release=<rel>/*.parquet` returns the same row count as the
  local DuckDB, with the same schema preserved.
- Live monitoring panel + non-TTY fallback: both work.

## Findings that affect the production design

- **Real source size**: 232 GB compressed for the World tar
  (`ebd_relMar-2026`). The eBird page text says "39+ GB" — that is wildly
  stale.
- **Real download cap**: ~23 MB/s per IP (was estimated at 10 MB/s).
  Resulting wall time for the full tar is **~2.8 hours**, not 6.4h —
  comfortably under the GHA 6h runner limit.
- **Per-IP, not per-connection cap**: tested with 1 vs 4 parallel ranged
  downloads. Aggregate throughput across 4 connections (~21 MB/s) matched
  a single connection (~23 MB/s). **Parallel ranged downloads do not
  help.**
- **Source ordering**: the inner `.txt.gz` is sorted chronologically (the
  first 300 MB was 100% 2011 observations). This means:
  1. The NYC subset cannot be built from a partial stream — we have to
     read the entire 232 GB to capture all 14.7M NYC rows.
  2. Parquet chunks land in date order on R2, so date-range queries
     ("last 5 years") naturally hit only the most recent chunks even
     without explicit partitioning. A future reshuffle pass to
     `year=YYYY/` partitions is straightforward but not blocking.
- **Bottleneck is parsing, not download**: the smoke test sustained
  ~12 MB/s downloaded while pulling at ~23 MB/s would be possible; the
  delta is the pyarrow CSV parser holding the read pipeline back. If
  end-to-end time becomes a problem, parallel decompression via
  `rapidgzip` + `use_threads=True` on the parser would help. Today it
  doesn't matter.

## What's deliberately NOT built yet

- **Schedule / orchestration.** No GHA workflow, no cron, no Render cron
  job. Today the script is run by hand. The deploy plan
  (`~/.claude/plans/i-wanted-to-explore-joyful-walrus.md`) covers options
  (self-hosted GHA on the lacie machine vs. an ephemeral VPS spun up by
  GHA vs. a Render cron service). No decision yet.
- **Render scp + atomic swap + redeploy.** The script writes
  `ebd_nyc.db` locally; nothing pushes it to `/var/data/ebd_nyc.db.new`
  on the cloaca service or triggers a redeploy. Memory entries
  `reference_render_scp.md` and `reference_render_deploy.md` have the
  command shapes.
- **URL-pattern handling.** The URL is hard-coded per run; production
  needs to compute `ebd_relMMM-YYYY.tar` from the current date (eBird
  releases on the 15th).
- **ENUM-backed NYC schema.** The smoke test uses plain VARCHAR for what
  `build_ebd_nyc.sh` casts to `category_t`, `protocol_t`,
  `locality_type_t`, `exotic_code_t`. Worth restoring before this
  replaces `build_ebd_nyc.sh` so downstream Piper queries don't break.
- **Schema sort within chunks.** Each Parquet chunk is currently written
  in input order. A cheap per-chunk sort by
  `(state_code, observation_date)` would meaningfully improve column
  compression — never measured.
- **Stage 2 reshuffle.** Optional follow-up to repartition the input-order
  chunks into `year=YYYY/` (or finer) Parquet partitions for query
  pruning. Easy to express as a GHA matrix where each worker handles a
  range of chunks.
- **Cleanup of smoke-test artifacts.** Local
  `src/cloaca/swan_lake/dbs/ebd_nyc_smoketest.db` and the
  `release=<rel>-smoketest/` R2 prefix are still around for inspection.

## Running it

The EBD download URL is sensitive and is **never** committed to the repo. It
lives in `EBD_TAR_URL` in your local `.env` and as the `EBD_TAR_URL` GitHub
Actions secret. The examples below show `"$EBD_TAR_URL"` instead of the
literal URL — make sure your shell has it loaded (`set -a; source .env;
set +a`) before running.

```bash
# Smoke test (~300 MB decompressed, ~15 seconds, 2 chunks to R2)
uv run src/cloaca/swan_lake/scripts/ingest_ebd_streaming.py \
  --url "$EBD_TAR_URL" \
  --max-bytes 300M --chunk-bytes 80M --block-bytes 16M \
  --prefix-suffix=-smoketest

# Skip R2 entirely and write Parquet to a local workdir
uv run src/cloaca/swan_lake/scripts/ingest_ebd_streaming.py \
  --url "$EBD_TAR_URL" --dry-run-r2 --workdir /tmp/ebd-out --max-bytes 50M

# Full run (no caps) — streaming mode, ~2.8h, no resume on network drop
uv run src/cloaca/swan_lake/scripts/ingest_ebd_streaming.py \
  --url "$EBD_TAR_URL"

# Full run with disk + R2 raw mirror (resumable)
uv run src/cloaca/swan_lake/scripts/ingest_ebd_streaming.py \
  --url "$EBD_TAR_URL" \
  --cache-dir /Volumes/lacie_disk/ebd-cache
# or set EBD_CACHE_DIR=/Volumes/lacie_disk/ebd-cache in .env and drop --cache-dir
```

## --cache-dir mode (resumable)

Adds a Stage 1 that materializes the raw release somewhere durable before
processing. Stage 1 does two things:

1. **Disk download with HTTP Range resume.** Writes
   `<cache-dir>/<basename>.tar.partial` chunk by chunk. On disconnect /
   re-run, sends a `Range: bytes=<local_size>-` and appends. Atomic-renames
   to `<basename>.tar` once the on-disk size matches the URL's
   `Content-Length`.

2. **R2 raw mirror via boto3 multipart.** After the disk file is complete,
   uploads to `r2://<bucket>/raw/<basename>.tar`. boto3 handles multipart
   internally with 4-way concurrency and 64 MiB parts. If
   `s3.head_object(...)` already returns the object at the expected
   ContentLength, the upload is skipped.

Stage 2 then opens the local `.tar` and runs the existing fan-out pipeline
(NYC DuckDB + rolling Parquet chunks to `r2://<bucket>/release=…/`).

Re-running with the same `--cache-dir` is fully idempotent:

| State on retry | Stage 1 behavior |
|---|---|
| `.tar` exists, full size | skip disk download |
| `.tar.partial` exists, partial size | resume HTTP from that offset |
| `.tar.partial` somehow > total | truncate, restart |
| R2 raw object exists, full size | skip R2 upload |
| R2 raw object missing or wrong size | re-upload from disk |
| Stage 2 crashed mid-run | re-run reuses the disk file; chunks overwrite by name |

### What's not (yet) resumable

- **Stage 2 crashes during decompression / parsing don't checkpoint
  parser state.** A re-run starts the .tar from byte 0 again. Cheap
  relative to network download (disk reads are 100s of MB/s, not 23 MB/s),
  but not free.
- **Mid-multipart R2 upload crashes restart the whole boto3
  upload_file.** boto3 doesn't currently persist multipart state across
  process exits; aborted multiparts may linger on R2 with no default
  lifecycle. Configure an R2 lifecycle rule to abort incomplete
  multiparts after N days if you re-run a lot.
- **Disk-download response stream stalls without closing.** The 300s
  timeout on `urlopen(..., timeout=300)` covers connect; for stalled
  bodies you may need to Ctrl+C and let the resume logic restart from
  the on-disk size.

`uv run` reads the PEP 723 inline metadata block at the top of the script
and provisions `pyarrow`, `boto3`, `duckdb`, `rich`, `python-dotenv` into an
ephemeral venv. The script reads R2 credentials from `.env` at the repo
root (`R2_ACCOUNT_ID`, `R2_BUCKET`, `R2_ACCESS_KEY_ID`,
`R2_SECRET_ACCESS_KEY`).

Querying the R2-hosted Parquet from anywhere with DuckDB:

```sql
INSTALL httpfs; LOAD httpfs;
CREATE OR REPLACE SECRET ebd_r2 (
    TYPE r2,
    KEY_ID '...', SECRET '...', ACCOUNT_ID '...'
);
SELECT count(*) FROM read_parquet('r2://ebd/release=ebd_relMar-2026/*.parquet');
```
