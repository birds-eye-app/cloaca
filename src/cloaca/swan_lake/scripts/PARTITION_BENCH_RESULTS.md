# EBD R2 partition benchmark — sample-30 results

Comparing three R2 dataset shapes via `bench_partitions.py`.

**Sample**: 30 random chunks (~3.6%) of the full `ebd_relMar-2026` release. 78.5M rows, full 253-country coverage, full date range.

## Storage

| Shape | Path | Size | Files |
|---|---|---:|---:|
| unpartitioned | `r2://ebd/release=sample-30/` | 2.89 GB | 30 |
| partitioned | `r2://ebd/partitioned-by-state/release=sample-30/` | **1.21 GB** | 3,481 |
| native-duckdb | `r2://ebd/duckdb/sample-30.duckdb` | 2.61 GB | 1 |

Notable: partitioned is the *smallest* on disk because sort-clustered ZSTD compresses much better than unsorted.

## Cold-start (first query against fresh DuckDB connection)

| Query | unpart | partitioned | native | winner |
|---|---:|---:|---:|---|
| q1 California state aggregate | 93.5s | 6.4s | 7.2s | partitioned/native |
| q2 Central Park hotspot | 33.8s | 82.2s | 25.2s | native |
| q3 NY state + 2024+ | 9.6s | 3.5s | 2.0s | native |
| q4 Bald Eagle by country | 8.1s | 32.0s | 23.5s | unpart |
| q5 Owl pattern scan | 5.0s | 53.2s | 17.8s | unpart |
| q6 NY state + Eastern Phoebe | 7.1s | 2.8s | 1.4s | native |

## Warm (subsequent queries on same DuckDB connection)

| Query | unpart | partitioned | native |
|---|---:|---:|---:|
| q1 | 1.13s | 2.29s | **0.08s** |
| q2 | 0.35s | 2.28s | 0.49s |
| q3 | 0.21s | 2.42s | **0.00s** |
| q4 | 0.22s | 2.01s | **0.03s** |
| q5 | 0.52s | 1.93s | **0.05s** |
| q6 | 0.86s | 1.83s | **0.01s** |

## Findings

### 1. Native DuckDB dominates warm queries

10×–86× faster than partitioned Parquet. Single-file metadata caching + sort clustering wins outright.

### 2. Partitioned Parquet *underperforms* for non-region queries

q2 hotspot, q4 country aggregate, q5 owl scan are all worse on partitioned than on plain unpartitioned. Cause: 3,481 file footers × ~1 round-trip-equivalent overhead each > the bytes savings from partition pruning.

The "small-files problem" — even a perfectly-sorted partition layout can be a net loss if the partition cardinality is high enough that per-file overhead dominates per-byte work.

### 3. Sort clustering > partition tree

Native is a single file with the same sort order as partitioned. It beats partitioned on every single query.

### 4. Cold-start time tracks file count

Approximate cold-start time scales with the number of file footers DuckDB has to fetch. Native is fastest because there's just one file.

## Recommendation for this project

**Switch to native DuckDB on R2** as the primary "all-the-data" artifact for the cloaca + Piper use case. The single-file shape:

- Matches the access pattern (DuckDB-native queries from laptop / Render / future cron jobs)
- Bites a 9-min upload at build time (one-time per monthly release)
- Pays it back many times over on every subsequent query

Keep `r2://ebd/raw/<release>.tar` as the canonical raw archive. Keep `r2://ebd/release=<release>/*.parquet` as a one-shot transient if needed (otherwise the new monthly job can write straight to native-DuckDB after Stage 1).

## Things this benchmark didn't cover

- **Different partition cardinality** — country-only Hive partitioning (~250 buckets, larger files per bucket) might fix the small-files problem and could match or beat native. Worth a third dataset experiment.
- **Compute geographically close to R2** — every number above includes residential-NYC ↔ Cloudflare RTT. A runner in a Cloudflare-friendly region (Workers / Hetzner near a major IX / AWS us-east-1) would change all these timings, and might shift the winner.
- **Concurrent access** — only single-reader. Native DuckDB's `ATTACH … READ_ONLY` may behave differently under concurrent connections.
- **Update path** — every new monthly release fully replaces the .duckdb file. There is no "append" story; that's a real downside if you want incremental ingest.
- **Sample bias** — 30 random chunks of 825 covers all 253 countries but the sample's per-state row counts are 30/825 of the full. Per-query absolute numbers are not directly comparable to full-dataset numbers; relative ordering should hold.

## How to reproduce

```
# Build the sample (server-side R2 copy)
EBD_TAR_URL=… python -c "..." # see issue #44 for snippet

# Build the partitioned variant
uv run src/cloaca/swan_lake/scripts/repartition_ebd.py \
    --release sample-30 partitioned-parquet --sort

# Build the native-DuckDB variant
uv run src/cloaca/swan_lake/scripts/repartition_ebd.py \
    --release sample-30 native-duckdb

# Run the benchmark
PYTHONUNBUFFERED=1 uv run src/cloaca/swan_lake/scripts/bench_partitions.py \
    --release sample-30
```
