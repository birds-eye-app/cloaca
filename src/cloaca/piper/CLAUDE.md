# Piper

Piper is a Discord bot that answers birding questions about NYC using Claude, the eBird API, and a local DuckDB database of historical observations.

## Architecture

Piper runs inside the cloaca FastAPI server as an asyncio background task (not a separate service). The startup/shutdown lifecycle is managed in `src/cloaca/main.py`.

### Files

- `main.py` — Discord bot event handlers (`on_ready`, `on_message`), conversation cache, reply-chain context builder. Entry point is the `start()` coroutine.
- `bird_query.py` — Core query logic. Connects to two MCP servers (eBird API via SSE, DuckDB via stdio), builds a tool-augmented Claude conversation, and streams the response. Contains the system prompt and the cached DuckDB connection pool.
- `birdcast.py` — Daily BirdCast migration forecast and nightly migration traffic report. The **forecast** fetches a 3-night forecast from the BirdCast alert API for NYC, formats a color-coded Discord message (🔵 Low, 🟡 Medium, 🔴 High), and posts to #bird-cast-updates. Polls every 15 minutes starting at 7:00 AM Eastern until the forecast updates for today, then stops retrying. Posts are deduplicated via the `birdcast_post_log` table. The **migration traffic report** fetches last night's live migration data from the BirdCast dashboard API (`livemigration` endpoint for Kings County / US-NY-047), and posts a summary (total birds crossed, peak traffic with compass direction) to the same channel at 6:00 AM Eastern daily.
- `cli.py` — Standalone CLI for testing queries without Discord: `uv run python -m cloaca.piper.cli "what warblers are in prospect park?"`
- `year_lifers.py` — Year lifer and all-time park lifer tracking for multiple hotspots. Polls the eBird historic observations API every 15 minutes (hourly at night) to detect new species. Observations are fetched once per hotspot and checked against both the year list and all-time list. Stores state in Postgres (`DATABASE_URL`). On first run, backfills the current year day-by-day from the historic API, and backfills all-time species via the `product/spplist` endpoint (single API call per hotspot). Posts Discord notifications per hotspot channel — celebratory 🎉🥳 for all-time lifers, bird emojis for year lifers. If a species is both a year lifer and an all-time lifer, only the all-time notification is posted. Hotspots are configured via the `WATCHED_HOTSPOTS` list of `Hotspot` dataclasses. Reuses `eBirdHistoricFullObservation` from `scripts/fetch_yearly_hotspot_data.py` and `get_phoebe_client()` from `api/shared.py`.
- `rare_bird_alert.py` — Automated rare bird alert for NYC. Polls the eBird notable observations endpoint for all 5 NYC counties (Bronx, Brooklyn, Manhattan, Queens, Staten Island) every 15 minutes. Cross-references observations against a static ABA code lookup table (`data/aba_codes.csv`) and only alerts on species with ABA Code >= 3 (Rare, Casual, Accidental). Deduplicates via the `rare_bird_alerts` Postgres table — same species + region won't re-alert within 7 days. Posts to a dedicated Discord channel with ABA code label and emoji severity (🔔 Rare, ❗ Casual, 🚨 Accidental). Can be tested standalone: `uv run python -m cloaca.piper.rare_bird_alert`.
- `data/aba_codes.csv` — Static mapping of eBird species codes to ABA rarity codes (Code 3–6 only). Update this file when the ABA publishes checklist revisions. Download the latest from https://www.aba.org/aba-checklist/.
- `db_pool.py` — SQLAlchemy async engine management for the Postgres connection pool. Provides `get_engine()` and `close_engine()`.
- `dry_run.py` — Local dev mode. Runs the full lifer-check loop (backfill + poll + provisional review) against a local Postgres, printing Discord messages to stdout instead of posting them. Run with `uv run python -m cloaca.piper.dry_run`.
- `sql/schema.sql` — Postgres table definitions (source of truth for sqlc codegen).
- `sql/queries.sql` — Annotated SQL queries (source of truth for sqlc codegen).
- `db/` — Auto-generated code from sqlc. Contains `models.py` (dataclasses) and `queries.py` (typed `AsyncQuerier` class). Regenerate with `sqlc generate`. Do not edit `models.py`; the import in `queries.py` has been patched to use the full package path.

### How a query flows

1. User @mentions or replies to Piper in Discord
2. `on_message` extracts the query text and builds conversation context (from in-memory cache or by walking the Discord reply chain)
3. `ask_bird_query()` opens an SSE connection to the eBird MCP server, optionally reuses a cached DuckDB MCP connection, and runs Claude with those tools
4. Claude can call eBird API tools (recent sightings, hotspots, species lists) and DuckDB tools (historical queries against 14.7M NYC observation records)
5. The response is posted as a Discord reply with a stats footer (time, tokens, cost, tool calls)

### Conversation continuity

Piper maintains a 50-entry in-memory cache mapping bot reply message IDs to full Claude message histories. When a user replies to a bot message:
- **Cache hit**: the full message history is passed to Claude for true multi-turn conversation
- **Cache miss**: Piper walks the Discord reply chain (up to 5 messages) and reconstructs a text summary as context

### DuckDB connection caching

The DuckDB MCP server (mcp-server-motherduck) is spawned as a subprocess. To avoid respawning it on every query, `bird_query.py` keeps a cached connection with:
- A use counter to prevent closing during in-flight queries
- A 5-minute idle timer that closes the subprocess when unused
- A global lock for thread-safe access

### Year lifer tracking

`year_lifers.py` tracks first-of-year species sightings at watched hotspots. It uses the eBird historic observations API (`detail=full`, `rank=create`) which returns observer names — the standard recent observations endpoint does not.

**Watched hotspots** are defined in the `WATCHED_HOTSPOTS` list. Each `Hotspot` has an eBird ID, display name, and Discord channel ID. Currently:
- McGolrick Park (`L2987624`) → #mcgolrick-park
- Franz Sigel Park (`L1814508`) → #franz-sigel-park

To add a new hotspot, append a `Hotspot` entry to the list.

**State DB**: Postgres (via `DATABASE_URL`). Managed by Alembic migrations (`alembic/versions/`). Queries are generated by sqlc from `sql/queries.sql`. Tables:
- `hotspot_year_species` — year lifer tracking (species code, first observation date, observer name, checklist ID). PK: `(hotspot_id, year, species_code)`.
- `hotspot_all_time_species` — all-time park species (species code only). PK: `(hotspot_id, species_code)`. Backfilled via the eBird `product/spplist` API.
- `backfill_status` — tracks backfill completion per hotspot. PK: `(hotspot_id, year)`. Uses `year=0` as sentinel for all-time backfills.
- `pending_provisional_lifers` — provisional observations awaiting eBird review. PK: `(hotspot_id, species_code, lifer_type)`.
- `birdcast_post_log` — tracks which forecast dates have been posted per location, to prevent duplicate posts. PK: `(location, forecast_date)`.
- `rare_bird_alerts` — deduplication for rare bird alerts. Tracks which species+region combos have been alerted recently. PK: `(species_code, region_code, obs_date)`. A 7-day lookback window prevents re-alerting on the same species.

**Changing the schema**: Edit `sql/schema.sql` and `sql/queries.sql`, run `sqlc generate`, fix the import in `db/queries.py` (`from db import models` → `from cloaca.piper.db import models`), and create a new Alembic migration.

**Year backfill**: On first startup per hotspot (tracked in `backfill_status` table), iterates Jan 1 through yesterday calling the eBird API day-by-day (~100 calls per hotspot, 0.5s delay between each). Subsequent startups skip completed hotspots.

**All-time backfill**: Single API call to `product/spplist/{hotspot_id}` returns all species codes ever recorded. Tracked in `backfill_status` with `year=0`.

**Polling**: Every 15 minutes, fetches observations for today and yesterday via the historic endpoint (2 API calls per hotspot), plus the notable/rarities endpoint (1 API call per hotspot). The historic endpoint detects new species (one record per species, deduplicated). The notable endpoint provides full review status for rare species — every individual report with `obsValid` and `obsReviewed`. Common species (not in notable) are auto-confirmed. Rare species with only `obsValid=False` reports are provisional (awaiting eBird reviewer approval). During night hours (10pm–6am ET), enforces at least 1 hour between checks. New species are inserted and posted to the hotspot's Discord channel. All-time lifers take priority: if a species is both a year lifer and an all-time lifer, only the all-time notification is posted.

**Year rollover**: On Jan 1, the table is empty for the new year, triggering a backfill of 0 days. The regular poll picks up the first species naturally.

### Scheduled tasks

| Task | Schedule | Channel | Module |
|------|----------|---------|--------|
| BirdCast forecast | Every 15 min, 7 AM–noon Eastern (stops after posting) | #bird-cast-updates | `birdcast.py` |
| Migration traffic report | Daily at 6:00 AM Eastern | #bird-cast-updates | `birdcast.py` |
| Lifer check (year + all-time) | Every 15 min (hourly at night) | Per hotspot (see `WATCHED_HOTSPOTS`) | `year_lifers.py` |
| Rare bird alert (NYC) | Every 15 min | `RARE_BIRD_ALERT_CHANNEL_ID` | `rare_bird_alert.py` |

All are started in `on_ready()` via `discord.ext.tasks.loop`.

## Environment variables

- `PIPER_DISCORD_BOT_TOKEN` — Discord bot token (required for piper to start)
- `PIPER_POSTGRES_DB_URL` — Postgres connection string for lifer state (required; falls back to `DATABASE_URL`)
- `EBIRD_MCP_URL` — SSE endpoint for the eBird MCP server
- `ANTHROPIC_API_KEY` — Claude API key
- `PIPER_DUCK_DB_PATH` — Path to the ebd_nyc.db DuckDB file (optional; enables historical queries)
- `BIRDCAST_API_KEY` — API key for BirdCast forecast endpoint (required for daily forecast)

## Local development

### Prerequisites

Start Postgres locally:
```bash
docker compose up -d
```

Set `PIPER_POSTGRES_DB_URL` (or `DATABASE_URL`) in your `.env`:
```
PIPER_POSTGRES_DB_URL=postgresql://piper:piper@localhost:5432/piper_state
```

Run the Alembic migration:
```bash
uv run alembic upgrade head
```

### Running piper

Piper starts automatically with cloaca (`uv run fastapi dev src/cloaca/main.py`) as long as `PIPER_DISCORD_BOT_TOKEN` is set in `.env`.

To test the lifer check loop locally without Discord (dry-run mode — prints messages to stdout):
```bash
uv run python -m cloaca.piper.dry_run
```

To test queries without the Discord bot:
```bash
uv run python -m cloaca.piper.cli "when do eastern phoebes usually arrive in central park?"
```

To test the BirdCast forecast message locally:
```bash
uv run python -m cloaca.piper.birdcast
```

To run the bot standalone (outside of cloaca):
```bash
uv run python -m cloaca.piper.main
```

### Running tests

Tests use testcontainers to start a Postgres container automatically (requires Docker):
```bash
uv run python -m pytest tests/piper/
```

To use the docker-compose Postgres instead (faster, no container startup per session):
```bash
TEST_DATABASE_URL=postgresql://piper:piper@localhost:5432/piper_state uv run python -m pytest tests/piper/
```

### Changing the database schema

1. Edit `sql/schema.sql` (table definitions) and `sql/queries.sql` (queries)
2. Run `sqlc generate` to regenerate `db/models.py` and `db/queries.py`
3. Fix the import in `db/queries.py`: change `from db import models` to `from cloaca.piper.db import models`
4. Create a new Alembic migration: `uv run alembic revision -m "description"`
5. Edit the migration file in `alembic/versions/`
6. Apply: `uv run alembic upgrade head`

## Key constants

- `PIPER_BOT_UPDATES_CHANNEL_ID` in `main.py` — Discord channel where Piper posts "I'm up!" on startup (#piper-bot-updates)
- `BIRDCAST_CHANNEL_ID` in `birdcast.py` — Discord channel for daily migration forecasts (#bird-cast-updates)
- `RARE_BIRD_ALERT_CHANNEL_ID` in `main.py` — Discord channel for ABA Code 3+ rare bird alerts
- `NYC_COUNTIES` in `rare_bird_alert.py` — List of NYC county eBird region codes to poll
- `MIN_ABA_CODE` in `rare_bird_alert.py` — Minimum ABA code to trigger alerts (default: 3)
- `WATCHED_HOTSPOTS` in `year_lifers.py` — List of `Hotspot` dataclasses (id, name, channel_id) for year lifer tracking
- `CACHE_MAX_SIZE` in `main.py` — Max conversation cache entries (50)
- `_DUCK_IDLE_SECONDS` in `bird_query.py` — DuckDB connection idle timeout (5 min)
- Claude model used: `claude-sonnet-4-6` (set in `bird_query.py`)
