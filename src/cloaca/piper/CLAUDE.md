# Piper

Piper is a Discord bot that answers birding questions about NYC using Claude, the eBird API, and a local DuckDB database of historical observations.

## Architecture

Piper runs inside the cloaca FastAPI server as an asyncio background task (not a separate service). The startup/shutdown lifecycle is managed in `src/cloaca/main.py`.

### Files

- `main.py` — Discord bot event handlers (`on_ready`, `on_message`), conversation cache, reply-chain context builder. Entry point is the `start()` coroutine.
- `bird_query.py` — Core query logic. Connects to two MCP servers (eBird API via SSE, DuckDB via stdio), builds a tool-augmented Claude conversation, and streams the response. Contains the system prompt and the cached DuckDB connection pool.
- `birdcast.py` — Daily BirdCast migration forecast. Fetches a 3-night forecast from the BirdCast API for NYC, formats a color-coded Discord message (🔵 Low, 🟡 Medium, 🔴 High), and posts to #bird-cast-updates. Scheduled via `discord.ext.tasks.loop` at 7:00 AM Eastern. Response is validated with Pydantic models (`BirdcastForecast`, `ForecastNight`).
- `cli.py` — Standalone CLI for testing queries without Discord: `uv run python -m cloaca.piper.cli "what warblers are in prospect park?"`
- `year_lifers.py` — Year lifer and all-time park lifer tracking for multiple hotspots. Polls the eBird historic observations API every 15 minutes (hourly at night) to detect new species. Observations are fetched once per hotspot and checked against both the year list and all-time list. Stores state in a writable DuckDB state DB (`PIPER_STATE_DB_PATH`). On first run, backfills the current year day-by-day from the historic API, and backfills all-time species via the `product/spplist` endpoint (single API call per hotspot). Posts Discord notifications per hotspot channel — celebratory 🎉🥳 for all-time lifers, bird emojis for year lifers. If a species is both a year lifer and an all-time lifer, only the all-time notification is posted. Hotspots are configured via the `WATCHED_HOTSPOTS` list of `Hotspot` dataclasses. Reuses `eBirdHistoricFullObservation` from `scripts/fetch_yearly_hotspot_data.py` and `get_phoebe_client()` from `api/shared.py`.

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

**State DB**: A separate writable DuckDB file (`PIPER_STATE_DB_PATH`) stores species tracking tables. This is independent of the large read-only `ebd_nyc.db`. Tables:
- `hotspot_year_species` — year lifer tracking (species code, first observation date, observer name, checklist ID). PK: `(hotspot_id, year, species_code)`.
- `hotspot_all_time_species` — all-time park species (species code only). PK: `(hotspot_id, species_code)`. Backfilled via the eBird `product/spplist` API.
- `backfill_status` — tracks backfill completion per hotspot. PK: `(hotspot_id, year)`. Uses `year=0` as sentinel for all-time backfills.

**Year backfill**: On first startup per hotspot (tracked in `backfill_status` table), iterates Jan 1 through yesterday calling the eBird API day-by-day (~100 calls per hotspot, 0.5s delay between each). Subsequent startups skip completed hotspots.

**All-time backfill**: Single API call to `product/spplist/{hotspot_id}` returns all species codes ever recorded. Tracked in `backfill_status` with `year=0`.

**Polling**: Every 15 minutes, fetches observations for today and yesterday (2 API calls per hotspot). The same observations are checked against both the year list and all-time list — no extra API calls. During night hours (10pm–6am ET), enforces at least 1 hour between checks. New species are inserted and posted to the hotspot's Discord channel. All-time lifers take priority: if a species is both a year lifer and an all-time lifer, only the all-time notification is posted.

**Year rollover**: On Jan 1, the table is empty for the new year, triggering a backfill of 0 days. The regular poll picks up the first species naturally.

### Scheduled tasks

| Task | Schedule | Channel | Module |
|------|----------|---------|--------|
| BirdCast forecast | Daily at 7:00 AM Eastern | #bird-cast-updates | `birdcast.py` |
| Lifer check (year + all-time) | Every 15 min (hourly at night) | Per hotspot (see `WATCHED_HOTSPOTS`) | `year_lifers.py` |

Both are started in `on_ready()` via `discord.ext.tasks.loop`.

## Environment variables

- `PIPER_DISCORD_BOT_TOKEN` — Discord bot token (required for piper to start)
- `EBIRD_MCP_URL` — SSE endpoint for the eBird MCP server
- `ANTHROPIC_API_KEY` — Claude API key
- `PIPER_DUCK_DB_PATH` — Path to the ebd_nyc.db DuckDB file (optional; enables historical queries)
- `BIRDCAST_API_KEY` — API key for BirdCast forecast endpoint (required for daily forecast)
- `PIPER_STATE_DB_PATH` — Path to the writable DuckDB state file for year lifers (defaults to `piper_state.db`)

## Local development

Piper starts automatically with cloaca (`uv run fastapi dev src/cloaca/main.py`) as long as `PIPER_DISCORD_BOT_TOKEN` is set in `.env`.

To test queries without the Discord bot:
```bash
uv run python -m cloaca.piper.cli "when do eastern phoebes usually arrive in central park?"
```

To test the BirdCast forecast message locally:
```bash
uv run python -m cloaca.piper.birdcast
```

To test the year lifers feature (backfill + poll) without Discord:
```bash
uv run python -m cloaca.piper.year_lifers
```

To run the bot standalone (outside of cloaca):
```bash
uv run python -m cloaca.piper.main
```

## Key constants

- `PIPER_BOT_UPDATES_CHANNEL_ID` in `main.py` — Discord channel where Piper posts "I'm up!" on startup (#piper-bot-updates)
- `BIRDCAST_CHANNEL_ID` in `birdcast.py` — Discord channel for daily migration forecasts (#bird-cast-updates)
- `WATCHED_HOTSPOTS` in `year_lifers.py` — List of `Hotspot` dataclasses (id, name, channel_id) for year lifer tracking
- `CACHE_MAX_SIZE` in `main.py` — Max conversation cache entries (50)
- `_DUCK_IDLE_SECONDS` in `bird_query.py` — DuckDB connection idle timeout (5 min)
- Claude model used: `claude-sonnet-4-6` (set in `bird_query.py`)
