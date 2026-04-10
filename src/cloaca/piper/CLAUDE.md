# Piper

Piper is a Discord bot that answers birding questions about NYC using Claude, the eBird API, and a local DuckDB database of historical observations.

## Architecture

Piper runs inside the cloaca FastAPI server as an asyncio background task (not a separate service). The startup/shutdown lifecycle is managed in `src/cloaca/main.py`.

### Files

- `main.py` — Discord bot event handlers (`on_ready`, `on_message`), conversation cache, reply-chain context builder. Entry point is the `start()` coroutine.
- `bird_query.py` — Core query logic. Connects to two MCP servers (eBird API via SSE, DuckDB via stdio), builds a tool-augmented Claude conversation, and streams the response. Contains the system prompt and the cached DuckDB connection pool.
- `birdcast.py` — Daily BirdCast migration forecast. Fetches a 3-night forecast from the BirdCast API for NYC, formats a color-coded Discord message (🔵 Low, 🟡 Medium, 🔴 High), and posts to #bird-cast-updates. Scheduled via `discord.ext.tasks.loop` at 22:00 UTC (6 PM EDT). Response is validated with Pydantic models (`BirdcastForecast`, `ForecastNight`).
- `cli.py` — Standalone CLI for testing queries without Discord: `uv run python -m cloaca.piper.cli "what warblers are in prospect park?"`

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

## Environment variables

- `PIPER_DISCORD_BOT_TOKEN` — Discord bot token (required for piper to start)
- `EBIRD_MCP_URL` — SSE endpoint for the eBird MCP server
- `ANTHROPIC_API_KEY` — Claude API key
- `PIPER_DUCK_DB_PATH` — Path to the ebd_nyc.db DuckDB file (optional; enables historical queries)
- `BIRDCAST_API_KEY` — API key for BirdCast forecast endpoint (required for daily forecast)

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

To run the bot standalone (outside of cloaca):
```bash
uv run python -m cloaca.piper.main
```

## Key constants

- `PIPER_BOT_UPDATES_CHANNEL_ID` in `main.py` — Discord channel where Piper posts "I'm up!" on startup (#piper-bot-updates)
- `BIRDCAST_CHANNEL_ID` in `birdcast.py` — Discord channel for daily migration forecasts (#bird-cast-updates)
- `CACHE_MAX_SIZE` in `main.py` — Max conversation cache entries (50)
- `_DUCK_IDLE_SECONDS` in `bird_query.py` — DuckDB connection idle timeout (5 min)
- Claude model used: `claude-sonnet-4-6` (set in `bird_query.py`)
