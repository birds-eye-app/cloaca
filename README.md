# cloaca

## Achievements

Turn a personal eBird export (`MyEBirdData.csv`) into an achievement timeline —
lifers (with milestone numbers), patch firsts, region firsts, first-of-years,
reunions with long-unseen species, big days, streaks, passport stamps, and more.
Everything is tiered (🏆 gold / ✨ silver / 🌱 bronze) and replayed
deterministically, so diffing "what did my latest export unlock?" is just a
date filter.

```bash
# trophy case + last 30 days
uv run python -m cloaca.achievements.cli src/cloaca/MyEBirdData.csv

# what did I unlock since my previous export?
uv run python -m cloaca.achievements.cli src/cloaca/MyEBirdData.csv --since 2025-06-01

# the whole timeline, from spark day onward
uv run python -m cloaca.achievements.cli src/cloaca/MyEBirdData.csv --full
```

Also served as an API endpoint: `POST /v1/achievements` (multipart CSV upload,
optional `since` query param).

Patches (McGolrick Park, My House) and regions (New York, Martha's Vineyard)
are configured in `src/cloaca/achievements/config.py` — edit the location IDs
and region codes there to define your own.

## Development

Run the application:
```bash
uv run fastapi dev src/cloaca/main.py
```

## Testing

Run all tests:
```bash
python -m pytest
```

Run tests for a specific module:
```bash
python -m pytest tests/api/ -v
python -m pytest tests/parsing/ -v
python -m pytest tests/scripts/ -v
```

Run tests with verbose output:
```bash
python -m pytest -v
```

Note: Some tests in `tests/db/` may have import issues and might need to be run individually or skipped.

## 
todos:

duckdb stuff: 
- [ ] clean up duplication of logging / tracing
- [ ] connection pooling