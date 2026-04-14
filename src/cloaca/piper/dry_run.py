"""Dry-run mode for piper local development.

Run with: uv run python -m cloaca.piper.dry_run

Runs the real check_year_lifers loop from main.py against a local Postgres
and the real eBird API, but patches bot.get_channel to return a mock channel
that prints messages to stdout instead of posting to Discord.

Pass --backfill to also run the year/all-time backfill (makes ~100+ eBird
API calls per hotspot, skipped by default).

Requires:
  - DATABASE_URL pointing at a local Postgres (default: docker-compose)
  - EBIRD_API_KEY set in .env
"""

import argparse
import asyncio
import logging
from unittest.mock import AsyncMock, MagicMock, patch

from cloaca.piper.db_pool import close_engine
from cloaca.piper.year_lifers import WATCHED_HOTSPOTS

logger = logging.getLogger(__name__)

# Map channel IDs to hotspot names for display
_CHANNEL_NAMES: dict[int, str] = {
    h.channel_id: h.name.lower().replace(" ", "-") for h in WATCHED_HOTSPOTS
}
# Add the rare bird alert channel
from cloaca.piper.main import RARE_BIRD_ALERT_CHANNEL_ID

_CHANNEL_NAMES[RARE_BIRD_ALERT_CHANNEL_ID] = "rare-bird-alerts"


def _make_mock_channel(channel_id: int) -> AsyncMock:
    """Create a mock channel that prints send() calls to stdout."""
    channel_name = _CHANNEL_NAMES.get(channel_id, f"channel-{channel_id}")
    prefix = f"  [#{channel_name}]"

    async def fake_send(content="", **kwargs):
        for line in content.split("\n"):
            print(f"{prefix} {line}")
        if view := kwargs.get("view"):
            for item in getattr(view, "children", []):
                if hasattr(item, "url") and hasattr(item, "label"):
                    print(f"{prefix}   -> {item.label}: {item.url}")
        print()

    ch = AsyncMock()
    ch.id = channel_id
    ch.send = fake_send
    return ch


async def main():
    parser = argparse.ArgumentParser(description="Piper dry-run mode")
    parser.add_argument(
        "--backfill",
        action="store_true",
        help="Run backfill (makes ~100+ eBird API calls per hotspot)",
    )
    parser.add_argument(
        "--rare-birds",
        action="store_true",
        help="Run rare bird alert check for NYC (ABA Code 3+)",
    )
    args = parser.parse_args()

    from cloaca.piper import main as piper_main

    # Patch bot.get_channel to return our printing mock
    mock_bot = MagicMock()
    mock_bot.get_channel = lambda cid: _make_mock_channel(cid)

    try:
        with patch.object(piper_main, "bot", mock_bot):
            if args.backfill:
                from cloaca.piper.year_lifers import (
                    backfill_all_time_species,
                    backfill_year_species,
                )

                for hotspot in WATCHED_HOTSPOTS:
                    print(f"\nBackfilling {hotspot.name}...")
                    await backfill_year_species(hotspot.id)
                    await backfill_all_time_species(hotspot.id)

            if args.rare_birds:
                # Run the rare bird alert check once
                await piper_main.check_rare_bird_alerts.coro()
            else:
                # Run the real check_year_lifers loop once
                await piper_main.check_year_lifers.coro()
    finally:
        await close_engine()


if __name__ == "__main__":
    from dotenv import load_dotenv

    load_dotenv(override=True)
    logging.basicConfig(level=logging.INFO)
    asyncio.run(main())
