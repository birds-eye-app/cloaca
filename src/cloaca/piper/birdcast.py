import datetime
import logging
import os
import random

import aiohttp
import discord
from pydantic import BaseModel

logger = logging.getLogger(__name__)

BIRDCAST_API_BASE = (
    "https://alert.birdcast.org/api/is-birdcast-alert-api"
    "/40.7127753,-74.0059728/birdcast"
)
BIRDCAST_PAGE_URL = (
    "https://alert.birdcast.org/"
    "?latLng=40.7127753,-74.0059728"
    "&locName=New%20York,%20NY,%20USA"
)
BIRDCAST_CHANNEL_ID = 1492204794931052666

BIRD_EMOJIS = [
    "\U0001f426",  # bird
    "\U0001f985",  # eagle
    "\U0001f989",  # owl
    "\U0001f99c",  # parrot
    "\U0001f9a9",  # flamingo
    "\U0001f427",  # penguin
    "\U0001fab6",  # feather
    "\U0001f54a\ufe0f",  # dove
    "\U0001f423",  # hatching chick
    "\U0001fabd",  # wing
]

CODE_TO_TIER = {
    1: ("Low", "\U0001f535"),  # blue circle
    2: ("Medium", "\U0001f7e1"),  # yellow circle
    3: ("High", "\U0001f534"),  # red circle
}


class ForecastNight(BaseModel):
    date: datetime.datetime
    total: int
    code: int


class BirdcastForecast(BaseModel):
    generatedDate: datetime.datetime
    forecastNights: list[ForecastNight]


async def fetch_birdcast_forecast() -> BirdcastForecast | None:
    key = os.environ.get("BIRDCAST_API_KEY")
    if not key:
        logger.warning("BIRDCAST_API_KEY not set, skipping forecast")
        return None
    url = f"{BIRDCAST_API_BASE}?key={key}"
    async with aiohttp.ClientSession() as session:
        async with session.get(url) as resp:
            if resp.status != 200:
                logger.warning("BirdCast API returned %d", resp.status)
                return None
            data = await resp.json()
    try:
        return BirdcastForecast.model_validate(data)
    except Exception:
        logger.exception("failed to parse BirdCast response")
        return None


def format_forecast_message(forecast: BirdcastForecast) -> str:
    birds = random.sample(BIRD_EMOJIS, 4)
    header = f"{birds[0]}{birds[1]} **Migration Update** {birds[2]}{birds[3]}"
    lines = [header + "\n"]

    for i, night in enumerate(forecast.forecastNights):
        date_str = night.date.strftime("%b %-d")

        if i == 0:
            label = f"Tonight ({date_str})"
        elif i == 1:
            label = f"Tomorrow ({date_str})"
        else:
            label = f"{night.date.strftime('%A')} ({date_str})"

        tier, emoji = CODE_TO_TIER.get(night.code, ("Unknown", "\u26aa"))
        lines.append(f"{emoji} **{label}:** {tier}")

    return "\n".join(lines)


def birdcast_link_view() -> discord.ui.View:
    view = discord.ui.View()
    view.add_item(
        discord.ui.Button(
            style=discord.ButtonStyle.link,
            label="View on BirdCast",
            url=BIRDCAST_PAGE_URL,
        )
    )
    return view


if __name__ == "__main__":
    import asyncio

    async def _main():
        data = await fetch_birdcast_forecast()
        if data is None:
            print("Failed to fetch forecast")
            return
        print(format_forecast_message(data))

    asyncio.run(_main())
