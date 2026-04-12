import datetime
import logging
import os
import random
from zoneinfo import ZoneInfo

import aiohttp
import discord
from pydantic import BaseModel

from cloaca.piper.db.queries import AsyncQuerier
from cloaca.piper.db_pool import get_engine

logger = logging.getLogger(__name__)

EASTERN = ZoneInfo("America/New_York")
BIRDCAST_LOCATION = "nyc"

BIRDCAST_API_BASE = (
    "https://alert.birdcast.org/api/is-birdcast-alert-api"
    "/40.7127753,-74.0059728/birdcast"
)
BIRDCAST_DASHBOARD_API_BASE = (
    "https://dashboard.birdcast.org/api/v1/is-birdcast-alert-api"
)
BIRDCAST_PAGE_URL = (
    "https://alert.birdcast.org/"
    "?latLng=40.7127753,-74.0059728"
    "&locName=New%20York,%20NY,%20USA"
)
BIRDCAST_DASHBOARD_URL = "https://dashboard.birdcast.org/region/US-NY-047"
BIRDCAST_CHANNEL_ID = 1492204794931052666
BIRDCAST_REGION = "US-NY-047"

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


class NightSeriesEntry(BaseModel):
    localTime: str
    utc: str
    numAloft: int
    meanHeight: float | None = None
    maxHeight: float | None = None
    avgDirection: float | None = None
    avgSpeed: float | None = None
    vid: float = 0
    avgSpeedLevel: int | None = None
    isHigh: bool = False


class MigrationSeason(BaseModel):
    code: str
    startDate: str
    endDate: str


class MigrationTraffic(BaseModel):
    lastUpdated: str
    regionCode: str
    timezoneName: str
    cumulativeBirds: int
    isHigh: bool
    season: MigrationSeason
    nightSeries: list[NightSeriesEntry]


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


def is_todays_forecast(forecast: BirdcastForecast) -> bool:
    """True if the first forecast night's date is today (Eastern time)."""
    if not forecast.forecastNights:
        return False
    today = datetime.datetime.now(EASTERN).date()
    return forecast.forecastNights[0].date.date() == today


async def is_forecast_posted(date: datetime.date) -> bool:
    engine = get_engine()
    async with engine.connect() as conn:
        q = AsyncQuerier(conn)
        return (
            await q.is_birdcast_posted(location=BIRDCAST_LOCATION, forecast_date=date)
            is not None
        )


async def mark_forecast_posted(date: datetime.date) -> None:
    engine = get_engine()
    async with engine.begin() as conn:
        q = AsyncQuerier(conn)
        await q.insert_birdcast_post(location=BIRDCAST_LOCATION, forecast_date=date)


def format_forecast_message(forecast: BirdcastForecast) -> str:
    birds = random.sample(BIRD_EMOJIS, 4)
    header = f"{birds[0]}{birds[1]} **Forecast Update** {birds[2]}{birds[3]}"
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


# ---------------------------------------------------------------------------
# Migration traffic (dashboard live-migration data)
# ---------------------------------------------------------------------------

_COMPASS_DIRECTIONS = [
    "N",
    "NNE",
    "NE",
    "ENE",
    "E",
    "ESE",
    "SE",
    "SSE",
    "S",
    "SSW",
    "SW",
    "WSW",
    "W",
    "WNW",
    "NW",
    "NNW",
]


def _compass_direction(degrees: float) -> str:
    idx = round(degrees / 22.5) % 16
    return _COMPASS_DIRECTIONS[idx]


async def fetch_migration_traffic(
    date: datetime.date,
) -> MigrationTraffic | None:
    key = os.environ.get("BIRDCAST_API_KEY")
    if not key:
        logger.warning("BIRDCAST_API_KEY not set, skipping traffic fetch")
        return None
    url = (
        f"{BIRDCAST_DASHBOARD_API_BASE}/livemigration"
        f"/{BIRDCAST_REGION}/{date}?key={key}"
    )
    async with aiohttp.ClientSession() as session:
        async with session.get(url) as resp:
            if resp.status != 200:
                logger.warning("BirdCast dashboard API returned %d", resp.status)
                return None
            data = await resp.json()
    try:
        return MigrationTraffic.model_validate(data)
    except Exception:
        logger.exception("failed to parse BirdCast traffic response")
        return None


def format_migration_traffic_message(traffic: MigrationTraffic) -> str:
    birds = random.sample(BIRD_EMOJIS, 4)
    header = f"{birds[0]}{birds[1]} **Last Night's Migration** {birds[2]}{birds[3]}"

    lines = [header + "\n"]

    total = traffic.cumulativeBirds
    lines.append(f"\U0001f4ca **{total:,} birds** crossed Kings County")

    if not traffic.nightSeries:
        lines.append("\nNo migration data available for last night.")
        return "\n".join(lines)

    first = traffic.nightSeries[0]
    last = traffic.nightSeries[-1]
    start_dt = datetime.datetime.fromisoformat(first.localTime)
    end_dt = datetime.datetime.fromisoformat(last.localTime)
    night_str = start_dt.strftime("%a %b %-d")
    start_str = start_dt.strftime("%-I:%M %p")
    end_str = end_dt.strftime("%-I:%M %p")
    lines.append(f"\u23f0 {night_str}, {start_str} \u2013 {end_str}")

    peak = max(traffic.nightSeries, key=lambda e: e.numAloft)
    if peak.numAloft > 0:
        peak_time = datetime.datetime.fromisoformat(peak.localTime)
        peak_str = peak_time.strftime("%-I:%M %p")
        peak_line = f"\n\U0001f4c8 **Peak:** {peak.numAloft:,} birds at {peak_str}"
        if peak.avgDirection is not None:
            direction = _compass_direction(peak.avgDirection)
            peak_line += f" heading {direction}"
        lines.append(peak_line)
    else:
        lines.append("\nA quiet night \u2014 no significant migration detected.")

    return "\n".join(lines)


def migration_dashboard_link_view() -> discord.ui.View:
    view = discord.ui.View()
    view.add_item(
        discord.ui.Button(
            style=discord.ButtonStyle.link,
            label="View Dashboard",
            url=BIRDCAST_DASHBOARD_URL,
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
