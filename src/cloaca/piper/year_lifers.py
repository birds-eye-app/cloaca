import asyncio
import datetime
import logging
import os
import random
from dataclasses import dataclass
from zoneinfo import ZoneInfo

import duckdb
import discord

from cloaca.api.shared import get_phoebe_client
from cloaca.piper.birdcast import BIRD_EMOJIS
from cloaca.scripts.fetch_yearly_hotspot_data import (
    eBirdHistoricFullObservation,
    parse_historic_observations_json_text,
)

logger = logging.getLogger(__name__)


@dataclass
class Hotspot:
    id: str
    name: str
    channel_id: int


WATCHED_HOTSPOTS = [
    Hotspot(id="L2987624", name="McGolrick Park", channel_id=1492224353537102025),
    Hotspot(id="L1814508", name="Franz Sigel Park", channel_id=1492237397700776128),
]

EASTERN = ZoneInfo("America/New_York")

_state_db: duckdb.DuckDBPyConnection | None = None


def get_state_db() -> duckdb.DuckDBPyConnection:
    global _state_db
    if _state_db is None:
        path = os.environ.get("PIPER_STATE_DB_PATH", "piper_state.db")
        _state_db = duckdb.connect(path, read_only=False)
        _ensure_tables(_state_db)
    return _state_db


def close_state_db():
    global _state_db
    if _state_db is not None:
        _state_db.close()
        _state_db = None


def _ensure_tables(con: duckdb.DuckDBPyConnection):
    con.execute("""
        CREATE TABLE IF NOT EXISTS hotspot_year_species (
            hotspot_id VARCHAR NOT NULL,
            year INTEGER NOT NULL,
            species_code VARCHAR NOT NULL,
            common_name VARCHAR NOT NULL,
            scientific_name VARCHAR NOT NULL,
            first_obs_date DATE NOT NULL,
            observer_name VARCHAR NOT NULL,
            checklist_id VARCHAR NOT NULL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (hotspot_id, year, species_code)
        );
    """)
    con.execute("""
        CREATE TABLE IF NOT EXISTS backfill_status (
            hotspot_id VARCHAR NOT NULL,
            year INTEGER NOT NULL,
            completed_at TIMESTAMP NOT NULL,
            species_count INTEGER NOT NULL,
            PRIMARY KEY (hotspot_id, year)
        );
    """)


def get_year_total(hotspot_id: str) -> int:
    now = datetime.datetime.now(EASTERN)
    rows = (
        get_state_db()
        .execute(
            "SELECT COUNT(*) FROM hotspot_year_species WHERE hotspot_id = ? AND year = ?",
            [hotspot_id, now.year],
        )
        .fetchone()
    )
    return rows[0] if rows else 0


def _get_known_species(hotspot_id: str, year: int) -> set[str]:
    rows = (
        get_state_db()
        .execute(
            "SELECT species_code FROM hotspot_year_species WHERE hotspot_id = ? AND year = ?",
            [hotspot_id, year],
        )
        .fetchall()
    )
    return {row[0] for row in rows}


def _insert_species(
    hotspot_id: str,
    year: int,
    obs: eBirdHistoricFullObservation,
):
    get_state_db().execute(
        """INSERT INTO hotspot_year_species
           (hotspot_id, year, species_code, common_name, scientific_name,
            first_obs_date, observer_name, checklist_id)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?)
           ON CONFLICT DO NOTHING""",
        [
            hotspot_id,
            year,
            obs.speciesCode,
            obs.comName,
            obs.sciName,
            obs.obsDt.date(),
            obs.userDisplayName,
            obs.checklistId,
        ],
    )


# ---------------------------------------------------------------------------
# eBird API
# ---------------------------------------------------------------------------


async def fetch_observations_for_date(
    hotspot_id: str, date: datetime.date
) -> list[eBirdHistoricFullObservation]:
    raw_response = await get_phoebe_client().data.with_raw_response.observations.recent.historic.list(
        region_code="US-NY",
        rank="create",
        y=date.year,
        m=date.month,
        d=date.day,
        cat="species",
        r=[hotspot_id],
        detail="full",
    )
    text = await raw_response.text()
    if not text or text.strip() == "[]":
        return []
    observations = parse_historic_observations_json_text(text)
    # Filter out exotics
    return [o for o in observations if o.exoticCategory != "X"]


# ---------------------------------------------------------------------------
# Backfill
# ---------------------------------------------------------------------------


def _is_backfill_complete(hotspot_id: str, year: int) -> bool:
    row = (
        get_state_db()
        .execute(
            "SELECT 1 FROM backfill_status WHERE hotspot_id = ? AND year = ?",
            [hotspot_id, year],
        )
        .fetchone()
    )
    return row is not None


def _mark_backfill_complete(hotspot_id: str, year: int, species_count: int):
    get_state_db().execute(
        """INSERT INTO backfill_status (hotspot_id, year, completed_at, species_count)
           VALUES (?, ?, CURRENT_TIMESTAMP, ?)
           ON CONFLICT DO NOTHING""",
        [hotspot_id, year, species_count],
    )


async def backfill_year_species(hotspot_id: str) -> int:
    now = datetime.datetime.now(EASTERN)
    year = now.year

    if _is_backfill_complete(hotspot_id, year):
        logger.info(
            "skipping backfill for %s/%d — already complete",
            hotspot_id,
            year,
        )
        return 0

    logger.info("starting year species backfill for %s/%d", hotspot_id, year)

    # earliest_per_species tracks the first observation of each species
    earliest_per_species: dict[str, eBirdHistoricFullObservation] = {}

    start_date = datetime.date(year, 1, 1)
    yesterday = now.date() - datetime.timedelta(days=1)
    current = start_date
    failed_days = 0

    while current <= yesterday:
        try:
            observations = await fetch_observations_for_date(hotspot_id, current)
            for obs in observations:
                if obs.speciesCode not in earliest_per_species:
                    earliest_per_species[obs.speciesCode] = obs
                elif obs.obsDt < earliest_per_species[obs.speciesCode].obsDt:
                    earliest_per_species[obs.speciesCode] = obs
        except Exception:
            logger.exception("backfill failed for %s on %s", hotspot_id, current)
            failed_days += 1

        current += datetime.timedelta(days=1)
        await asyncio.sleep(0.5)

    # Bulk insert
    for obs in earliest_per_species.values():
        _insert_species(hotspot_id, year, obs)

    count = len(earliest_per_species)
    _mark_backfill_complete(hotspot_id, year, count)
    # Flush WAL so external readers (e.g. SSH) can see the data
    get_state_db().execute("FORCE CHECKPOINT")
    logger.info(
        "backfill complete for %s/%d: %d species (%d failed days)",
        hotspot_id,
        year,
        count,
        failed_days,
    )
    return count


# ---------------------------------------------------------------------------
# Polling
# ---------------------------------------------------------------------------


async def check_for_new_year_lifers(
    hotspot_id: str,
) -> list[eBirdHistoricFullObservation]:
    now = datetime.datetime.now(EASTERN)
    today = now.date()
    yesterday = today - datetime.timedelta(days=1)

    # Fetch observations for today and yesterday (skip yesterday on Jan 1
    # to avoid counting last year's observations as new-year lifers)
    dates_to_check = [today]
    if yesterday.year == today.year:
        dates_to_check.append(yesterday)

    observations: list[eBirdHistoricFullObservation] = []
    for date in dates_to_check:
        try:
            observations.extend(await fetch_observations_for_date(hotspot_id, date))
        except Exception:
            logger.exception(
                "failed to fetch observations for %s on %s", hotspot_id, date
            )

    if not observations:
        return []

    known = _get_known_species(hotspot_id, now.year)

    # Group by species, keep earliest observation per species
    earliest_new: dict[str, eBirdHistoricFullObservation] = {}
    for obs in observations:
        if obs.speciesCode in known:
            continue
        if obs.speciesCode not in earliest_new:
            earliest_new[obs.speciesCode] = obs
        elif obs.obsDt < earliest_new[obs.speciesCode].obsDt:
            earliest_new[obs.speciesCode] = obs

    if not earliest_new:
        return []

    # Insert new lifers into DB
    new_lifers = list(earliest_new.values())
    for obs in new_lifers:
        _insert_species(hotspot_id, now.year, obs)
    # Flush WAL so external readers (e.g. SSH) can see the data
    get_state_db().execute("FORCE CHECKPOINT")

    logger.info(
        "found %d new year lifer(s) at %s: %s",
        len(new_lifers),
        hotspot_id,
        ", ".join(o.comName for o in new_lifers),
    )
    return new_lifers


# ---------------------------------------------------------------------------
# Discord formatting
# ---------------------------------------------------------------------------


def format_year_lifer_message(
    new_lifers: list[eBirdHistoricFullObservation],
    hotspot_name: str,
    year_total: int,
) -> str:
    birds = random.sample(BIRD_EMOJIS, min(2, len(BIRD_EMOJIS)))

    if len(new_lifers) == 1:
        obs = new_lifers[0]
        date_str = obs.obsDt.strftime("%b %-d")
        header = f"{birds[0]} **Year Bird #{year_total} for {hotspot_name}!**"
        checklist_url = f"https://ebird.org/checklist/{obs.checklistId}"
        body = (
            f"\U0001f426 **{obs.comName}** — first spotted by "
            f"{obs.userDisplayName} ({date_str})\n"
            f"[View checklist]({checklist_url})"
        )
        return f"{header}\n\n{body}"

    # Multiple lifers
    header = (
        f"{birds[0]}{birds[1]} **{len(new_lifers)} New Year Birds "
        f"for {hotspot_name}!** (now at {year_total} species)"
    )
    lines = [header, ""]
    for obs in new_lifers:
        date_str = obs.obsDt.strftime("%b %-d")
        checklist_url = f"https://ebird.org/checklist/{obs.checklistId}"
        lines.append(
            f"\U0001f426 **{obs.comName}** — {obs.userDisplayName} "
            f"({date_str}) · [checklist]({checklist_url})"
        )
    return "\n".join(lines)


def year_list_link_view(hotspot_id: str) -> discord.ui.View:
    view = discord.ui.View()
    view.add_item(
        discord.ui.Button(
            style=discord.ButtonStyle.link,
            label="View Year List on eBird",
            url=f"https://ebird.org/hotspot/{hotspot_id}/bird-list?yr=cur",
        )
    )
    return view


# ---------------------------------------------------------------------------
# CLI testing
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    from dotenv import load_dotenv

    load_dotenv(override=True)
    logging.basicConfig(level=logging.INFO)

    async def _main():
        for hotspot in WATCHED_HOTSPOTS:
            print(f"\n--- {hotspot.name} ({hotspot.id}) ---")
            count = await backfill_year_species(hotspot.id)
            print(f"Backfilled {count} species")

            total = get_year_total(hotspot.id)
            print(f"Year total: {total}")

            new = await check_for_new_year_lifers(hotspot.id)
            if new:
                total = get_year_total(hotspot.id)
                msg = format_year_lifer_message(new, hotspot.name, total)
                print(msg)
            else:
                print("No new year lifers found")

        close_state_db()

    asyncio.run(_main())
