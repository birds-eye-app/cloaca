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


@dataclass
class PendingProvisional:
    hotspot_id: str
    species_code: str
    common_name: str
    scientific_name: str
    obs_date: datetime.date
    observer_name: str
    checklist_id: str
    lifer_type: str  # 'year' or 'all_time'
    year: int | None  # set for year lifers
    created_at: datetime.datetime | None = None


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
    con.execute("""
        CREATE TABLE IF NOT EXISTS hotspot_all_time_species (
            hotspot_id VARCHAR NOT NULL,
            species_code VARCHAR NOT NULL,
            PRIMARY KEY (hotspot_id, species_code)
        );
    """)
    con.execute("""
        CREATE TABLE IF NOT EXISTS pending_provisional_lifers (
            hotspot_id VARCHAR NOT NULL,
            species_code VARCHAR NOT NULL,
            common_name VARCHAR NOT NULL,
            scientific_name VARCHAR NOT NULL,
            obs_date DATE NOT NULL,
            observer_name VARCHAR NOT NULL,
            checklist_id VARCHAR NOT NULL,
            lifer_type VARCHAR NOT NULL,
            year INTEGER,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (hotspot_id, species_code, lifer_type)
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


def get_all_time_total(hotspot_id: str) -> int:
    rows = (
        get_state_db()
        .execute(
            "SELECT COUNT(*) FROM hotspot_all_time_species WHERE hotspot_id = ?",
            [hotspot_id],
        )
        .fetchone()
    )
    return rows[0] if rows else 0


def _get_known_all_time_species(hotspot_id: str) -> set[str]:
    rows = (
        get_state_db()
        .execute(
            "SELECT species_code FROM hotspot_all_time_species WHERE hotspot_id = ?",
            [hotspot_id],
        )
        .fetchall()
    )
    return {row[0] for row in rows}


def _insert_all_time_species(hotspot_id: str, species_code: str):
    get_state_db().execute(
        """INSERT INTO hotspot_all_time_species (hotspot_id, species_code)
           VALUES (?, ?)
           ON CONFLICT DO NOTHING""",
        [hotspot_id, species_code],
    )


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
# Pending provisional helpers
# ---------------------------------------------------------------------------


def _get_pending_provisionals(hotspot_id: str) -> list[PendingProvisional]:
    rows = (
        get_state_db()
        .execute(
            """SELECT hotspot_id, species_code, common_name, scientific_name,
                      obs_date, observer_name, checklist_id, lifer_type, year,
                      created_at
               FROM pending_provisional_lifers
               WHERE hotspot_id = ?""",
            [hotspot_id],
        )
        .fetchall()
    )
    return [
        PendingProvisional(
            hotspot_id=r[0],
            species_code=r[1],
            common_name=r[2],
            scientific_name=r[3],
            obs_date=r[4] if isinstance(r[4], datetime.date) else r[4].date()
            if isinstance(r[4], datetime.datetime) else r[4],
            observer_name=r[5],
            checklist_id=r[6],
            lifer_type=r[7],
            year=r[8],
            created_at=r[9],
        )
        for r in rows
    ]


def _insert_pending_provisional(
    hotspot_id: str,
    obs: eBirdHistoricFullObservation,
    lifer_type: str,
    year: int | None = None,
):
    get_state_db().execute(
        """INSERT INTO pending_provisional_lifers
           (hotspot_id, species_code, common_name, scientific_name,
            obs_date, observer_name, checklist_id, lifer_type, year)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
           ON CONFLICT DO NOTHING""",
        [
            hotspot_id,
            obs.speciesCode,
            obs.comName,
            obs.sciName,
            obs.obsDt.date(),
            obs.userDisplayName,
            obs.checklistId,
            lifer_type,
            year,
        ],
    )


def _remove_pending_provisional(
    hotspot_id: str, species_code: str, lifer_type: str
):
    get_state_db().execute(
        """DELETE FROM pending_provisional_lifers
           WHERE hotspot_id = ? AND species_code = ? AND lifer_type = ?""",
        [hotspot_id, species_code, lifer_type],
    )


def _remove_year_species(hotspot_id: str, year: int, species_code: str):
    get_state_db().execute(
        """DELETE FROM hotspot_year_species
           WHERE hotspot_id = ? AND year = ? AND species_code = ?""",
        [hotspot_id, year, species_code],
    )


def _remove_all_time_species(hotspot_id: str, species_code: str):
    get_state_db().execute(
        """DELETE FROM hotspot_all_time_species
           WHERE hotspot_id = ? AND species_code = ?""",
        [hotspot_id, species_code],
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
        include_provisional=True,
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

    logger.info(
        "backfill complete for %s/%d: %d species (%d failed days)",
        hotspot_id,
        year,
        count,
        failed_days,
    )
    return count


ALL_TIME_BACKFILL_YEAR = 0


async def backfill_all_time_species(hotspot_id: str) -> int:
    if _is_backfill_complete(hotspot_id, ALL_TIME_BACKFILL_YEAR):
        logger.info("skipping all-time backfill for %s — already complete", hotspot_id)
        return 0

    logger.info("starting all-time species backfill for %s", hotspot_id)

    species_codes = await get_phoebe_client().product.species_list.list(
        region_code=hotspot_id
    )

    for code in species_codes:
        _insert_all_time_species(hotspot_id, code)

    count = len(species_codes)
    _mark_backfill_complete(hotspot_id, ALL_TIME_BACKFILL_YEAR, count)
    logger.info("all-time backfill complete for %s: %d species", hotspot_id, count)
    return count


# ---------------------------------------------------------------------------
# Polling
# ---------------------------------------------------------------------------


async def fetch_recent_observations(
    hotspot_id: str,
) -> list[eBirdHistoricFullObservation]:
    """Fetch observations for today and yesterday (shared by year + all-time checks)."""
    now = datetime.datetime.now(EASTERN)
    today = now.date()
    yesterday = today - datetime.timedelta(days=1)

    # Skip yesterday on Jan 1 to avoid counting last year's observations
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

    return observations


def _find_new_species(
    observations: list[eBirdHistoricFullObservation],
    known: set[str],
) -> list[eBirdHistoricFullObservation]:
    """From observations, find species not in known set. Returns earliest obs per species."""
    earliest_new: dict[str, eBirdHistoricFullObservation] = {}
    for obs in observations:
        if obs.speciesCode in known:
            continue
        if obs.speciesCode not in earliest_new:
            earliest_new[obs.speciesCode] = obs
        elif obs.obsDt < earliest_new[obs.speciesCode].obsDt:
            earliest_new[obs.speciesCode] = obs
    return list(earliest_new.values())


def _split_confirmed_provisional(
    new_lifers: list[eBirdHistoricFullObservation],
    all_observations: list[eBirdHistoricFullObservation],
) -> tuple[list[eBirdHistoricFullObservation], list[eBirdHistoricFullObservation]]:
    """Split new lifers into confirmed (reviewed) and provisional (unreviewed).

    A species is confirmed if ANY observation of it is reviewed (not just the
    earliest one returned by _find_new_species).
    """
    confirmed: list[eBirdHistoricFullObservation] = []
    provisional: list[eBirdHistoricFullObservation] = []
    for obs in new_lifers:
        if any(
            o.obsReviewed
            for o in all_observations
            if o.speciesCode == obs.speciesCode
        ):
            confirmed.append(obs)
        else:
            provisional.append(obs)
    return confirmed, provisional


def check_for_new_year_lifers(
    hotspot_id: str,
    observations: list[eBirdHistoricFullObservation],
) -> tuple[list[eBirdHistoricFullObservation], list[eBirdHistoricFullObservation]]:
    """Returns (confirmed, provisional) new year lifers."""
    now = datetime.datetime.now(EASTERN)

    if not observations:
        return [], []

    known = _get_known_species(hotspot_id, now.year)
    new_lifers = _find_new_species(observations, known)

    if not new_lifers:
        return [], []

    # Insert all into known set (prevents re-alerting on next poll)
    for obs in new_lifers:
        _insert_species(hotspot_id, now.year, obs)

    confirmed, provisional = _split_confirmed_provisional(
        new_lifers, observations
    )

    # Track provisional species for follow-up
    for obs in provisional:
        _insert_pending_provisional(hotspot_id, obs, "year", now.year)

    logger.info(
        "found %d new year lifer(s) at %s (%d confirmed, %d provisional): %s",
        len(new_lifers),
        hotspot_id,
        len(confirmed),
        len(provisional),
        ", ".join(o.comName for o in new_lifers),
    )
    return confirmed, provisional


def check_for_new_all_time_lifers(
    hotspot_id: str,
    observations: list[eBirdHistoricFullObservation],
) -> tuple[list[eBirdHistoricFullObservation], list[eBirdHistoricFullObservation]]:
    """Returns (confirmed, provisional) new all-time lifers."""
    if not observations:
        return [], []

    known = _get_known_all_time_species(hotspot_id)
    new_lifers = _find_new_species(observations, known)

    if not new_lifers:
        return [], []

    # Insert all into known set (prevents re-alerting on next poll)
    for obs in new_lifers:
        _insert_all_time_species(hotspot_id, obs.speciesCode)

    confirmed, provisional = _split_confirmed_provisional(
        new_lifers, observations
    )

    # Track provisional species for follow-up
    for obs in provisional:
        _insert_pending_provisional(hotspot_id, obs, "all_time")

    logger.info(
        "found %d new all-time lifer(s) at %s (%d confirmed, %d provisional): %s",
        len(new_lifers),
        hotspot_id,
        len(confirmed),
        len(provisional),
        ", ".join(o.comName for o in new_lifers),
    )
    return confirmed, provisional


# ---------------------------------------------------------------------------
# Pending provisional review checks
# ---------------------------------------------------------------------------

# If a provisional observation disappears from the API for this many days,
# we assume it was invalidated (the observer deleted the checklist or the
# reviewer rejected the record).
_PROVISIONAL_STALE_DAYS = 14


async def check_pending_provisionals(
    hotspot_id: str,
    recent_observations: list[eBirdHistoricFullObservation],
) -> tuple[list[PendingProvisional], list[PendingProvisional]]:
    """Re-check pending provisional observations for review-status changes.

    *recent_observations* should already contain today + yesterday (the normal
    poll data).  For any pending provisional whose obs_date falls outside that
    window we issue an extra API call so we can see its current review status.

    Returns ``(confirmed, invalidated)`` lists.

    Confirmed = a reviewed observation of the species now exists (eBird only
    returns ``obsValid=True`` records, so presence + ``obsReviewed`` is enough).

    Invalidated = the observation has vanished from the API for longer than
    ``_PROVISIONAL_STALE_DAYS``, meaning the reviewer rejected it or the
    checklist was deleted.
    """
    pending = _get_pending_provisionals(hotspot_id)
    if not pending:
        return [], []

    now = datetime.datetime.now(EASTERN)
    today = now.date()
    yesterday = today - datetime.timedelta(days=1)
    covered_dates = {today, yesterday}

    # Collect extra dates we need to fetch
    extra_dates = {p.obs_date for p in pending} - covered_dates
    extra_observations: list[eBirdHistoricFullObservation] = []
    for date in extra_dates:
        try:
            extra_observations.extend(
                await fetch_observations_for_date(hotspot_id, date)
            )
        except Exception:
            logger.exception(
                "failed to fetch observations for pending check %s on %s",
                hotspot_id,
                date,
            )

    all_observations = list(recent_observations) + extra_observations

    confirmed: list[PendingProvisional] = []
    invalidated: list[PendingProvisional] = []

    for p in pending:
        matching = [
            o for o in all_observations if o.speciesCode == p.species_code
        ]
        if not matching:
            # Observation gone — invalidated or transient API issue.
            age = (today - p.obs_date).days
            if age >= _PROVISIONAL_STALE_DAYS:
                invalidated.append(p)
            continue

        if any(o.obsReviewed for o in matching):
            # Reviewed observations in the API are always valid (invalid ones
            # are removed entirely), so this means confirmed.
            confirmed.append(p)

    # Apply DB changes
    for p in confirmed:
        _remove_pending_provisional(p.hotspot_id, p.species_code, p.lifer_type)
        logger.info(
            "pending provisional confirmed: %s at %s (%s)",
            p.common_name,
            p.hotspot_id,
            p.lifer_type,
        )

    for p in invalidated:
        _remove_pending_provisional(p.hotspot_id, p.species_code, p.lifer_type)
        # Remove from known species so a future valid observation can trigger
        # a fresh alert.
        if p.lifer_type == "year" and p.year is not None:
            _remove_year_species(p.hotspot_id, p.year, p.species_code)
        elif p.lifer_type == "all_time":
            _remove_all_time_species(p.hotspot_id, p.species_code)
        logger.info(
            "pending provisional invalidated: %s at %s (%s)",
            p.common_name,
            p.hotspot_id,
            p.lifer_type,
        )

    return confirmed, invalidated


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
            f"**{obs.comName}** — first spotted by "
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
            f"**{obs.comName}** — {obs.userDisplayName} "
            f"({date_str}) · [checklist]({checklist_url})"
        )
    return "\n".join(lines)


def format_all_time_lifer_message(
    new_lifers: list[eBirdHistoricFullObservation],
    hotspot_name: str,
    all_time_total: int,
) -> str:
    if len(new_lifers) == 1:
        obs = new_lifers[0]
        date_str = obs.obsDt.strftime("%b %-d")
        header = (
            f"🎉🥳 **New Park Bird for {hotspot_name}!** (#{all_time_total} all-time)"
        )
        checklist_url = f"https://ebird.org/checklist/{obs.checklistId}"
        body = (
            f"**{obs.comName}** — first spotted by "
            f"{obs.userDisplayName} ({date_str})\n"
            f"[View checklist]({checklist_url})"
        )
        return f"{header}\n\n{body}"

    header = (
        f"🎉🥳 **{len(new_lifers)} New Park Birds "
        f"for {hotspot_name}!** (now at {all_time_total} species all-time)"
    )
    lines = [header, ""]
    for obs in new_lifers:
        date_str = obs.obsDt.strftime("%b %-d")
        checklist_url = f"https://ebird.org/checklist/{obs.checklistId}"
        lines.append(
            f"**{obs.comName}** — {obs.userDisplayName} "
            f"({date_str}) · [checklist]({checklist_url})"
        )
    return "\n".join(lines)


# --- Tentative (unreviewed) alerts ---


def format_tentative_year_lifer_message(
    new_lifers: list[eBirdHistoricFullObservation],
    hotspot_name: str,
) -> str:
    if len(new_lifers) == 1:
        obs = new_lifers[0]
        date_str = obs.obsDt.strftime("%b %-d")
        checklist_url = f"https://ebird.org/checklist/{obs.checklistId}"
        return (
            f"👀 **Possible Year Bird for {hotspot_name}!**\n\n"
            f"**{obs.comName}** — reported by "
            f"{obs.userDisplayName} ({date_str})\n"
            f"[View checklist]({checklist_url})\n"
            f"-# Awaiting eBird review — we'll celebrate once confirmed!"
        )

    lines = [
        f"👀 **{len(new_lifers)} Possible New Year Birds "
        f"for {hotspot_name}!**",
        "",
    ]
    for obs in new_lifers:
        date_str = obs.obsDt.strftime("%b %-d")
        checklist_url = f"https://ebird.org/checklist/{obs.checklistId}"
        lines.append(
            f"**{obs.comName}** — {obs.userDisplayName} "
            f"({date_str}) · [checklist]({checklist_url})"
        )
    lines.append(
        f"-# Awaiting eBird review — we'll celebrate once confirmed!"
    )
    return "\n".join(lines)


def format_tentative_all_time_lifer_message(
    new_lifers: list[eBirdHistoricFullObservation],
    hotspot_name: str,
) -> str:
    if len(new_lifers) == 1:
        obs = new_lifers[0]
        date_str = obs.obsDt.strftime("%b %-d")
        checklist_url = f"https://ebird.org/checklist/{obs.checklistId}"
        return (
            f"👀 **Possible New Park Bird for {hotspot_name}!**\n\n"
            f"**{obs.comName}** — reported by "
            f"{obs.userDisplayName} ({date_str})\n"
            f"[View checklist]({checklist_url})\n"
            f"-# Awaiting eBird review — we'll celebrate once confirmed!"
        )

    lines = [
        f"👀 **{len(new_lifers)} Possible New Park Birds "
        f"for {hotspot_name}!**",
        "",
    ]
    for obs in new_lifers:
        date_str = obs.obsDt.strftime("%b %-d")
        checklist_url = f"https://ebird.org/checklist/{obs.checklistId}"
        lines.append(
            f"**{obs.comName}** — {obs.userDisplayName} "
            f"({date_str}) · [checklist]({checklist_url})"
        )
    lines.append(
        f"-# Awaiting eBird review — we'll celebrate once confirmed!"
    )
    return "\n".join(lines)


# --- Confirmed (after review) alerts ---


def format_confirmed_year_lifer_message(
    confirmed: list[PendingProvisional],
    hotspot_name: str,
    year_total: int,
) -> str:
    birds = random.sample(BIRD_EMOJIS, min(2, len(BIRD_EMOJIS)))

    if len(confirmed) == 1:
        p = confirmed[0]
        checklist_url = f"https://ebird.org/checklist/{p.checklist_id}"
        return (
            f"{birds[0]} **Confirmed! Year Bird #{year_total} "
            f"for {hotspot_name}!**\n\n"
            f"**{p.common_name}** has been reviewed and confirmed on eBird!\n"
            f"[View checklist]({checklist_url})"
        )

    header = (
        f"{birds[0]}{birds[1]} **{len(confirmed)} Year Birds Confirmed "
        f"for {hotspot_name}!** (now at {year_total} species)"
    )
    lines = [header, ""]
    for p in confirmed:
        checklist_url = f"https://ebird.org/checklist/{p.checklist_id}"
        lines.append(
            f"**{p.common_name}** — confirmed! · "
            f"[checklist]({checklist_url})"
        )
    return "\n".join(lines)


def format_confirmed_all_time_lifer_message(
    confirmed: list[PendingProvisional],
    hotspot_name: str,
    all_time_total: int,
) -> str:
    if len(confirmed) == 1:
        p = confirmed[0]
        checklist_url = f"https://ebird.org/checklist/{p.checklist_id}"
        return (
            f"🎉🥳 **Confirmed! New Park Bird for {hotspot_name}!** "
            f"(#{all_time_total} all-time)\n\n"
            f"**{p.common_name}** has been reviewed and confirmed on eBird!\n"
            f"[View checklist]({checklist_url})"
        )

    header = (
        f"🎉🥳 **{len(confirmed)} New Park Birds Confirmed "
        f"for {hotspot_name}!** (now at {all_time_total} species all-time)"
    )
    lines = [header, ""]
    for p in confirmed:
        checklist_url = f"https://ebird.org/checklist/{p.checklist_id}"
        lines.append(
            f"**{p.common_name}** — confirmed! · "
            f"[checklist]({checklist_url})"
        )
    return "\n".join(lines)


def format_invalidated_lifer_message(
    invalidated: list[PendingProvisional],
    hotspot_name: str,
) -> str:
    names = ", ".join(f"**{p.common_name}**" for p in invalidated)
    if len(invalidated) == 1:
        return (
            f"**Update:** {names} at {hotspot_name} was not confirmed "
            f"after eBird review."
        )
    return (
        f"**Update:** {names} at {hotspot_name} were not confirmed "
        f"after eBird review."
    )


def all_time_list_link_view(hotspot_id: str) -> discord.ui.View:
    view = discord.ui.View()
    view.add_item(
        discord.ui.Button(
            style=discord.ButtonStyle.link,
            label="View All-Time List on eBird",
            url=f"https://ebird.org/hotspot/{hotspot_id}/bird-list",
        )
    )
    return view


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

            year_count = await backfill_year_species(hotspot.id)
            print(f"Backfilled {year_count} year species")

            all_time_count = await backfill_all_time_species(hotspot.id)
            print(f"Backfilled {all_time_count} all-time species")

            print(f"Year total: {get_year_total(hotspot.id)}")
            print(f"All-time total: {get_all_time_total(hotspot.id)}")

            observations = await fetch_recent_observations(hotspot.id)

            confirmed_at, provisional_at = check_for_new_all_time_lifers(
                hotspot.id, observations
            )
            if confirmed_at:
                total = get_all_time_total(hotspot.id)
                print(format_all_time_lifer_message(confirmed_at, hotspot.name, total))
            if provisional_at:
                print(format_tentative_all_time_lifer_message(provisional_at, hotspot.name))
            if not confirmed_at and not provisional_at:
                print("No new all-time lifers found")

            # Filter out all-time lifers from year lifer notifications
            all_time_codes = {
                o.speciesCode for o in confirmed_at + provisional_at
            }
            confirmed_yr, provisional_yr = check_for_new_year_lifers(
                hotspot.id, observations
            )
            confirmed_yr = [o for o in confirmed_yr if o.speciesCode not in all_time_codes]
            provisional_yr = [o for o in provisional_yr if o.speciesCode not in all_time_codes]
            if confirmed_yr:
                total = get_year_total(hotspot.id)
                print(format_year_lifer_message(confirmed_yr, hotspot.name, total))
            if provisional_yr:
                print(format_tentative_year_lifer_message(provisional_yr, hotspot.name))
            if not confirmed_yr and not provisional_yr:
                print("No new year lifers found")

            # Check pending provisionals
            confirmed_pending, invalidated = await check_pending_provisionals(
                hotspot.id, observations
            )
            if confirmed_pending:
                year_confirmed = [p for p in confirmed_pending if p.lifer_type == "year"]
                at_confirmed = [p for p in confirmed_pending if p.lifer_type == "all_time"]
                if at_confirmed:
                    total = get_all_time_total(hotspot.id)
                    print(format_confirmed_all_time_lifer_message(at_confirmed, hotspot.name, total))
                if year_confirmed:
                    total = get_year_total(hotspot.id)
                    print(format_confirmed_year_lifer_message(year_confirmed, hotspot.name, total))
            if invalidated:
                print(format_invalidated_lifer_message(invalidated, hotspot.name))

        close_state_db()

    asyncio.run(_main())
