import asyncio
import datetime
import logging
import random
from dataclasses import dataclass
from zoneinfo import ZoneInfo

import discord
from phoebe_bird.types.data.observation import Observation

from cloaca.api.shared import get_phoebe_client
from cloaca.piper.birdcast import BIRD_EMOJIS
from cloaca.piper.db.queries import (
    AsyncQuerier,
    InsertPendingProvisionalParams,
    InsertSpeciesParams,
)
from cloaca.piper.db_pool import close_engine, get_engine
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
    sub_id: str
    lifer_type: str  # 'year' or 'all_time'
    year: int | None  # set for year lifers
    created_at: datetime.datetime | None = None


WATCHED_HOTSPOTS = [
    Hotspot(id="L2987624", name="McGolrick Park", channel_id=1492224353537102025),
    Hotspot(id="L1814508", name="Franz Sigel Park", channel_id=1492237397700776128),
]

EASTERN = ZoneInfo("America/New_York")


async def get_year_total(hotspot_id: str) -> int:
    now = datetime.datetime.now(EASTERN)
    async with get_engine().connect() as conn:
        result = await AsyncQuerier(conn).get_year_total(
            hotspot_id=hotspot_id, year=now.year
        )
        return result or 0


async def _get_known_species(hotspot_id: str, year: int) -> set[str]:
    async with get_engine().connect() as conn:
        return {
            code
            async for code in AsyncQuerier(conn).get_known_species(
                hotspot_id=hotspot_id, year=year
            )
        }


async def get_all_time_total(hotspot_id: str) -> int:
    async with get_engine().connect() as conn:
        result = await AsyncQuerier(conn).get_all_time_total(hotspot_id=hotspot_id)
        return result or 0


async def _get_known_all_time_species(hotspot_id: str) -> set[str]:
    async with get_engine().connect() as conn:
        return {
            code
            async for code in AsyncQuerier(conn).get_known_all_time_species(
                hotspot_id=hotspot_id
            )
        }


async def _insert_all_time_species(hotspot_id: str, species_code: str):
    async with get_engine().begin() as conn:
        await AsyncQuerier(conn).insert_all_time_species(
            hotspot_id=hotspot_id, species_code=species_code
        )


async def _insert_species(
    hotspot_id: str,
    year: int,
    obs: eBirdHistoricFullObservation,
):
    async with get_engine().begin() as conn:
        await AsyncQuerier(conn).insert_species(
            InsertSpeciesParams(
                hotspot_id=hotspot_id,
                year=year,
                species_code=obs.speciesCode,
                common_name=obs.comName,
                scientific_name=obs.sciName,
                first_obs_date=obs.obsDt.date(),
                observer_name=obs.userDisplayName,
                checklist_id=obs.checklistId,
            )
        )


# ---------------------------------------------------------------------------
# Pending provisional helpers
# ---------------------------------------------------------------------------


async def _get_pending_provisionals(hotspot_id: str) -> list[PendingProvisional]:
    async with get_engine().connect() as conn:
        return [
            PendingProvisional(
                hotspot_id=r.hotspot_id,
                species_code=r.species_code,
                common_name=r.common_name,
                scientific_name=r.scientific_name,
                obs_date=r.obs_date,
                observer_name=r.observer_name,
                sub_id=r.sub_id,
                lifer_type=r.lifer_type,
                year=r.year,
                created_at=r.created_at,
            )
            async for r in AsyncQuerier(conn).get_pending_provisionals(
                hotspot_id=hotspot_id
            )
        ]


async def _insert_pending_provisional(
    hotspot_id: str,
    obs: eBirdHistoricFullObservation,
    lifer_type: str,
    year: int | None = None,
):
    async with get_engine().begin() as conn:
        await AsyncQuerier(conn).insert_pending_provisional(
            InsertPendingProvisionalParams(
                hotspot_id=hotspot_id,
                species_code=obs.speciesCode,
                common_name=obs.comName,
                scientific_name=obs.sciName,
                obs_date=obs.obsDt.date(),
                observer_name=obs.userDisplayName,
                sub_id=obs.subId,
                lifer_type=lifer_type,
                year=year,
            )
        )


async def _remove_pending_provisional(
    hotspot_id: str, species_code: str, lifer_type: str
):
    async with get_engine().begin() as conn:
        await AsyncQuerier(conn).remove_pending_provisional(
            hotspot_id=hotspot_id, species_code=species_code, lifer_type=lifer_type
        )


async def _remove_year_species(hotspot_id: str, year: int, species_code: str):
    async with get_engine().begin() as conn:
        await AsyncQuerier(conn).remove_year_species(
            hotspot_id=hotspot_id, year=year, species_code=species_code
        )


async def _remove_all_time_species(hotspot_id: str, species_code: str):
    async with get_engine().begin() as conn:
        await AsyncQuerier(conn).remove_all_time_species(
            hotspot_id=hotspot_id, species_code=species_code
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


async def fetch_notable_observations(
    hotspot_id: str,
) -> list[Observation]:
    """Fetch recent notable (rare/unusual) observations for a hotspot.

    Unlike the historic endpoint which returns one observation per species,
    the notable endpoint returns every individual report with full review
    status (obs_valid, obs_reviewed). This lets us distinguish between:
    - Confirmed rarities (obs_valid=True)
    - Unconfirmed rarities awaiting review (obs_valid=False)

    Common species never appear in notable results.
    """
    return await get_phoebe_client().data.observations.recent.notable.list(
        region_code=hotspot_id,
        back=10,
        detail="full",
    )


# ---------------------------------------------------------------------------
# Backfill
# ---------------------------------------------------------------------------


async def _is_backfill_complete(hotspot_id: str, year: int) -> bool:
    async with get_engine().connect() as conn:
        result = await AsyncQuerier(conn).is_backfill_complete(
            hotspot_id=hotspot_id, year=year
        )
        return result is not None


async def _mark_backfill_complete(hotspot_id: str, year: int, species_count: int):
    async with get_engine().begin() as conn:
        await AsyncQuerier(conn).mark_backfill_complete(
            hotspot_id=hotspot_id, year=year, species_count=species_count
        )


async def backfill_year_species(hotspot_id: str) -> int:
    now = datetime.datetime.now(EASTERN)
    year = now.year

    if await _is_backfill_complete(hotspot_id, year):
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
        await _insert_species(hotspot_id, year, obs)

    count = len(earliest_per_species)
    await _mark_backfill_complete(hotspot_id, year, count)

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
    if await _is_backfill_complete(hotspot_id, ALL_TIME_BACKFILL_YEAR):
        logger.info("skipping all-time backfill for %s — already complete", hotspot_id)
        return 0

    logger.info("starting all-time species backfill for %s", hotspot_id)

    species_codes = await get_phoebe_client().product.species_list.list(
        region_code=hotspot_id
    )

    for code in species_codes:
        await _insert_all_time_species(hotspot_id, code)

    count = len(species_codes)
    await _mark_backfill_complete(hotspot_id, ALL_TIME_BACKFILL_YEAR, count)
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
    notable_observations: list[Observation],
) -> tuple[list[eBirdHistoricFullObservation], list[eBirdHistoricFullObservation]]:
    """Split new lifers into confirmed and provisional (awaiting review).

    Uses the notable/rarities endpoint data which returns every individual
    report for rare species with full review status.

    A species is provisional only if it appears in notable with NO confirmed
    (obs_valid=True) reports — meaning it's a rarity that's still awaiting
    eBird reviewer approval.

    Common species never appear in notable and are automatically confirmed.
    """
    # Build sets of species codes from notable observations
    confirmed_notable: set[str] = set()
    unconfirmed_notable: set[str] = set()
    for o in notable_observations:
        if o.obs_valid:
            confirmed_notable.add(o.species_code)
        else:
            unconfirmed_notable.add(o.species_code)

    confirmed: list[eBirdHistoricFullObservation] = []
    provisional: list[eBirdHistoricFullObservation] = []
    for obs in new_lifers:
        if obs.speciesCode in confirmed_notable:
            # Rarity with at least one confirmed report
            confirmed.append(obs)
        elif obs.speciesCode in unconfirmed_notable:
            # Rarity with only unconfirmed reports
            provisional.append(obs)
        else:
            # Common species (not in notable at all) — confirmed
            confirmed.append(obs)
    return confirmed, provisional


async def check_for_new_year_lifers(
    hotspot_id: str,
    observations: list[eBirdHistoricFullObservation],
    notable_observations: list[Observation],
) -> tuple[list[eBirdHistoricFullObservation], list[eBirdHistoricFullObservation]]:
    """Returns (confirmed, provisional) new year lifers."""
    now = datetime.datetime.now(EASTERN)

    if not observations:
        return [], []

    known = await _get_known_species(hotspot_id, now.year)
    new_lifers = _find_new_species(observations, known)

    if not new_lifers:
        return [], []

    # Insert all into known set (prevents re-alerting on next poll)
    for obs in new_lifers:
        await _insert_species(hotspot_id, now.year, obs)

    confirmed, provisional = _split_confirmed_provisional(
        new_lifers, notable_observations
    )

    # Track provisional species for follow-up
    for obs in provisional:
        await _insert_pending_provisional(hotspot_id, obs, "year", now.year)

    logger.info(
        "found %d new year lifer(s) at %s (%d confirmed, %d provisional): %s",
        len(new_lifers),
        hotspot_id,
        len(confirmed),
        len(provisional),
        ", ".join(o.comName for o in new_lifers),
    )
    return confirmed, provisional


async def check_for_new_all_time_lifers(
    hotspot_id: str,
    observations: list[eBirdHistoricFullObservation],
    notable_observations: list[Observation],
) -> tuple[list[eBirdHistoricFullObservation], list[eBirdHistoricFullObservation]]:
    """Returns (confirmed, provisional) new all-time lifers."""
    if not observations:
        return [], []

    known = await _get_known_all_time_species(hotspot_id)
    new_lifers = _find_new_species(observations, known)

    if not new_lifers:
        return [], []

    # Insert all into known set (prevents re-alerting on next poll)
    for obs in new_lifers:
        await _insert_all_time_species(hotspot_id, obs.speciesCode)

    confirmed, provisional = _split_confirmed_provisional(
        new_lifers, notable_observations
    )

    # Track provisional species for follow-up
    for obs in provisional:
        await _insert_pending_provisional(hotspot_id, obs, "all_time")

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
    notable_observations: list[Observation],
) -> tuple[list[PendingProvisional], list[PendingProvisional]]:
    """Re-check pending provisional observations for review-status changes.

    Uses the notable endpoint which returns every individual report for rare
    species with full review status. This avoids the historic endpoint's
    one-per-species deduplication.

    Returns ``(confirmed, invalidated)`` lists.

    Confirmed = at least one observation of the species now has obs_valid=True
    (a reviewer approved it).

    Invalidated = the species has disappeared from notable for longer than
    ``_PROVISIONAL_STALE_DAYS``, meaning the reviewer rejected it or the
    checklist was deleted.
    """
    pending = await _get_pending_provisionals(hotspot_id)
    if not pending:
        return [], []

    now = datetime.datetime.now(EASTERN)
    today = now.date()

    confirmed: list[PendingProvisional] = []
    invalidated: list[PendingProvisional] = []

    for p in pending:
        matching = [o for o in notable_observations if o.species_code == p.species_code]
        if not matching:
            # Species gone from notable — invalidated or transient API issue.
            age = (today - p.obs_date).days
            if age >= _PROVISIONAL_STALE_DAYS:
                invalidated.append(p)
            continue

        if any(o.obs_valid for o in matching):
            # At least one report has been confirmed by a reviewer.
            confirmed.append(p)

    # Apply DB changes
    for p in confirmed:
        await _remove_pending_provisional(p.hotspot_id, p.species_code, p.lifer_type)
        logger.info(
            "pending provisional confirmed: %s at %s (%s)",
            p.common_name,
            p.hotspot_id,
            p.lifer_type,
        )

    for p in invalidated:
        await _remove_pending_provisional(p.hotspot_id, p.species_code, p.lifer_type)
        # Remove from known species so a future valid observation can trigger
        # a fresh alert.
        if p.lifer_type == "year" and p.year is not None:
            await _remove_year_species(p.hotspot_id, p.year, p.species_code)
        elif p.lifer_type == "all_time":
            await _remove_all_time_species(p.hotspot_id, p.species_code)
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
        checklist_url = f"https://ebird.org/checklist/{obs.subId}"
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
        checklist_url = f"https://ebird.org/checklist/{obs.subId}"
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
        checklist_url = f"https://ebird.org/checklist/{obs.subId}"
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
        checklist_url = f"https://ebird.org/checklist/{obs.subId}"
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
        return (
            f"👀 **Possible Year Bird for {hotspot_name}!**\n\n"
            f"**{obs.comName}** — reported by "
            f"{obs.userDisplayName} ({date_str})\n"
            "-# Awaiting eBird review — we'll celebrate once confirmed!"
        )

    lines = [
        f"👀 **{len(new_lifers)} Possible New Year Birds for {hotspot_name}!**",
        "",
    ]
    for obs in new_lifers:
        date_str = obs.obsDt.strftime("%b %-d")
        lines.append(f"**{obs.comName}** — {obs.userDisplayName} ({date_str})")
    lines.append("-# Awaiting eBird review — we'll celebrate once confirmed!")
    return "\n".join(lines)


def format_tentative_all_time_lifer_message(
    new_lifers: list[eBirdHistoricFullObservation],
    hotspot_name: str,
) -> str:
    if len(new_lifers) == 1:
        obs = new_lifers[0]
        date_str = obs.obsDt.strftime("%b %-d")
        return (
            f"👀 **Possible New Park Bird for {hotspot_name}!**\n\n"
            f"**{obs.comName}** — reported by "
            f"{obs.userDisplayName} ({date_str})\n"
            "-# Awaiting eBird review — we'll celebrate once confirmed!"
        )

    lines = [
        f"👀 **{len(new_lifers)} Possible New Park Birds for {hotspot_name}!**",
        "",
    ]
    for obs in new_lifers:
        date_str = obs.obsDt.strftime("%b %-d")
        lines.append(f"**{obs.comName}** — {obs.userDisplayName} ({date_str})")
    lines.append("-# Awaiting eBird review — we'll celebrate once confirmed!")
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
        return (
            f"{birds[0]} **Confirmed! Year Bird #{year_total} "
            f"for {hotspot_name}!**\n\n"
            f"**{p.common_name}** has been reviewed and confirmed on eBird!"
        )

    header = (
        f"{birds[0]}{birds[1]} **{len(confirmed)} Year Birds Confirmed "
        f"for {hotspot_name}!** (now at {year_total} species)"
    )
    lines = [header, ""]
    for p in confirmed:
        lines.append(f"**{p.common_name}** — confirmed!")
    return "\n".join(lines)


def format_confirmed_all_time_lifer_message(
    confirmed: list[PendingProvisional],
    hotspot_name: str,
    all_time_total: int,
) -> str:
    if len(confirmed) == 1:
        p = confirmed[0]
        return (
            f"🎉🥳 **Confirmed! New Park Bird for {hotspot_name}!** "
            f"(#{all_time_total} all-time)\n\n"
            f"**{p.common_name}** has been reviewed and confirmed on eBird!"
        )

    header = (
        f"🎉🥳 **{len(confirmed)} New Park Birds Confirmed "
        f"for {hotspot_name}!** (now at {all_time_total} species all-time)"
    )
    lines = [header, ""]
    for p in confirmed:
        lines.append(f"**{p.common_name}** — confirmed!")
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
        f"**Update:** {names} at {hotspot_name} were not confirmed after eBird review."
    )


def checklist_link_view(
    checklists: list[tuple[str, str]],
) -> discord.ui.View:
    """Button view linking to one or more eBird checklists.

    *checklists* is a list of ``(species_name, sub_id)`` tuples.
    """
    view = discord.ui.View()
    if len(checklists) == 1:
        _, sub_id = checklists[0]
        view.add_item(
            discord.ui.Button(
                style=discord.ButtonStyle.link,
                label="View Checklist on eBird",
                url=f"https://ebird.org/checklist/{sub_id}",
            )
        )
    else:
        # Discord views support max 25 items
        for name, sub_id in checklists[:25]:
            view.add_item(
                discord.ui.Button(
                    style=discord.ButtonStyle.link,
                    label=f"{name} checklist",
                    url=f"https://ebird.org/checklist/{sub_id}",
                )
            )
    return view


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

            print(f"Year total: {await get_year_total(hotspot.id)}")
            print(f"All-time total: {await get_all_time_total(hotspot.id)}")

            observations = await fetch_recent_observations(hotspot.id)
            notable = await fetch_notable_observations(hotspot.id)

            confirmed_at, provisional_at = await check_for_new_all_time_lifers(
                hotspot.id, observations, notable
            )
            if confirmed_at:
                total = await get_all_time_total(hotspot.id)
                print(format_all_time_lifer_message(confirmed_at, hotspot.name, total))
            if provisional_at:
                print(
                    format_tentative_all_time_lifer_message(
                        provisional_at, hotspot.name
                    )
                )
            if not confirmed_at and not provisional_at:
                print("No new all-time lifers found")

            # Filter out all-time lifers from year lifer notifications
            all_time_codes = {o.speciesCode for o in confirmed_at + provisional_at}
            confirmed_yr, provisional_yr = await check_for_new_year_lifers(
                hotspot.id, observations, notable
            )
            confirmed_yr = [
                o for o in confirmed_yr if o.speciesCode not in all_time_codes
            ]
            provisional_yr = [
                o for o in provisional_yr if o.speciesCode not in all_time_codes
            ]
            if confirmed_yr:
                total = await get_year_total(hotspot.id)
                print(format_year_lifer_message(confirmed_yr, hotspot.name, total))
            if provisional_yr:
                print(format_tentative_year_lifer_message(provisional_yr, hotspot.name))
            if not confirmed_yr and not provisional_yr:
                print("No new year lifers found")

            # Check pending provisionals
            confirmed_pending, invalidated = await check_pending_provisionals(
                hotspot.id, notable
            )
            if confirmed_pending:
                year_confirmed = [
                    p for p in confirmed_pending if p.lifer_type == "year"
                ]
                at_confirmed = [
                    p for p in confirmed_pending if p.lifer_type == "all_time"
                ]
                if at_confirmed:
                    total = await get_all_time_total(hotspot.id)
                    print(
                        format_confirmed_all_time_lifer_message(
                            at_confirmed, hotspot.name, total
                        )
                    )
                if year_confirmed:
                    total = await get_year_total(hotspot.id)
                    print(
                        format_confirmed_year_lifer_message(
                            year_confirmed, hotspot.name, total
                        )
                    )
            if invalidated:
                print(format_invalidated_lifer_message(invalidated, hotspot.name))

        await close_engine()

    asyncio.run(_main())
