"""Rare bird alert for NYC — filters eBird notable observations by ABA rarity code.

Polls the eBird notable observations endpoint for each NYC county every 15 minutes.
Only alerts on species with ABA code >= 3 (Rare, Casual, Accidental).
Deduplicates alerts: same species + region won't re-alert within 7 days.
"""

import csv
import datetime
import logging
from dataclasses import dataclass
from pathlib import Path

import discord
from phoebe_bird.types.data.observation import Observation

from cloaca.api.shared import get_phoebe_client
from cloaca.piper.db.queries import AsyncQuerier, InsertRareBirdAlertParams
from cloaca.piper.db_pool import get_engine

logger = logging.getLogger(__name__)


# NYC counties (eBird region codes)
NYC_COUNTIES = [
    ("US-NY-005", "Bronx"),
    ("US-NY-047", "Kings (Brooklyn)"),
    ("US-NY-061", "New York (Manhattan)"),
    ("US-NY-081", "Queens"),
    ("US-NY-085", "Richmond (Staten Island)"),
]

ABA_CODE_LABELS = {
    3: "Rare",
    4: "Casual",
    5: "Accidental",
    6: "Extinct/Extirpated",
}

# Minimum ABA code to trigger an alert
MIN_ABA_CODE = 3


@dataclass
class ABASpecies:
    species_code: str
    common_name: str
    aba_code: int


@dataclass
class RareBirdSighting:
    species_code: str
    common_name: str
    scientific_name: str
    aba_code: int
    obs_date: datetime.date
    observer_name: str
    sub_id: str
    location_name: str
    region_code: str
    county_name: str
    obs_valid: bool


# ---------------------------------------------------------------------------
# ABA code lookup
# ---------------------------------------------------------------------------

_aba_lookup: dict[str, ABASpecies] | None = None


def _load_aba_codes() -> dict[str, ABASpecies]:
    """Load ABA code mapping from CSV. Cached after first call."""
    global _aba_lookup
    if _aba_lookup is not None:
        return _aba_lookup

    csv_path = Path(__file__).parent / "data" / "aba_codes.csv"
    _aba_lookup = {}
    with open(csv_path) as f:
        reader = csv.DictReader(f)
        for row in reader:
            species_code = row["species_code"].strip()
            aba_code = int(row["aba_code"].strip())
            if aba_code >= MIN_ABA_CODE:
                _aba_lookup[species_code] = ABASpecies(
                    species_code=species_code,
                    common_name=row["common_name"].strip(),
                    aba_code=aba_code,
                )
    logger.info("loaded %d ABA code 3+ species from %s", len(_aba_lookup), csv_path)
    return _aba_lookup


def get_aba_code(species_code: str) -> ABASpecies | None:
    """Look up ABA code for a species. Returns None if not code 3+."""
    return _load_aba_codes().get(species_code)


# ---------------------------------------------------------------------------
# eBird API
# ---------------------------------------------------------------------------


async def fetch_notable_for_county(region_code: str) -> list[Observation]:
    """Fetch recent notable observations for a county."""
    return await get_phoebe_client().data.observations.recent.notable.list(
        region_code=region_code,
        back=7,
        detail="full",
    )


async def fetch_all_nyc_notables() -> list[tuple[str, str, list[Observation]]]:
    """Fetch notable observations for all NYC counties.

    Returns list of (region_code, county_name, observations) tuples.
    """
    results: list[tuple[str, str, list[Observation]]] = []
    for region_code, county_name in NYC_COUNTIES:
        try:
            observations = await fetch_notable_for_county(region_code)
            results.append((region_code, county_name, observations))
        except Exception:
            logger.exception(
                "failed to fetch notable observations for %s (%s)",
                county_name,
                region_code,
            )
    return results


# ---------------------------------------------------------------------------
# Filtering + dedup
# ---------------------------------------------------------------------------


def _filter_aba_rarities(
    observations: list[Observation],
    region_code: str,
    county_name: str,
) -> list[RareBirdSighting]:
    """Filter notable observations to only ABA code 3+ species.

    For each species, picks the earliest observation.
    """
    earliest: dict[str, RareBirdSighting] = {}
    for obs in observations:
        aba = get_aba_code(obs.species_code)
        if aba is None:
            continue

        obs_date = datetime.date.fromisoformat(obs.obs_dt[:10]) if obs.obs_dt else None
        if obs_date is None:
            continue

        sighting = RareBirdSighting(
            species_code=obs.species_code,
            common_name=obs.com_name or aba.common_name,
            scientific_name=obs.sci_name or "",
            aba_code=aba.aba_code,
            obs_date=obs_date,
            observer_name=obs.user_display_name or "Unknown",
            sub_id=obs.sub_id or "",
            location_name=obs.loc_name or "",
            region_code=region_code,
            county_name=county_name,
            obs_valid=obs.obs_valid if obs.obs_valid is not None else False,
        )

        key = obs.species_code
        if key not in earliest or obs_date < earliest[key].obs_date:
            earliest[key] = sighting

    return list(earliest.values())


async def _was_recently_alerted(species_code: str, region_code: str) -> bool:
    """Check if we already alerted on this species in this region within 7 days."""
    async with get_engine().connect() as conn:
        result = await AsyncQuerier(conn).get_recent_rare_bird_alert(
            species_code=species_code, region_code=region_code
        )
        return result is not None


async def _record_alert(sighting: RareBirdSighting) -> None:
    """Record that we sent an alert for this sighting."""
    async with get_engine().begin() as conn:
        await AsyncQuerier(conn).insert_rare_bird_alert(
            InsertRareBirdAlertParams(
                species_code=sighting.species_code,
                region_code=sighting.region_code,
                common_name=sighting.common_name,
                aba_code=sighting.aba_code,
                obs_date=sighting.obs_date,
                observer_name=sighting.observer_name,
                sub_id=sighting.sub_id,
                location_name=sighting.location_name,
            )
        )


async def check_for_rare_birds() -> list[RareBirdSighting]:
    """Poll all NYC counties for ABA Code 3+ species, deduplicated.

    Returns sightings that haven't been alerted on recently.
    """
    all_county_data = await fetch_all_nyc_notables()

    new_sightings: list[RareBirdSighting] = []
    for region_code, county_name, observations in all_county_data:
        rarities = _filter_aba_rarities(observations, region_code, county_name)
        for sighting in rarities:
            if await _was_recently_alerted(sighting.species_code, sighting.region_code):
                logger.debug(
                    "skipping %s in %s — already alerted recently",
                    sighting.common_name,
                    county_name,
                )
                continue
            await _record_alert(sighting)
            new_sightings.append(sighting)
            logger.info(
                "new rare bird alert: %s (ABA %d) in %s",
                sighting.common_name,
                sighting.aba_code,
                county_name,
            )

    return new_sightings


# ---------------------------------------------------------------------------
# Discord formatting
# ---------------------------------------------------------------------------


def _aba_emoji(aba_code: int) -> str:
    if aba_code >= 5:
        return "\U0001f6a8"  # 🚨
    if aba_code == 4:
        return "\U00002757"  # ❗
    return "\U0001f514"  # 🔔


def format_rare_bird_message(sightings: list[RareBirdSighting]) -> str:
    """Format one or more rare bird sightings into a Discord message."""
    if len(sightings) == 1:
        s = sightings[0]
        emoji = _aba_emoji(s.aba_code)
        label = ABA_CODE_LABELS.get(s.aba_code, f"Code {s.aba_code}")
        date_str = s.obs_date.strftime("%b %-d")
        checklist_url = f"https://ebird.org/checklist/{s.sub_id}"
        review_note = "" if s.obs_valid else "\n-# Awaiting eBird review"

        return (
            f"{emoji} **Rare Bird Alert — {s.common_name}** (ABA {label})\n\n"
            f"Spotted at **{s.location_name}** ({s.county_name})\n"
            f"Reported by {s.observer_name} ({date_str})\n"
            f"[View checklist]({checklist_url})"
            f"{review_note}"
        )

    # Multiple sightings
    lines = [
        f"\U0001f514 **Rare Bird Alert — {len(sightings)} species in NYC!**",
        "",
    ]
    for s in sorted(sightings, key=lambda x: x.aba_code, reverse=True):
        emoji = _aba_emoji(s.aba_code)
        label = ABA_CODE_LABELS.get(s.aba_code, f"Code {s.aba_code}")
        date_str = s.obs_date.strftime("%b %-d")
        checklist_url = f"https://ebird.org/checklist/{s.sub_id}"
        review = " *(unreviewed)*" if not s.obs_valid else ""
        lines.append(
            f"{emoji} **{s.common_name}** (ABA {label}) — "
            f"{s.location_name}, {s.county_name} · "
            f"{s.observer_name} ({date_str}) · "
            f"[checklist]({checklist_url}){review}"
        )
    return "\n".join(lines)


def rare_bird_link_view(sightings: list[RareBirdSighting]) -> discord.ui.View:
    """Button view linking to eBird checklists for rare bird sightings."""
    view = discord.ui.View()
    for s in sightings[:25]:
        view.add_item(
            discord.ui.Button(
                style=discord.ButtonStyle.link,
                label=f"{s.common_name} checklist",
                url=f"https://ebird.org/checklist/{s.sub_id}",
            )
        )
    return view


# ---------------------------------------------------------------------------
# CLI testing
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    import asyncio

    from dotenv import load_dotenv

    load_dotenv(override=True)
    logging.basicConfig(level=logging.INFO)

    async def _main():
        # Pre-load ABA codes
        codes = _load_aba_codes()
        print(f"Loaded {len(codes)} ABA code 3+ species")

        print("\nFetching notable observations for NYC counties...")
        all_data = await fetch_all_nyc_notables()

        for region_code, county_name, observations in all_data:
            print(f"\n--- {county_name} ({region_code}) ---")
            print(f"  Total notable observations: {len(observations)}")
            rarities = _filter_aba_rarities(observations, region_code, county_name)
            if rarities:
                for s in rarities:
                    label = ABA_CODE_LABELS.get(s.aba_code, f"Code {s.aba_code}")
                    valid = "confirmed" if s.obs_valid else "unreviewed"
                    print(
                        f"  ABA {label}: {s.common_name} — "
                        f"{s.location_name} ({s.observer_name}, {s.obs_date}) [{valid}]"
                    )
            else:
                print("  No ABA code 3+ species found")

    asyncio.run(_main())
