#!/usr/bin/env python3
"""eBird achievement engine — single-file, stdlib-only build.

GENERATED FILE — do not edit by hand. Regenerate with:
    python3 scripts/build_standalone_achievements.py

Runs anywhere Python 3.11+ exists, with no repo checkout, no virtualenv, and
no third-party packages:

    python3 ebird_achievements_standalone.py MyEBirdData.csv --since 2026-08-13

Output is byte-identical to `python -m cloaca.achievements.cli`.
"""

from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass
from dataclasses import dataclass, field
from enum import Enum
import argparse
import csv
import datetime

# Expected MyEBirdData.csv columns
EXPECTED_HEADERS = [
    "Submission ID",
    "Common Name",
    "Scientific Name",
    "Taxonomic Order",
    "Count",
    "State/Province",
    "County",
    "Location ID",
    "Location",
    "Latitude",
    "Longitude",
    "Date",
    "Time",
    "Protocol",
    "Duration (Min)",
    "All Obs Reported",
    "Distance Traveled (km)",
    "Area Covered (ha)",
    "Number of Observers",
    "Breeding Code",
    "Observation Details",
    "Checklist Comments",
    "ML Catalog Numbers",
]

# --- from src/cloaca/parsing/parsing_helpers.py ---

@dataclass
class Observation:
    submission_id: str
    common_name: str
    scientific_name: str
    taxonomic_order: int
    count: int
    state_province: str
    county: str
    location_id: str
    location: str
    latitude: float
    longitude: float
    date: str
    time: str
    protocol: str
    duration_min: int
    all_obs_reported: str
    distance_traveled_km: float
    area_covered_ha: float
    number_of_observers: int
    breeding_code: str
    observation_details: str
    checklist_comments: str
    ml_catalog_numbers: str


# remove:
# spuh (eg `gull sp.`)
# combo species (eg `long-billed/short-billed dowitcher`)
# hybrid species (eg `Mallard x American Black Duck`)
def is_singular_bird_species(observation: Observation) -> bool:
    scientific_name = observation.scientific_name
    common_name = observation.common_name

    if "sp." in scientific_name or "sp." in common_name:
        return False

    if "/" in scientific_name or "/" in common_name:
        return False

    if " x " in scientific_name or " x " in common_name:
        return False

    return True


# Get lifers (first observation of each species)
def get_lifers(observations):
    lifers: dict[str, Observation] = {}
    for obs in observations:
        if not is_singular_bird_species(obs):
            print(
                f"removing unwanted observation {obs.common_name}({obs.scientific_name})"
            )
            continue

        if obs.scientific_name not in lifers:
            lifers[obs.scientific_name] = obs
    return list(lifers.values())


@dataclass
class Lifer:
    common_name: str
    latitude: float
    longitude: float
    date: str
    taxonomic_order: int
    location: str
    location_id: str
    scientific_name: str
    species_code: str | None = None


@dataclass
class Location:
    location_name: str
    latitude: float
    longitude: float
    location_id: str


@dataclass
class LocationToLifers:
    location: Location
    lifers: list[Lifer]


def observations_to_lifers(observations: list[Observation]) -> list[Lifer]:
    return [
        Lifer(
            common_name=obs.common_name,
            latitude=obs.latitude,
            longitude=obs.longitude,
            date=obs.date,
            taxonomic_order=obs.taxonomic_order,
            location=obs.location,
            location_id=obs.location_id,
            scientific_name=obs.scientific_name,
        )
        for obs in observations
    ]


@dataclass
class HomeLocation:
    location_id: str
    location_name: str
    latitude: float
    longitude: float
    checklist_count: int


def calculate_home_location(observations: list[Observation]) -> HomeLocation | None:
    """Calculate home location as the hotspot with the most checklists"""
    if not observations:
        return None

    # Group by location and count unique submission IDs (checklists)
    location_checklist_counts: dict[str, dict] = {}

    for obs in observations:
        if obs.location_id not in location_checklist_counts:
            location_checklist_counts[obs.location_id] = {
                "location_name": obs.location,
                "latitude": obs.latitude,
                "longitude": obs.longitude,
                "submission_ids": set(),
            }

        location_checklist_counts[obs.location_id]["submission_ids"].add(
            obs.submission_id
        )

    # Find location with most checklists
    max_checklists = 0
    home_location_data = None

    for location_id, data in location_checklist_counts.items():
        checklist_count = len(data["submission_ids"])
        if checklist_count > max_checklists:
            max_checklists = checklist_count
            home_location_data = {
                "location_id": location_id,
                "location_name": data["location_name"],
                "latitude": data["latitude"],
                "longitude": data["longitude"],
                "checklist_count": checklist_count,
            }

    if home_location_data:
        return HomeLocation(**home_location_data)

    return None


# --- from src/cloaca/achievements/config.py ---

@dataclass(frozen=True)
class Patch:
    name: str
    # Exact eBird location IDs that belong to this patch
    location_ids: frozenset[str] = frozenset()
    # Case-insensitive substrings matched against the location name, so new
    # personal locations (eg "McGolrick Park--north end") still count
    name_substrings: tuple[str, ...] = ()

    def matches(self, obs: Observation) -> bool:
        if obs.location_id in self.location_ids:
            return True
        location_name = str(obs.location).lower()
        return any(sub.lower() in location_name for sub in self.name_substrings)


@dataclass(frozen=True)
class Region:
    name: str
    # Full state/province codes, eg "US-NY"
    state_codes: frozenset[str] = frozenset()
    # (state_code, county_name) pairs for sub-state regions, eg ("US-MA", "Dukes")
    counties: frozenset[tuple[str, str]] = field(default_factory=frozenset)

    def matches(self, obs: Observation) -> bool:
        state = str(obs.state_province)
        if state in self.state_codes:
            return True
        county = str(obs.county)
        return (state, county) in self.counties


DEFAULT_PATCHES: tuple[Patch, ...] = (
    Patch(
        name="McGolrick Park",
        location_ids=frozenset({"L2987624"}),
        name_substrings=("mcgolrick",),
    ),
    Patch(
        # "My House" spans past + present home locations; add/remove IDs as
        # you move or create new personal locations at home. The "emily"
        # substring catches renames like "Emily's Backyard" ->
        # "Emily and David's Backyard" (eBird re-IDs renamed locations).
        name="My House",
        location_ids=frozenset({"L23333013", "L30645449", "L41095929"}),
        name_substrings=("emily",),
    ),
)

# Days with a submitted checklist that reported zero species. eBird's streak
# counts these, but they leave no observation rows in a personal export, so
# they're invisible to the engine. List them here (ISO dates) to keep streaks
# aligned with eBird. (2024-12-29: timezone-straddling flight to NZ;
# 2025-11-06: same trip.)
KNOWN_EMPTY_CHECKLIST_DAYS: frozenset[str] = frozenset(
    {
        "2024-12-29",
        "2025-11-06",
    }
)

DEFAULT_REGIONS: tuple[Region, ...] = (
    Region(name="New York", state_codes=frozenset({"US-NY"})),
    Region(
        name="Martha's Vineyard",
        counties=frozenset({("US-MA", "Dukes")}),
    ),
)


# --- from src/cloaca/achievements/engine.py ---

class Tier(str, Enum):
    GOLD = "gold"
    SILVER = "silver"
    BRONZE = "bronze"


TIER_EMOJI = {Tier.GOLD: "🏆", Tier.SILVER: "✨", Tier.BRONZE: "🌱"}


class AchievementType(str, Enum):
    LIFER = "lifer"
    PATCH_FIRST = "patch_first"
    PATCH_FIRST_OF_YEAR = "patch_first_of_year"
    REGION_FIRST = "region_first"
    FIRST_OF_YEAR = "first_of_year"
    REUNION = "reunion"
    BEST_BUDS = "best_buds"
    BIG_DAY = "big_day"
    BIG_CHECKLIST = "big_checklist"
    STREAK = "streak"
    SPARK_DAY = "spark_day"
    DAWN_PATROL = "dawn_patrol"
    NIGHT_OWL = "night_owl"
    NEW_COUNTRY = "new_country"
    NEW_STATE = "new_state"
    CHECKLIST_MILESTONE = "checklist_milestone"
    PATCH_VISIT_MILESTONE = "patch_visit_milestone"
    YEAR_LIST_MILESTONE = "year_list_milestone"


@dataclass
class Achievement:
    key: str  # stable dedupe key, eg "lifer:Anser caerulescens"
    type: AchievementType
    tier: Tier
    date: str  # ISO date the achievement was earned
    title: str
    description: str
    species: str | None = None  # common name
    location: str | None = None
    context: dict = field(default_factory=dict)

    @property
    def emoji(self) -> str:
        return TIER_EMOJI[self.tier]


@dataclass
class AchievementSummary:
    life_list_total: int
    total_checklists: int
    patch_totals: dict[str, int]
    region_totals: dict[str, int]
    biggest_day: tuple[str, int] | None  # (date, species count)
    longest_streak: int
    current_streak: int  # streak still alive on the last birding day in the export
    countries: int
    states: int
    events_by_tier: dict[str, int]


LIFER_MILESTONES = {25, 50, 100} | set(range(150, 10001, 50))
PATCH_LIST_MILESTONES = {10, 25, 50, 75, 100, 125, 150, 175, 200, 250, 300}
REGION_LIST_MILESTONES = set(range(50, 10001, 50))
CHECKLIST_MILESTONES = {10, 50, 100, 250, 500, 1000, 2000, 5000}
PATCH_VISIT_MILESTONES = {10, 25, 50, 100, 250, 500}
YEAR_LIST_MILESTONES = {100, 150, 200, 250, 300, 350, 400}
STREAK_MILESTONES = {3, 5, 7, 14, 21, 30, 50, 100, 365}
SPECIES_ENCOUNTER_MILESTONES = {50, 100, 250, 500}
REUNION_MIN_DAYS = 365
BIG_CHECKLIST_THRESHOLD = 50
CENTURY_CHECKLIST_THRESHOLD = 100
BIG_DAY_MIN_SPECIES = 15
DAWN_PATROL_CUTOFF = datetime.time(6, 0)
NIGHT_OWL_CUTOFF = datetime.time(21, 0)

PATCH_VISIT_TITLES = {
    10: "Regular",
    25: "Local Fixture",
    50: "Honorary Park Ranger",
    100: "Local Legend",
    250: "Mayor of the Patch",
    500: "The Patch Is Named After You Now",
}


def _species_key(scientific_name: str) -> str:
    """Collapse subspecies/forms to the species-level binomial.

    "Setophaga coronata coronata" (Myrtle), "Columba livia (Feral Pigeon)",
    and "Junco hyemalis [oreganus Group]" all key as their first two words,
    so forms don't show up as separate lifers/patch birds.
    """
    words = str(scientific_name).replace("[", " ").replace("(", " ").split()
    return " ".join(words[:2])


def _display_name(common_name: str) -> str:
    """Strip form qualifiers: "Dark-eyed Junco (Oregon)" -> "Dark-eyed Junco"."""
    name = str(common_name)
    idx = name.find(" (")
    return name[:idx] if idx > 0 else name


def _parse_date(raw: str) -> datetime.date | None:
    try:
        return datetime.date.fromisoformat(str(raw))
    except ValueError:
        return None


def _parse_time(raw) -> datetime.time | None:
    for fmt in ("%I:%M %p", "%H:%M"):
        try:
            return datetime.datetime.strptime(str(raw).strip(), fmt).time()
        except ValueError:
            continue
    return None


def _years_and_days(days: int) -> str:
    years = days / 365.25
    if years >= 2:
        return f"{years:.1f} years"
    return f"{days} days"


def _obs_sort_key(obs: Observation) -> tuple:
    date = _parse_date(obs.date) or datetime.date.min
    time = _parse_time(obs.time) or datetime.time.min
    return (date, time, str(obs.submission_id), obs.taxonomic_order)


@dataclass
class _ChecklistInfo:
    submission_id: str
    date: datetime.date
    time: datetime.time | None
    location: str
    location_id: str
    species_count: int


class _Emitter:
    """Collects events with a monotonic sequence for stable final ordering."""

    def __init__(self) -> None:
        self.events: list[tuple[datetime.date, int, Achievement]] = []
        self._seq = 0

    def emit(self, date: datetime.date, achievement: Achievement) -> None:
        self.events.append((date, self._seq, achievement))
        self._seq += 1

    def sorted_achievements(self) -> list[Achievement]:
        return [a for _, _, a in sorted(self.events, key=lambda e: (e[0], e[1]))]


def compute_achievements(
    observations: list[Observation],
    patches: tuple[Patch, ...] = DEFAULT_PATCHES,
    regions: tuple[Region, ...] = DEFAULT_REGIONS,
    extra_birding_days: frozenset[str] = KNOWN_EMPTY_CHECKLIST_DAYS,
) -> list[Achievement]:
    """Replay observations chronologically and return the achievement timeline."""
    dated_obs = [o for o in observations if _parse_date(o.date) is not None]
    species_obs = [o for o in dated_obs if is_singular_bird_species(o)]
    species_obs.sort(key=_obs_sort_key)

    checklists = _summarize_checklists(species_obs)

    out = _Emitter()

    life_list: dict[str, int] = {}  # sci name -> lifer number
    lifer_dates: dict[str, datetime.date] = {}
    last_seen: dict[str, datetime.date] = {}
    encounter_counts: dict[str, int] = defaultdict(int)
    year_lists: dict[int, set[str]] = defaultdict(set)
    patch_lists: dict[str, dict[str, datetime.date]] = defaultdict(dict)
    patch_year_lists: dict[tuple[str, int], set[str]] = defaultdict(set)
    region_lists: dict[str, set[str]] = defaultdict(set)

    seen_checklists: set[str] = set()
    patch_visits: dict[str, set[str]] = defaultdict(set)
    seen_countries: set[str] = set()
    seen_states: set[str] = set()
    dawn_patrol_done = False
    night_owl_done = False

    for obs in species_obs:
        date = _parse_date(obs.date)
        assert date is not None  # filtered above
        sub_id = str(obs.submission_id)

        if sub_id not in seen_checklists:
            seen_checklists.add(sub_id)
            info = checklists[sub_id]
            dawn_patrol_done, night_owl_done = _process_new_checklist(
                out,
                info,
                checklist_number=len(seen_checklists),
                patches=patches,
                patch_visits=patch_visits,
                seen_countries=seen_countries,
                seen_states=seen_states,
                state=str(obs.state_province),
                dawn_patrol_done=dawn_patrol_done,
                night_owl_done=night_owl_done,
            )

        _process_species_sighting(
            out,
            obs,
            date,
            patches=patches,
            regions=regions,
            life_list=life_list,
            lifer_dates=lifer_dates,
            last_seen=last_seen,
            encounter_counts=encounter_counts,
            year_lists=year_lists,
            patch_lists=patch_lists,
            patch_year_lists=patch_year_lists,
            region_lists=region_lists,
        )

    _process_big_days(out, species_obs)
    # Streaks match eBird's logic: any day with a checklist counts, including
    # spuh/hybrid-only days. Days whose only checklist reported zero species
    # leave no rows in the export, so they come in via extra_birding_days.
    _process_streaks(out, _birding_days(dated_obs, extra_birding_days))

    return out.sorted_achievements()


def _birding_days(
    dated_obs: list[Observation], extra_birding_days: frozenset[str]
) -> list[datetime.date]:
    days = {d for o in dated_obs if (d := _parse_date(o.date)) is not None}
    days.update(d for raw in extra_birding_days if (d := _parse_date(raw)) is not None)
    return sorted(days)


def _summarize_checklists(
    species_obs: list[Observation],
) -> dict[str, _ChecklistInfo]:
    checklists: dict[str, _ChecklistInfo] = {}
    species_per_checklist: dict[str, set[str]] = defaultdict(set)
    for obs in species_obs:
        sub_id = str(obs.submission_id)
        species_per_checklist[sub_id].add(_species_key(obs.scientific_name))
        if sub_id not in checklists:
            date = _parse_date(obs.date)
            assert date is not None
            checklists[sub_id] = _ChecklistInfo(
                submission_id=sub_id,
                date=date,
                time=_parse_time(obs.time),
                location=str(obs.location),
                location_id=str(obs.location_id),
                species_count=0,
            )
    for sub_id, info in checklists.items():
        info.species_count = len(species_per_checklist[sub_id])
    return checklists


def _process_new_checklist(
    out: _Emitter,
    info: _ChecklistInfo,
    checklist_number: int,
    patches: tuple[Patch, ...],
    patch_visits: dict[str, set[str]],
    seen_countries: set[str],
    seen_states: set[str],
    state: str,
    dawn_patrol_done: bool,
    night_owl_done: bool,
) -> tuple[bool, bool]:
    date_iso = info.date.isoformat()

    if checklist_number == 1:
        out.emit(
            info.date,
            Achievement(
                key="spark_day",
                type=AchievementType.SPARK_DAY,
                tier=Tier.GOLD,
                date=date_iso,
                title="🎇 Spark day — where it all began",
                description=(
                    f"Your very first checklist: {info.location} on {date_iso}."
                ),
                location=info.location,
            ),
        )

    if checklist_number in CHECKLIST_MILESTONES:
        out.emit(
            info.date,
            Achievement(
                key=f"checklist_milestone:{checklist_number}",
                type=AchievementType.CHECKLIST_MILESTONE,
                tier=Tier.SILVER if checklist_number >= 250 else Tier.BRONZE,
                date=date_iso,
                title=f"🧾 Checklist #{checklist_number}",
                description=(
                    f"You've submitted {checklist_number} checklists — "
                    f"#{checklist_number} was at {info.location}."
                ),
                location=info.location,
                context={"checklist_number": checklist_number},
            ),
        )

    country = state.split("-")[0] if state and state != "nan" else ""
    if country and country not in seen_countries:
        seen_countries.add(country)
        if len(seen_countries) > 1:  # your first-ever country isn't news
            out.emit(
                info.date,
                Achievement(
                    key=f"new_country:{country}",
                    type=AchievementType.NEW_COUNTRY,
                    tier=Tier.GOLD,
                    date=date_iso,
                    title=f"🛂 Passport stamp: {country}",
                    description=(
                        f"First checklist in {country} — country #{len(seen_countries)}! "
                        f"({info.location})"
                    ),
                    location=info.location,
                    context={"country": country, "number": len(seen_countries)},
                ),
            )
    if state and state != "nan" and state not in seen_states:
        seen_states.add(state)
        if len(seen_states) > 1:
            out.emit(
                info.date,
                Achievement(
                    key=f"new_state:{state}",
                    type=AchievementType.NEW_STATE,
                    tier=Tier.SILVER,
                    date=date_iso,
                    title=f"🗺️ New state/province: {state}",
                    description=(
                        f"First checklist in {state} — region #{len(seen_states)} "
                        f"on your map. ({info.location})"
                    ),
                    location=info.location,
                    context={"state": state, "number": len(seen_states)},
                ),
            )

    for patch in patches:
        if _checklist_in_patch(info, patch):
            patch_visits[patch.name].add(info.submission_id)
            visit_count = len(patch_visits[patch.name])
            if visit_count in PATCH_VISIT_MILESTONES:
                nickname = PATCH_VISIT_TITLES.get(visit_count, "Devotee")
                out.emit(
                    info.date,
                    Achievement(
                        key=f"patch_visits:{patch.name}:{visit_count}",
                        type=AchievementType.PATCH_VISIT_MILESTONE,
                        tier=Tier.GOLD if visit_count >= 100 else Tier.SILVER,
                        date=date_iso,
                        title=f"🪑 {nickname}: {visit_count} visits to {patch.name}",
                        description=(
                            f"That's checklist #{visit_count} at {patch.name}. "
                            "The birds are starting to recognize you."
                        ),
                        location=patch.name,
                        context={"patch": patch.name, "visits": visit_count},
                    ),
                )

    if info.time is not None:
        if not dawn_patrol_done and info.time < DAWN_PATROL_CUTOFF:
            dawn_patrol_done = True
            out.emit(
                info.date,
                Achievement(
                    key="dawn_patrol",
                    type=AchievementType.DAWN_PATROL,
                    tier=Tier.BRONZE,
                    date=date_iso,
                    title="🌅 Dawn patrol",
                    description=(
                        f"First checklist started before 6 AM "
                        f"({info.time.strftime('%-I:%M %p')} at {info.location})."
                    ),
                    location=info.location,
                ),
            )
        if not night_owl_done and info.time >= NIGHT_OWL_CUTOFF:
            night_owl_done = True
            out.emit(
                info.date,
                Achievement(
                    key="night_owl",
                    type=AchievementType.NIGHT_OWL,
                    tier=Tier.BRONZE,
                    date=date_iso,
                    title="🦉 Night owl",
                    description=(
                        f"First checklist started after 9 PM "
                        f"({info.time.strftime('%-I:%M %p')} at {info.location})."
                    ),
                    location=info.location,
                ),
            )

    if info.species_count >= BIG_CHECKLIST_THRESHOLD:
        century = info.species_count >= CENTURY_CHECKLIST_THRESHOLD
        out.emit(
            info.date,
            Achievement(
                key=f"big_checklist:{info.submission_id}",
                type=AchievementType.BIG_CHECKLIST,
                tier=Tier.GOLD if century else Tier.SILVER,
                date=date_iso,
                title=(
                    f"💯 Century checklist: {info.species_count} species"
                    if century
                    else f"🧮 Big checklist: {info.species_count} species"
                ),
                description=(
                    f"{info.species_count} species on a single checklist at "
                    f"{info.location}."
                ),
                location=info.location,
                context={"species_count": info.species_count},
            ),
        )

    return dawn_patrol_done, night_owl_done


def _checklist_in_patch(info: _ChecklistInfo, patch: Patch) -> bool:
    if info.location_id in patch.location_ids:
        return True
    location_name = info.location.lower()
    return any(sub.lower() in location_name for sub in patch.name_substrings)


def _process_species_sighting(
    out: _Emitter,
    obs: Observation,
    date: datetime.date,
    patches: tuple[Patch, ...],
    regions: tuple[Region, ...],
    life_list: dict[str, int],
    lifer_dates: dict[str, datetime.date],
    last_seen: dict[str, datetime.date],
    encounter_counts: dict[str, int],
    year_lists: dict[int, set[str]],
    patch_lists: dict[str, dict[str, datetime.date]],
    patch_year_lists: dict[tuple[str, int], set[str]],
    region_lists: dict[str, set[str]],
) -> None:
    sci = _species_key(obs.scientific_name)
    common = _display_name(obs.common_name)
    date_iso = date.isoformat()
    year = date.year

    is_lifer = sci not in life_list
    if is_lifer:
        number = len(life_list) + 1
        life_list[sci] = number
        lifer_dates[sci] = date
        milestone = number in LIFER_MILESTONES
        out.emit(
            date,
            Achievement(
                key=f"lifer:{sci}",
                type=AchievementType.LIFER,
                tier=Tier.GOLD if milestone else Tier.SILVER,
                date=date_iso,
                title=(
                    f"🎉 LIFER MILESTONE — #{number}: {common}"
                    if milestone
                    else f"🐦 Lifer #{number}: {common}"
                ),
                description=(
                    f"Your first-ever {common}, at {obs.location}."
                    + (f" That's {number} species on your life list!" if milestone else "")
                ),
                species=common,
                location=str(obs.location),
                context={"number": number, "milestone": milestone},
            ),
        )
    else:
        gap_days = (date - last_seen[sci]).days
        if gap_days >= REUNION_MIN_DAYS:
            long_lost = gap_days >= 3 * 365
            out.emit(
                date,
                Achievement(
                    key=f"reunion:{sci}:{date_iso}",
                    type=AchievementType.REUNION,
                    tier=Tier.SILVER if long_lost else Tier.BRONZE,
                    date=date_iso,
                    title=(
                        f"🫂 Long-lost friend: {common}"
                        if long_lost
                        else f"🤝 Old friend: {common}"
                    ),
                    description=(
                        f"First {common} in {_years_and_days(gap_days)} "
                        f"(at {obs.location})."
                    ),
                    species=common,
                    location=str(obs.location),
                    context={"gap_days": gap_days},
                ),
            )

    if year not in year_lists or sci not in year_lists[year]:
        first_of_year = sci in life_list and not is_lifer
        year_lists[year].add(sci)
        if first_of_year:
            out.emit(
                date,
                Achievement(
                    key=f"foy:{year}:{sci}",
                    type=AchievementType.FIRST_OF_YEAR,
                    tier=Tier.BRONZE,
                    date=date_iso,
                    title=f"📅 FOY {year}: {common}",
                    description=(
                        f"First {common} of {year} "
                        f"(#{len(year_lists[year])} for the year, at {obs.location})."
                    ),
                    species=common,
                    location=str(obs.location),
                    context={"year": year, "year_number": len(year_lists[year])},
                ),
            )
        year_count = len(year_lists[year])
        if year_count in YEAR_LIST_MILESTONES:
            out.emit(
                date,
                Achievement(
                    key=f"year_list:{year}:{year_count}",
                    type=AchievementType.YEAR_LIST_MILESTONE,
                    tier=Tier.GOLD if year_count >= 200 else Tier.SILVER,
                    date=date_iso,
                    title=f"📈 {year_count} species in {year}",
                    description=(
                        f"{common} was species #{year_count} for {year}. "
                        "Big year energy."
                    ),
                    species=common,
                    context={"year": year, "count": year_count},
                ),
            )

    encounter_counts[sci] += 1
    if encounter_counts[sci] in SPECIES_ENCOUNTER_MILESTONES:
        count = encounter_counts[sci]
        out.emit(
            date,
            Achievement(
                key=f"best_buds:{sci}:{count}",
                type=AchievementType.BEST_BUDS,
                tier=Tier.BRONZE,
                date=date_iso,
                title=f"❤️ Best buds: {common} × {count}",
                description=(
                    f"That's the {count}th time you've logged {common}. "
                    "At this point you owe each other holiday cards."
                ),
                species=common,
                context={"count": count},
            ),
        )

    last_seen[sci] = date

    for patch in patches:
        if not patch.matches(obs):
            continue
        if sci not in patch_lists[patch.name]:
            patch_lists[patch.name][sci] = date
            patch_count = len(patch_lists[patch.name])
            milestone = patch_count in PATCH_LIST_MILESTONES
            out.emit(
                date,
                Achievement(
                    key=f"patch_first:{patch.name}:{sci}",
                    type=AchievementType.PATCH_FIRST,
                    tier=Tier.GOLD,
                    date=date_iso,
                    title=(
                        f"🏞️ Patch bird #{patch_count} for {patch.name}: {common}"
                        + (" — milestone!" if milestone else "")
                    ),
                    description=(
                        f"First {common} ever recorded on your {patch.name} list."
                    ),
                    species=common,
                    location=patch.name,
                    context={
                        "patch": patch.name,
                        "patch_number": patch_count,
                        "milestone": milestone,
                    },
                ),
            )
        elif sci not in patch_year_lists[(patch.name, year)]:
            out.emit(
                date,
                Achievement(
                    key=f"patch_foy:{patch.name}:{year}:{sci}",
                    type=AchievementType.PATCH_FIRST_OF_YEAR,
                    tier=Tier.BRONZE,
                    date=date_iso,
                    title=f"🌳 {patch.name} FOY {year}: {common}",
                    description=(
                        f"First {common} of {year} at {patch.name}."
                    ),
                    species=common,
                    location=patch.name,
                    context={"patch": patch.name, "year": year},
                ),
            )
        patch_year_lists[(patch.name, year)].add(sci)

    for region in regions:
        if not region.matches(obs):
            continue
        if sci in region_lists[region.name]:
            continue
        region_lists[region.name].add(sci)
        region_count = len(region_lists[region.name])
        milestone = region_count in REGION_LIST_MILESTONES
        out.emit(
            date,
            Achievement(
                key=f"region_first:{region.name}:{sci}",
                type=AchievementType.REGION_FIRST,
                tier=Tier.GOLD if milestone else Tier.SILVER,
                date=date_iso,
                title=(
                    f"📍 First {common} for {region.name} (#{region_count})"
                    + (" — milestone!" if milestone else "")
                ),
                description=(
                    f"New for your {region.name} list, at {obs.location}."
                ),
                species=common,
                location=str(obs.location),
                context={
                    "region": region.name,
                    "region_number": region_count,
                    "milestone": milestone,
                },
            ),
        )


def _process_big_days(out: _Emitter, species_obs: list[Observation]) -> None:
    species_by_day: dict[datetime.date, set[str]] = defaultdict(set)
    for obs in species_obs:
        date = _parse_date(obs.date)
        assert date is not None
        species_by_day[date].add(_species_key(obs.scientific_name))

    record = 0
    for date in sorted(species_by_day):
        count = len(species_by_day[date])
        if count > record:
            previous = record
            record = count
            # The first day over the bar sets a baseline; later record-breakers
            # get celebrated with the previous record for context.
            if count >= BIG_DAY_MIN_SPECIES and previous >= BIG_DAY_MIN_SPECIES:
                out.emit(
                    date,
                    Achievement(
                        key=f"big_day:{date.isoformat()}",
                        type=AchievementType.BIG_DAY,
                        tier=Tier.GOLD,
                        date=date.isoformat(),
                        title=f"🔥 New biggest day: {count} species",
                        description=(
                            f"{count} species in one day — your previous record "
                            f"was {previous}."
                        ),
                        context={"count": count, "previous_record": previous},
                    ),
                )


def _process_streaks(out: _Emitter, dates: list[datetime.date]) -> None:
    streak = 0
    previous: datetime.date | None = None
    for date in dates:
        if previous is not None and (date - previous).days == 1:
            streak += 1
        else:
            streak = 1
        previous = date
        if streak in STREAK_MILESTONES:
            tier = (
                Tier.GOLD
                if streak >= 30
                else Tier.SILVER
                if streak >= 7
                else Tier.BRONZE
            )
            out.emit(
                date,
                Achievement(
                    key=f"streak:{date.isoformat()}:{streak}",
                    type=AchievementType.STREAK,
                    tier=tier,
                    date=date.isoformat(),
                    title=f"🔥 {streak}-day birding streak",
                    description=f"Checklists on {streak} consecutive days.",
                    context={"length": streak},
                ),
            )


def filter_since(
    achievements: list[Achievement], since: datetime.date
) -> list[Achievement]:
    """Events earned strictly after ``since`` — ie 'what did I just unlock?'"""
    return [
        a
        for a in achievements
        if (d := _parse_date(a.date)) is not None and d > since
    ]


def summarize(
    observations: list[Observation],
    achievements: list[Achievement],
    patches: tuple[Patch, ...] = DEFAULT_PATCHES,
    regions: tuple[Region, ...] = DEFAULT_REGIONS,
    extra_birding_days: frozenset[str] = KNOWN_EMPTY_CHECKLIST_DAYS,
) -> AchievementSummary:
    species_obs = [o for o in observations if is_singular_bird_species(o)]

    life_list = {_species_key(o.scientific_name) for o in species_obs}
    checklists = {str(o.submission_id) for o in species_obs}

    patch_totals = {
        p.name: len({_species_key(o.scientific_name) for o in species_obs if p.matches(o)})
        for p in patches
    }
    region_totals = {
        r.name: len({_species_key(o.scientific_name) for o in species_obs if r.matches(o)})
        for r in regions
    }

    species_by_day: dict[str, set[str]] = defaultdict(set)
    for o in species_obs:
        if _parse_date(o.date) is not None:
            species_by_day[str(o.date)].add(_species_key(o.scientific_name))
    biggest_day = (
        max(species_by_day.items(), key=lambda kv: len(kv[1])) if species_by_day else None
    )

    longest_streak = 0
    streak = 0
    previous: datetime.date | None = None
    # eBird streak logic: any checklist day counts (spuh-only days, plus
    # configured zero-species days that leave no rows in the export)
    dated = [o for o in observations if _parse_date(o.date) is not None]
    for date in _birding_days(dated, extra_birding_days):
        streak = streak + 1 if previous and (date - previous).days == 1 else 1
        previous = date
        longest_streak = max(longest_streak, streak)
    current_streak = streak

    states = {
        str(o.state_province)
        for o in species_obs
        if str(o.state_province) and str(o.state_province) != "nan"
    }
    countries = {s.split("-")[0] for s in states}

    events_by_tier: dict[str, int] = defaultdict(int)
    for a in achievements:
        events_by_tier[a.tier.value] += 1

    return AchievementSummary(
        life_list_total=len(life_list),
        total_checklists=len(checklists),
        patch_totals=patch_totals,
        region_totals=region_totals,
        biggest_day=(biggest_day[0], len(biggest_day[1])) if biggest_day else None,
        longest_streak=longest_streak,
        current_streak=current_streak,
        countries=len(countries),
        states=len(states),
        events_by_tier=dict(events_by_tier),
    )


# ---------------------------------------------------------------------------
# CSV reading (stdlib csv in place of pandas)
# ---------------------------------------------------------------------------


def read_observations(csv_path: str) -> list[Observation]:
    with open(csv_path, newline="", encoding="utf-8") as handle:
        reader = csv.DictReader(handle)
        if reader.fieldnames != EXPECTED_HEADERS:
            raise ValueError("The CSV headers do not match the expected headers.")
        rows = list(reader)

    # Mirror the pandas path, which sorts by Date then Time before parsing.
    rows.sort(key=lambda r: (r["Date"] or "", r["Time"] or ""))

    def to_int(value: str) -> int:
        try:
            return int(value)
        except (TypeError, ValueError):
            return 0

    def to_float(value: str) -> float:
        try:
            return float(value)
        except (TypeError, ValueError):
            return 0.0

    return [
        Observation(
            submission_id=row["Submission ID"],
            common_name=row["Common Name"],
            scientific_name=row["Scientific Name"],
            # int, matching pandas — the sort tiebreaker depends on it
            taxonomic_order=to_int(row["Taxonomic Order"]),
            count=row["Count"],
            state_province=row["State/Province"],
            county=row["County"],
            location_id=row["Location ID"],
            location=row["Location"],
            latitude=to_float(row["Latitude"]),
            longitude=to_float(row["Longitude"]),
            date=row["Date"],
            time=row["Time"],
            protocol=row["Protocol"],
            duration_min=row["Duration (Min)"],
            all_obs_reported=row["All Obs Reported"],
            distance_traveled_km=row["Distance Traveled (km)"],
            area_covered_ha=row["Area Covered (ha)"],
            number_of_observers=row["Number of Observers"],
            breeding_code=row["Breeding Code"],
            observation_details=row["Observation Details"],
            checklist_comments=row["Checklist Comments"],
            ml_catalog_numbers=row["ML Catalog Numbers"],
        )
        for row in rows
    ]


# ---------------------------------------------------------------------------
# CLI (mirrors cloaca.achievements.cli)
# ---------------------------------------------------------------------------


def _print_event(a: Achievement) -> None:
    print(f"  {a.date}  {a.emoji}  {a.title}")
    print(f"              {a.description}")


def main() -> None:
    parser = argparse.ArgumentParser(description="eBird achievement report")
    parser.add_argument("csv_path", help="Path to MyEBirdData.csv")
    parser.add_argument(
        "--since",
        type=datetime.date.fromisoformat,
        default=None,
        help="Show events after this date (default: 30 days ago)",
    )
    parser.add_argument(
        "--full", action="store_true", help="Print the entire achievement timeline"
    )
    args = parser.parse_args()

    observations = read_observations(args.csv_path)
    achievements = compute_achievements(observations)
    summary = summarize(observations, achievements)

    print("=" * 72)
    print("\U0001f3c6  ACHIEVEMENT REPORT")
    print("=" * 72)
    print(f"Life list: {summary.life_list_total} species")
    print(f"Checklists: {summary.total_checklists}")
    for name, total in summary.patch_totals.items():
        print(f"{name} patch list: {total} species")
    for name, total in summary.region_totals.items():
        print(f"{name} list: {total} species")
    if summary.biggest_day:
        date, count = summary.biggest_day
        print(f"Biggest day: {count} species on {date}")
    print(
        f"Streak: {summary.current_streak} days current \u00b7 "
        f"{summary.longest_streak} longest"
    )
    print(f"Countries: {summary.countries} \u00b7 States/provinces: {summary.states}")
    tiers = summary.events_by_tier
    print(
        f"Unlocked all-time: \U0001f3c6 {tiers.get('gold', 0)} gold \u00b7 "
        f"\u2728 {tiers.get('silver', 0)} silver \u00b7 "
        f"\U0001f331 {tiers.get('bronze', 0)} bronze"
    )

    if args.full:
        print("\n" + "=" * 72)
        print("FULL TIMELINE")
        print("=" * 72)
        for a in achievements:
            _print_event(a)
        return

    since = args.since or (datetime.date.today() - datetime.timedelta(days=30))
    recent = filter_since(achievements, since)
    print("\n" + "=" * 72)
    print(f"\U0001f389  UNLOCKED SINCE {since.isoformat()}  ({len(recent)} events)")
    print("=" * 72)
    if not recent:
        print("  Nothing new \u2014 time to go birding!")
        return

    by_tier: dict[Tier, list[Achievement]] = defaultdict(list)
    for a in recent:
        by_tier[a.tier].append(a)
    for tier in (Tier.GOLD, Tier.SILVER, Tier.BRONZE):
        events = by_tier.get(tier)
        if not events:
            continue
        print(f"\n--- {tier.value.upper()} ---")
        for a in events:
            _print_event(a)


if __name__ == "__main__":
    main()
