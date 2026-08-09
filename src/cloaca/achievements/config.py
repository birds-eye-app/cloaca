"""Patch and region definitions for the achievement engine.

A *patch* is a personally-meaningful place made up of one or more eBird
locations (hotspots or personal locations). A *region* is a broader area
matched by state/province code or (state, county) pairs.

Edit DEFAULT_PATCHES / DEFAULT_REGIONS to taste — the engine takes these as
arguments, so alternate configs can be passed in without touching this file.
"""

from dataclasses import dataclass, field

from cloaca.parsing.parsing_helpers import Observation


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

DEFAULT_REGIONS: tuple[Region, ...] = (
    Region(name="New York", state_codes=frozenset({"US-NY"})),
    Region(
        name="Martha's Vineyard",
        counties=frozenset({("US-MA", "Dukes")}),
    ),
)
