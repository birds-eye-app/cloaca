import datetime
from dataclasses import dataclass

import pytest

from cloaca.piper.year_lifers import (
    PendingProvisional,
    _find_new_species,
    _get_known_all_time_species,
    _get_known_species,
    _get_pending_provisionals,
    _insert_all_time_species,
    _insert_pending_provisional,
    _insert_species,
    _remove_all_time_species,
    _remove_pending_provisional,
    _remove_year_species,
    _split_confirmed_provisional,
    check_for_new_all_time_lifers,
    check_for_new_year_lifers,
    check_pending_provisionals,
    format_all_time_lifer_message,
    format_confirmed_all_time_lifer_message,
    format_confirmed_year_lifer_message,
    format_invalidated_lifer_message,
    format_tentative_all_time_lifer_message,
    format_tentative_year_lifer_message,
    format_year_lifer_message,
    get_all_time_total,
    get_year_total,
)
from cloaca.scripts.fetch_yearly_hotspot_data import eBirdHistoricFullObservation

HOTSPOT_ID = "L1814508"
YEAR = datetime.datetime.now(datetime.timezone(datetime.timedelta(hours=-5))).year


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def make_obs(
    species_code: str = "yetwar",
    com_name: str = "Yellow-throated Warbler",
    sci_name: str = "Setophaga dominica",
    obs_dt: datetime.datetime | None = None,
    checklist_id: str = "S100001",
    sub_id: str = "S100001",
    user_display_name: str = "Jane Doe",
    obs_reviewed: bool = False,
    obs_valid: bool = True,
    exotic_category: str | None = None,
) -> eBirdHistoricFullObservation:
    if obs_dt is None:
        obs_dt = datetime.datetime(2026, 4, 11, 8, 30)
    return eBirdHistoricFullObservation(
        speciesCode=species_code,
        comName=com_name,
        sciName=sci_name,
        locId=HOTSPOT_ID,
        locName="Franz Sigel Park",
        obsDt=obs_dt,
        lat=40.83,
        lng=-73.92,
        obsValid=obs_valid,
        obsReviewed=obs_reviewed,
        locationPrivate=False,
        subId=sub_id,
        subnational2Code="US-NY-005",
        subnational2Name="Bronx",
        subnational1Code="US-NY",
        subnational1Name="New York",
        countryCode="US",
        countryName="United States",
        userDisplayName=user_display_name,
        obsId=f"OBS{species_code}",
        checklistId=checklist_id,
        presenceNoted=False,
        hasComments=False,
        firstName="Jane",
        lastName="Doe",
        hasRichMedia=False,
        exoticCategory=exotic_category,
    )


# ---------------------------------------------------------------------------
# _find_new_species (pure function, no DB)
# ---------------------------------------------------------------------------


class TestFindNewSpecies:
    def test_finds_new_species(self):
        obs = [make_obs("amrob", "American Robin"), make_obs("yetwar")]
        known = {"amrob"}
        result = _find_new_species(obs, known)
        assert len(result) == 1
        assert result[0].speciesCode == "yetwar"

    def test_returns_empty_when_all_known(self):
        obs = [make_obs("amrob"), make_obs("yetwar")]
        known = {"amrob", "yetwar"}
        assert _find_new_species(obs, known) == []

    def test_returns_empty_for_empty_observations(self):
        assert _find_new_species([], set()) == []

    def test_picks_earliest_observation_per_species(self):
        early = make_obs(
            obs_dt=datetime.datetime(2026, 4, 11, 7, 0),
            checklist_id="S_early",
            user_display_name="Early Bird",
        )
        late = make_obs(
            obs_dt=datetime.datetime(2026, 4, 11, 10, 0),
            checklist_id="S_late",
            user_display_name="Late Bird",
        )
        result = _find_new_species([late, early], set())
        assert len(result) == 1
        assert result[0].checklistId == "S_early"

    def test_handles_multiple_new_species(self):
        obs = [
            make_obs("amrob", "American Robin"),
            make_obs("yetwar", "Yellow-throated Warbler"),
            make_obs("bkcchi", "Black-capped Chickadee"),
        ]
        result = _find_new_species(obs, set())
        codes = {o.speciesCode for o in result}
        assert codes == {"amrob", "yetwar", "bkcchi"}


# ---------------------------------------------------------------------------
# Notable observation helper (mimics phoebe Observation for split logic)
# ---------------------------------------------------------------------------


@dataclass
class NotableObs:
    """Minimal mock of a phoebe Observation from the notable endpoint."""

    species_code: str
    obs_valid: bool
    obs_reviewed: bool = False


def make_notable(species_code: str = "yetwar", obs_valid: bool = True) -> NotableObs:
    return NotableObs(
        species_code=species_code, obs_valid=obs_valid, obs_reviewed=obs_valid
    )


# ---------------------------------------------------------------------------
# _split_confirmed_provisional (pure function, no DB)
# ---------------------------------------------------------------------------


class TestSplitConfirmedProvisional:
    def test_common_species_not_in_notable_is_confirmed(self):
        """Common species never appear in notable — automatically confirmed."""
        obs = [make_obs("amerob", "American Robin")]
        confirmed, provisional = _split_confirmed_provisional(obs, [])
        assert len(confirmed) == 1
        assert len(provisional) == 0

    def test_rarity_with_confirmed_report(self):
        """Species in notable with obs_valid=True is confirmed."""
        obs = [make_obs()]
        notable = [make_notable("yetwar", obs_valid=True)]
        confirmed, provisional = _split_confirmed_provisional(obs, notable)
        assert len(confirmed) == 1
        assert len(provisional) == 0

    def test_rarity_with_only_unconfirmed_reports(self):
        """Species in notable with only obs_valid=False is provisional."""
        obs = [make_obs()]
        notable = [make_notable("yetwar", obs_valid=False)]
        confirmed, provisional = _split_confirmed_provisional(obs, notable)
        assert len(confirmed) == 0
        assert len(provisional) == 1

    def test_mixed_confirmed_and_provisional(self):
        common = make_obs("amerob", "American Robin")
        rarity = make_obs("yetwar")
        new_lifers = [common, rarity]
        notable = [make_notable("yetwar", obs_valid=False)]
        confirmed, provisional = _split_confirmed_provisional(new_lifers, notable)
        assert [o.speciesCode for o in confirmed] == ["amerob"]
        assert [o.speciesCode for o in provisional] == ["yetwar"]

    def test_confirmed_if_any_notable_report_is_valid(self):
        """Multiple notable reports — one confirmed is enough."""
        obs = [make_obs()]
        notable = [
            make_notable("yetwar", obs_valid=False),
            make_notable("yetwar", obs_valid=True),
        ]
        confirmed, provisional = _split_confirmed_provisional(obs, notable)
        assert len(confirmed) == 1
        assert len(provisional) == 0


# ---------------------------------------------------------------------------
# DB: year species helpers
# ---------------------------------------------------------------------------


class TestYearSpeciesDB:
    @pytest.mark.asyncio
    async def test_insert_and_get_known(self):
        obs = make_obs()
        await _insert_species(HOTSPOT_ID, YEAR, obs)
        known = await _get_known_species(HOTSPOT_ID, YEAR)
        assert "yetwar" in known

    @pytest.mark.asyncio
    async def test_get_year_total(self):
        assert await get_year_total(HOTSPOT_ID) == 0
        await _insert_species(HOTSPOT_ID, YEAR, make_obs("amrob", "American Robin"))
        await _insert_species(HOTSPOT_ID, YEAR, make_obs("yetwar"))
        assert await get_year_total(HOTSPOT_ID) == 2

    @pytest.mark.asyncio
    async def test_remove_year_species(self):
        await _insert_species(HOTSPOT_ID, YEAR, make_obs())
        assert await get_year_total(HOTSPOT_ID) == 1
        await _remove_year_species(HOTSPOT_ID, YEAR, "yetwar")
        assert await get_year_total(HOTSPOT_ID) == 0

    @pytest.mark.asyncio
    async def test_insert_duplicate_is_noop(self):
        obs = make_obs()
        await _insert_species(HOTSPOT_ID, YEAR, obs)
        await _insert_species(HOTSPOT_ID, YEAR, obs)
        assert await get_year_total(HOTSPOT_ID) == 1


# ---------------------------------------------------------------------------
# DB: all-time species helpers
# ---------------------------------------------------------------------------


class TestAllTimeSpeciesDB:
    @pytest.mark.asyncio
    async def test_insert_and_get_known(self):
        await _insert_all_time_species(HOTSPOT_ID, "yetwar")
        known = await _get_known_all_time_species(HOTSPOT_ID)
        assert "yetwar" in known

    @pytest.mark.asyncio
    async def test_get_all_time_total(self):
        assert await get_all_time_total(HOTSPOT_ID) == 0
        await _insert_all_time_species(HOTSPOT_ID, "amrob")
        await _insert_all_time_species(HOTSPOT_ID, "yetwar")
        assert await get_all_time_total(HOTSPOT_ID) == 2

    @pytest.mark.asyncio
    async def test_remove_all_time_species(self):
        await _insert_all_time_species(HOTSPOT_ID, "yetwar")
        assert await get_all_time_total(HOTSPOT_ID) == 1
        await _remove_all_time_species(HOTSPOT_ID, "yetwar")
        assert await get_all_time_total(HOTSPOT_ID) == 0


# ---------------------------------------------------------------------------
# DB: pending provisional helpers
# ---------------------------------------------------------------------------


class TestPendingProvisionalDB:
    @pytest.mark.asyncio
    async def test_insert_and_get(self):
        obs = make_obs()
        await _insert_pending_provisional(HOTSPOT_ID, obs, "year", YEAR)
        pending = await _get_pending_provisionals(HOTSPOT_ID)
        assert len(pending) == 1
        p = pending[0]
        assert p.species_code == "yetwar"
        assert p.common_name == "Yellow-throated Warbler"
        assert p.lifer_type == "year"
        assert p.year == YEAR
        assert isinstance(p.obs_date, datetime.date)

    @pytest.mark.asyncio
    async def test_remove(self):
        obs = make_obs()
        await _insert_pending_provisional(HOTSPOT_ID, obs, "year", YEAR)
        await _remove_pending_provisional(HOTSPOT_ID, "yetwar", "year")
        assert await _get_pending_provisionals(HOTSPOT_ID) == []

    @pytest.mark.asyncio
    async def test_empty_when_no_records(self):
        assert await _get_pending_provisionals(HOTSPOT_ID) == []

    @pytest.mark.asyncio
    async def test_duplicate_insert_is_noop(self):
        obs = make_obs()
        await _insert_pending_provisional(HOTSPOT_ID, obs, "year", YEAR)
        await _insert_pending_provisional(HOTSPOT_ID, obs, "year", YEAR)
        assert len(await _get_pending_provisionals(HOTSPOT_ID)) == 1

    @pytest.mark.asyncio
    async def test_same_species_different_lifer_types(self):
        obs = make_obs()
        await _insert_pending_provisional(HOTSPOT_ID, obs, "year", YEAR)
        await _insert_pending_provisional(HOTSPOT_ID, obs, "all_time")
        pending = await _get_pending_provisionals(HOTSPOT_ID)
        assert len(pending) == 2
        types = {p.lifer_type for p in pending}
        assert types == {"year", "all_time"}


# ---------------------------------------------------------------------------
# check_for_new_year_lifers
# ---------------------------------------------------------------------------


class TestCheckForNewYearLifers:
    @pytest.mark.asyncio
    async def test_confirmed_lifer(self):
        obs = [make_obs(obs_reviewed=True)]
        confirmed, provisional = await check_for_new_year_lifers(HOTSPOT_ID, obs, [])
        assert len(confirmed) == 1
        assert len(provisional) == 0
        # Should be inserted into known species
        assert "yetwar" in await _get_known_species(HOTSPOT_ID, YEAR)

    @pytest.mark.asyncio
    async def test_provisional_lifer(self):
        obs = [make_obs()]
        notable = [make_notable("yetwar", obs_valid=False)]
        confirmed, provisional = await check_for_new_year_lifers(
            HOTSPOT_ID, obs, notable
        )
        assert len(confirmed) == 0
        assert len(provisional) == 1
        # Should still be inserted into known species
        assert "yetwar" in await _get_known_species(HOTSPOT_ID, YEAR)
        # Should create a pending provisional
        pending = await _get_pending_provisionals(HOTSPOT_ID)
        assert len(pending) == 1
        assert pending[0].lifer_type == "year"

    @pytest.mark.asyncio
    async def test_empty_observations(self):
        confirmed, provisional = await check_for_new_year_lifers(HOTSPOT_ID, [], [])
        assert confirmed == []
        assert provisional == []

    @pytest.mark.asyncio
    async def test_already_known_species_not_detected(self):
        await _insert_species(HOTSPOT_ID, YEAR, make_obs())
        obs = [make_obs(obs_reviewed=True)]
        confirmed, provisional = await check_for_new_year_lifers(HOTSPOT_ID, obs, [])
        assert confirmed == []
        assert provisional == []

    @pytest.mark.asyncio
    async def test_mixed_confirmed_and_provisional(self):
        obs = [
            make_obs("amrob", "American Robin"),
            make_obs("yetwar", "Yellow-throated Warbler"),
        ]
        notable = [make_notable("yetwar", obs_valid=False)]
        confirmed, provisional = await check_for_new_year_lifers(
            HOTSPOT_ID, obs, notable
        )
        assert len(confirmed) == 1
        assert confirmed[0].speciesCode == "amrob"
        assert len(provisional) == 1
        assert provisional[0].speciesCode == "yetwar"


# ---------------------------------------------------------------------------
# check_for_new_all_time_lifers
# ---------------------------------------------------------------------------


class TestCheckForNewAllTimeLifers:
    @pytest.mark.asyncio
    async def test_confirmed_lifer(self):
        obs = [make_obs(obs_reviewed=True)]
        confirmed, provisional = await check_for_new_all_time_lifers(
            HOTSPOT_ID, obs, []
        )
        assert len(confirmed) == 1
        assert "yetwar" in await _get_known_all_time_species(HOTSPOT_ID)

    @pytest.mark.asyncio
    async def test_provisional_lifer(self):
        obs = [make_obs()]
        notable = [make_notable("yetwar", obs_valid=False)]
        confirmed, provisional = await check_for_new_all_time_lifers(
            HOTSPOT_ID, obs, notable
        )
        assert len(provisional) == 1
        assert "yetwar" in await _get_known_all_time_species(HOTSPOT_ID)
        pending = await _get_pending_provisionals(HOTSPOT_ID)
        assert len(pending) == 1
        assert pending[0].lifer_type == "all_time"

    @pytest.mark.asyncio
    async def test_already_known_species_not_detected(self):
        await _insert_all_time_species(HOTSPOT_ID, "yetwar")
        obs = [make_obs(obs_reviewed=True)]
        confirmed, provisional = await check_for_new_all_time_lifers(
            HOTSPOT_ID, obs, []
        )
        assert confirmed == []
        assert provisional == []


# ---------------------------------------------------------------------------
# check_pending_provisionals
# ---------------------------------------------------------------------------


class TestCheckPendingProvisionals:
    @pytest.mark.asyncio
    async def test_no_pending_returns_empty(self):
        confirmed, invalidated = await check_pending_provisionals(HOTSPOT_ID, [])
        assert confirmed == []
        assert invalidated == []

    @pytest.mark.asyncio
    async def test_confirmed_when_notable_shows_valid(self):
        """Pending provisional is confirmed when the notable endpoint shows
        obs_valid=True for the species (reviewer approved it)."""
        obs = make_obs()
        await _insert_pending_provisional(HOTSPOT_ID, obs, "year", YEAR)
        await _insert_species(HOTSPOT_ID, YEAR, obs)

        notable = [make_notable("yetwar", obs_valid=True)]
        confirmed, invalidated = await check_pending_provisionals(HOTSPOT_ID, notable)
        assert len(confirmed) == 1
        assert confirmed[0].species_code == "yetwar"
        assert invalidated == []
        # Pending record should be cleaned up
        assert await _get_pending_provisionals(HOTSPOT_ID) == []

    @pytest.mark.asyncio
    async def test_still_pending_when_unconfirmed(self):
        """Species still in notable but only with obs_valid=False — no change."""
        obs = make_obs()
        await _insert_pending_provisional(HOTSPOT_ID, obs, "year", YEAR)
        await _insert_species(HOTSPOT_ID, YEAR, obs)

        notable = [make_notable("yetwar", obs_valid=False)]
        confirmed, invalidated = await check_pending_provisionals(HOTSPOT_ID, notable)
        assert confirmed == []
        assert invalidated == []
        # Still in pending table
        assert len(await _get_pending_provisionals(HOTSPOT_ID)) == 1

    @pytest.mark.asyncio
    async def test_invalidated_after_stale_period(self):
        """Species vanishes from notable for >14 days → invalidated."""
        old_date = datetime.date.today() - datetime.timedelta(days=20)
        obs = make_obs(
            obs_dt=datetime.datetime(old_date.year, old_date.month, old_date.day, 8, 0)
        )
        await _insert_pending_provisional(HOTSPOT_ID, obs, "year", YEAR)
        await _insert_species(HOTSPOT_ID, YEAR, obs)

        # Empty notable = species gone
        confirmed, invalidated = await check_pending_provisionals(HOTSPOT_ID, [])

        assert confirmed == []
        assert len(invalidated) == 1
        assert invalidated[0].species_code == "yetwar"
        # Cleaned up from pending
        assert await _get_pending_provisionals(HOTSPOT_ID) == []
        # Also removed from known year species
        assert "yetwar" not in await _get_known_species(HOTSPOT_ID, YEAR)

    @pytest.mark.asyncio
    async def test_invalidated_all_time_removes_from_known(self):
        old_date = datetime.date.today() - datetime.timedelta(days=20)
        obs = make_obs(
            obs_dt=datetime.datetime(old_date.year, old_date.month, old_date.day, 8, 0)
        )
        await _insert_pending_provisional(HOTSPOT_ID, obs, "all_time")
        await _insert_all_time_species(HOTSPOT_ID, "yetwar")

        confirmed, invalidated = await check_pending_provisionals(HOTSPOT_ID, [])

        assert len(invalidated) == 1
        assert "yetwar" not in await _get_known_all_time_species(HOTSPOT_ID)

    @pytest.mark.asyncio
    async def test_not_invalidated_before_stale_period(self):
        """Species gone from notable but only 5 days old — don't invalidate yet."""
        recent_date = datetime.date.today() - datetime.timedelta(days=5)
        obs = make_obs(
            obs_dt=datetime.datetime(
                recent_date.year, recent_date.month, recent_date.day, 8, 0
            )
        )
        await _insert_pending_provisional(HOTSPOT_ID, obs, "year", YEAR)
        await _insert_species(HOTSPOT_ID, YEAR, obs)

        confirmed, invalidated = await check_pending_provisionals(HOTSPOT_ID, [])

        assert confirmed == []
        assert invalidated == []
        # Still pending
        assert len(await _get_pending_provisionals(HOTSPOT_ID)) == 1


# ---------------------------------------------------------------------------
# Integration tests: realistic end-to-end flows
#
# Based on real observations at Franz Sigel Park (L1814508), Apr 11 2026.
# User names anonymized.
# ---------------------------------------------------------------------------


class TestIntegrationCommonSpeciesLifer:
    """A common species (American Robin) is detected as a new year lifer.
    It's never flagged for review so it should be immediately confirmed."""

    @pytest.mark.asyncio
    async def test_common_species_is_immediately_confirmed(self):
        # Historic endpoint returns American Robin (obsValid=True, never flagged)
        observations = [
            make_obs(
                "amerob",
                "American Robin",
                obs_reviewed=False,
                obs_valid=True,
                obs_dt=datetime.datetime(2026, 4, 11, 7, 52),
                checklist_id="CL001",
                sub_id="S001",
            ),
        ]
        # Notable endpoint returns nothing for common species
        notable = []

        confirmed, provisional = await check_for_new_year_lifers(
            HOTSPOT_ID, observations, notable
        )

        assert len(confirmed) == 1
        assert confirmed[0].speciesCode == "amerob"
        assert len(provisional) == 0
        # No pending provisional created
        assert await _get_pending_provisionals(HOTSPOT_ID) == []


class TestIntegrationRarityUnconfirmedThenConfirmed:
    """A rare species (Yellow-throated Warbler) appears with only unconfirmed
    reports, then gets confirmed by a reviewer on a subsequent poll."""

    @pytest.mark.asyncio
    async def test_rarity_detected_as_provisional_then_confirmed(self):
        # --- Poll 1: species detected, only unconfirmed reports ---
        observations = [
            make_obs(
                "yetwar",
                "Yellow-throated Warbler",
                obs_dt=datetime.datetime(2026, 4, 11, 8, 47),
                checklist_id="CL002",
                sub_id="S002",
            ),
        ]
        # Notable shows only unconfirmed reports
        notable_poll1 = [
            make_notable("yetwar", obs_valid=False),
        ]

        confirmed, provisional = await check_for_new_all_time_lifers(
            HOTSPOT_ID, observations, notable_poll1
        )
        assert len(confirmed) == 0
        assert len(provisional) == 1
        assert provisional[0].speciesCode == "yetwar"

        # Pending provisional should be created
        pending = await _get_pending_provisionals(HOTSPOT_ID)
        assert len(pending) == 1
        assert pending[0].lifer_type == "all_time"

        # --- Poll 2: reviewer approved it ---
        notable_poll2 = [
            make_notable("yetwar", obs_valid=False),  # unconfirmed report still there
            make_notable("yetwar", obs_valid=True),  # but now there's a confirmed one
        ]

        pend_confirmed, pend_invalidated = await check_pending_provisionals(
            HOTSPOT_ID, notable_poll2
        )
        assert len(pend_confirmed) == 1
        assert pend_confirmed[0].species_code == "yetwar"
        assert pend_invalidated == []

        # Pending should be cleaned up
        assert await _get_pending_provisionals(HOTSPOT_ID) == []


class TestIntegrationMixedCommonAndRarity:
    """A poll returns both common species (confirmed) and a rarity
    (provisional). The common species gets the full celebration, the
    rarity gets the tentative message."""

    @pytest.mark.asyncio
    async def test_mixed_poll(self):
        observations = [
            make_obs(
                "amerob",
                "American Robin",
                obs_dt=datetime.datetime(2026, 4, 11, 7, 52),
                checklist_id="CL001",
                sub_id="S001",
            ),
            make_obs(
                "houwre",
                "Northern House Wren",
                obs_dt=datetime.datetime(2026, 4, 12, 9, 22),
                checklist_id="CL003",
                sub_id="S003",
            ),
        ]
        # Only the wren is in notable (it's unusual for this location)
        notable = [
            make_notable("houwre", obs_valid=False),
        ]

        confirmed, provisional = await check_for_new_year_lifers(
            HOTSPOT_ID, observations, notable
        )

        assert len(confirmed) == 1
        assert confirmed[0].speciesCode == "amerob"
        assert len(provisional) == 1
        assert provisional[0].speciesCode == "houwre"


class TestIntegrationRarityInvalidated:
    """A rare species appears, is never confirmed, and eventually disappears
    from the notable endpoint after the stale period."""

    @pytest.mark.asyncio
    async def test_rarity_invalidated_after_stale_period(self):
        old_date = datetime.date.today() - datetime.timedelta(days=20)
        observations = [
            make_obs(
                "yetwar",
                "Yellow-throated Warbler",
                obs_dt=datetime.datetime(
                    old_date.year, old_date.month, old_date.day, 8, 47
                ),
                checklist_id="CL002",
                sub_id="S002",
            ),
        ]
        notable_poll1 = [make_notable("yetwar", obs_valid=False)]

        await check_for_new_all_time_lifers(HOTSPOT_ID, observations, notable_poll1)

        # Later poll: species vanished from notable (reviewer rejected it)
        pend_confirmed, pend_invalidated = await check_pending_provisionals(
            HOTSPOT_ID, []
        )

        assert pend_confirmed == []
        assert len(pend_invalidated) == 1
        assert pend_invalidated[0].species_code == "yetwar"

        # Should be removed from known species
        assert "yetwar" not in await _get_known_all_time_species(HOTSPOT_ID)


# ---------------------------------------------------------------------------
# Formatting: year lifer messages
# ---------------------------------------------------------------------------


class TestFormatYearLiferMessage:
    def test_single(self):
        obs = make_obs(sub_id="S999")
        msg = format_year_lifer_message([obs], "Franz Sigel Park", 42)
        assert "Year Bird #42" in msg
        assert "Franz Sigel Park" in msg
        assert "Yellow-throated Warbler" in msg
        assert "Jane Doe" in msg
        assert "S999" in msg

    def test_multiple(self):
        obs1 = make_obs("amrob", "American Robin")
        obs2 = make_obs("yetwar", "Yellow-throated Warbler")
        msg = format_year_lifer_message([obs1, obs2], "Franz Sigel Park", 42)
        assert "2 New Year Birds" in msg
        assert "42 species" in msg
        assert "American Robin" in msg
        assert "Yellow-throated Warbler" in msg


class TestFormatAllTimeLiferMessage:
    def test_single(self):
        obs = make_obs()
        msg = format_all_time_lifer_message([obs], "Franz Sigel Park", 100)
        assert "New Park Bird" in msg
        assert "#100 all-time" in msg
        assert "Yellow-throated Warbler" in msg

    def test_multiple(self):
        obs1 = make_obs("amrob", "American Robin")
        obs2 = make_obs("yetwar", "Yellow-throated Warbler")
        msg = format_all_time_lifer_message([obs1, obs2], "Franz Sigel Park", 100)
        assert "2 New Park Birds" in msg
        assert "100 species all-time" in msg


# ---------------------------------------------------------------------------
# Formatting: tentative messages
# ---------------------------------------------------------------------------


class TestFormatTentativeMessages:
    def test_tentative_year_single(self):
        obs = make_obs()
        msg = format_tentative_year_lifer_message([obs], "Franz Sigel Park")
        assert "Possible Year Bird" in msg
        assert "Yellow-throated Warbler" in msg
        assert "Awaiting eBird review" in msg

    def test_tentative_year_multiple(self):
        obs1 = make_obs("amrob", "American Robin")
        obs2 = make_obs("yetwar", "Yellow-throated Warbler")
        msg = format_tentative_year_lifer_message([obs1, obs2], "Franz Sigel Park")
        assert "2 Possible New Year Birds" in msg
        assert "Awaiting eBird review" in msg

    def test_tentative_all_time_single(self):
        obs = make_obs()
        msg = format_tentative_all_time_lifer_message([obs], "Franz Sigel Park")
        assert "Possible New Park Bird" in msg
        assert "Yellow-throated Warbler" in msg
        assert "Awaiting eBird review" in msg

    def test_tentative_all_time_multiple(self):
        obs1 = make_obs("amrob", "American Robin")
        obs2 = make_obs("yetwar", "Yellow-throated Warbler")
        msg = format_tentative_all_time_lifer_message([obs1, obs2], "Franz Sigel Park")
        assert "2 Possible New Park Birds" in msg


# ---------------------------------------------------------------------------
# Formatting: confirmed messages
# ---------------------------------------------------------------------------


class TestFormatConfirmedMessages:
    def _pending(self, **kwargs):
        defaults = dict(
            hotspot_id=HOTSPOT_ID,
            species_code="yetwar",
            common_name="Yellow-throated Warbler",
            scientific_name="Setophaga dominica",
            obs_date=datetime.date(2026, 4, 11),
            observer_name="Jane Doe",
            sub_id="S999",
            lifer_type="year",
            year=YEAR,
        )
        defaults.update(kwargs)
        return PendingProvisional(**defaults)

    def test_confirmed_year_single(self):
        p = self._pending()
        msg = format_confirmed_year_lifer_message([p], "Franz Sigel Park", 42)
        assert "Confirmed!" in msg
        assert "Year Bird #42" in msg
        assert "Yellow-throated Warbler" in msg
        assert "reviewed and confirmed" in msg

    def test_confirmed_year_multiple(self):
        p1 = self._pending(species_code="amrob", common_name="American Robin")
        p2 = self._pending()
        msg = format_confirmed_year_lifer_message([p1, p2], "Franz Sigel Park", 42)
        assert "2 Year Birds Confirmed" in msg
        assert "42 species" in msg

    def test_confirmed_all_time_single(self):
        p = self._pending(lifer_type="all_time")
        msg = format_confirmed_all_time_lifer_message([p], "Franz Sigel Park", 100)
        assert "Confirmed!" in msg
        assert "New Park Bird" in msg
        assert "#100 all-time" in msg

    def test_confirmed_all_time_multiple(self):
        p1 = self._pending(
            species_code="amrob",
            common_name="American Robin",
            lifer_type="all_time",
        )
        p2 = self._pending(lifer_type="all_time")
        msg = format_confirmed_all_time_lifer_message([p1, p2], "Franz Sigel Park", 100)
        assert "2 New Park Birds Confirmed" in msg


# ---------------------------------------------------------------------------
# Formatting: invalidated message
# ---------------------------------------------------------------------------


class TestFormatInvalidatedMessage:
    def test_single(self):
        p = PendingProvisional(
            hotspot_id=HOTSPOT_ID,
            species_code="yetwar",
            common_name="Yellow-throated Warbler",
            scientific_name="Setophaga dominica",
            obs_date=datetime.date(2026, 4, 11),
            observer_name="Jane Doe",
            sub_id="S999",
            lifer_type="year",
            year=YEAR,
        )
        msg = format_invalidated_lifer_message([p], "Franz Sigel Park")
        assert "Yellow-throated Warbler" in msg
        assert "not confirmed" in msg
        assert "was not" in msg

    def test_multiple(self):
        p1 = PendingProvisional(
            hotspot_id=HOTSPOT_ID,
            species_code="amrob",
            common_name="American Robin",
            scientific_name="Turdus migratorius",
            obs_date=datetime.date(2026, 4, 11),
            observer_name="Jane Doe",
            sub_id="S998",
            lifer_type="year",
            year=YEAR,
        )
        p2 = PendingProvisional(
            hotspot_id=HOTSPOT_ID,
            species_code="yetwar",
            common_name="Yellow-throated Warbler",
            scientific_name="Setophaga dominica",
            obs_date=datetime.date(2026, 4, 11),
            observer_name="Jane Doe",
            sub_id="S999",
            lifer_type="year",
            year=YEAR,
        )
        msg = format_invalidated_lifer_message([p1, p2], "Franz Sigel Park")
        assert "were not confirmed" in msg
