import datetime
from dataclasses import dataclass
from unittest.mock import AsyncMock, patch

import pytest

from cloaca.piper.rare_bird_alert import (
    ABA_CODE_LABELS,
    RareBirdSighting,
    _filter_aba_rarities,
    _load_aba_codes,
    _record_alert,
    _was_recently_alerted,
    check_for_rare_birds,
    format_rare_bird_message,
    get_aba_code,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


@dataclass
class FakeObservation:
    species_code: str
    com_name: str
    sci_name: str
    obs_dt: str
    user_display_name: str
    sub_id: str
    loc_name: str
    obs_valid: bool | None = True
    obs_reviewed: bool | None = False


def make_notable(
    species_code: str = "tufduc",
    com_name: str = "Tufted Duck",
    sci_name: str = "Aythya fuligula",
    obs_dt: str = "2026-04-14 08:30",
    user_display_name: str = "Jane Doe",
    sub_id: str = "S200001",
    loc_name: str = "Central Park",
    obs_valid: bool | None = True,
) -> FakeObservation:
    return FakeObservation(
        species_code=species_code,
        com_name=com_name,
        sci_name=sci_name,
        obs_dt=obs_dt,
        user_display_name=user_display_name,
        sub_id=sub_id,
        loc_name=loc_name,
        obs_valid=obs_valid,
    )


def make_sighting(
    species_code: str = "tufduc",
    common_name: str = "Tufted Duck",
    aba_code: int = 3,
    obs_date: datetime.date | None = None,
    county_name: str = "Kings (Brooklyn)",
    obs_valid: bool = True,
) -> RareBirdSighting:
    if obs_date is None:
        obs_date = datetime.date(2026, 4, 14)
    return RareBirdSighting(
        species_code=species_code,
        common_name=common_name,
        scientific_name="Aythya fuligula",
        aba_code=aba_code,
        obs_date=obs_date,
        observer_name="Jane Doe",
        sub_id="S200001",
        location_name="Prospect Park",
        region_code="US-NY-047",
        county_name=county_name,
        obs_valid=obs_valid,
    )


# ---------------------------------------------------------------------------
# ABA code lookup
# ---------------------------------------------------------------------------


class TestABACodes:
    def test_load_aba_codes(self):
        codes = _load_aba_codes()
        assert len(codes) > 0
        # All loaded codes should be >= 3
        for species in codes.values():
            assert species.aba_code >= 3

    def test_get_aba_code_known_species(self):
        result = get_aba_code("tufduc")
        assert result is not None
        assert result.aba_code == 3
        assert result.common_name == "Tufted Duck"

    def test_get_aba_code_unknown_returns_none(self):
        result = get_aba_code("amrob")  # American Robin — Code 1
        assert result is None

    def test_get_aba_code_code5(self):
        result = get_aba_code("corcra")  # Corn Crake — Code 5
        assert result is not None
        assert result.aba_code == 5

    def test_extinct_species_loaded(self):
        result = get_aba_code("paspig")  # Passenger Pigeon — Code 6
        assert result is not None
        assert result.aba_code == 6


# ---------------------------------------------------------------------------
# Filtering
# ---------------------------------------------------------------------------


class TestFilterABARarities:
    def test_filters_aba_code_3_plus(self):
        observations = [
            make_notable("tufduc", "Tufted Duck"),  # ABA 3
            make_notable("amrob", "American Robin"),  # ABA 1 (not in our table)
        ]
        result = _filter_aba_rarities(observations, "US-NY-047", "Kings (Brooklyn)")
        assert len(result) == 1
        assert result[0].species_code == "tufduc"

    def test_picks_earliest_observation(self):
        observations = [
            make_notable("tufduc", obs_dt="2026-04-14 08:30", sub_id="S1"),
            make_notable("tufduc", obs_dt="2026-04-12 10:00", sub_id="S2"),
        ]
        result = _filter_aba_rarities(observations, "US-NY-047", "Kings (Brooklyn)")
        assert len(result) == 1
        assert result[0].obs_date == datetime.date(2026, 4, 12)

    def test_empty_observations(self):
        result = _filter_aba_rarities([], "US-NY-047", "Kings (Brooklyn)")
        assert result == []

    def test_multiple_species(self):
        observations = [
            make_notable("tufduc", "Tufted Duck"),  # ABA 3
            make_notable("gargan", "Garganey"),  # ABA 4
            make_notable("corcra", "Corn Crake"),  # ABA 5
        ]
        result = _filter_aba_rarities(observations, "US-NY-047", "Kings (Brooklyn)")
        codes = {s.species_code for s in result}
        assert codes == {"tufduc", "gargan", "corcra"}


# ---------------------------------------------------------------------------
# Dedup (DB tests)
# ---------------------------------------------------------------------------


class TestDedup:
    @pytest.mark.asyncio
    async def test_not_recently_alerted(self):
        result = await _was_recently_alerted("tufduc", "US-NY-047")
        assert result is False

    @pytest.mark.asyncio
    async def test_recently_alerted(self):
        sighting = make_sighting()
        await _record_alert(sighting)
        result = await _was_recently_alerted("tufduc", "US-NY-047")
        assert result is True

    @pytest.mark.asyncio
    async def test_different_region_not_deduped(self):
        sighting = make_sighting()
        await _record_alert(sighting)
        result = await _was_recently_alerted("tufduc", "US-NY-061")
        assert result is False


# ---------------------------------------------------------------------------
# Discord message formatting
# ---------------------------------------------------------------------------


class TestFormatMessage:
    def test_single_sighting(self):
        s = make_sighting()
        message = format_rare_bird_message([s])
        assert "Tufted Duck" in message
        assert "ABA Rare" in message
        assert "Prospect Park" in message
        assert "checklist" in message.lower()

    def test_single_unreviewed(self):
        s = make_sighting(obs_valid=False)
        message = format_rare_bird_message([s])
        assert "Awaiting eBird review" in message

    def test_multiple_sightings(self):
        sightings = [
            make_sighting("tufduc", "Tufted Duck", aba_code=3),
            make_sighting(
                "gargan", "Garganey", aba_code=4, county_name="New York (Manhattan)"
            ),
        ]
        message = format_rare_bird_message(sightings)
        assert "2 species" in message
        assert "Tufted Duck" in message
        assert "Garganey" in message

    def test_code_5_emoji(self):
        s = make_sighting(aba_code=5)
        message = format_rare_bird_message([s])
        assert "\U0001f6a8" in message  # 🚨

    def test_code_4_emoji(self):
        s = make_sighting(aba_code=4)
        message = format_rare_bird_message([s])
        assert "\U00002757" in message  # ❗

    def test_code_3_emoji(self):
        s = make_sighting(aba_code=3)
        message = format_rare_bird_message([s])
        assert "\U0001f514" in message  # 🔔


# ---------------------------------------------------------------------------
# Full pipeline (with mocks)
# ---------------------------------------------------------------------------


class TestCheckForRareBirds:
    @pytest.mark.asyncio
    async def test_finds_rare_birds(self):
        notable = make_notable("tufduc", "Tufted Duck")
        with patch(
            "cloaca.piper.rare_bird_alert.fetch_all_nyc_notables",
            new_callable=AsyncMock,
            return_value=[("US-NY-047", "Kings (Brooklyn)", [notable])],
        ):
            sightings = await check_for_rare_birds()
            assert len(sightings) == 1
            assert sightings[0].species_code == "tufduc"

    @pytest.mark.asyncio
    async def test_skips_common_species(self):
        notable = make_notable("amrob", "American Robin")
        with patch(
            "cloaca.piper.rare_bird_alert.fetch_all_nyc_notables",
            new_callable=AsyncMock,
            return_value=[("US-NY-047", "Kings (Brooklyn)", [notable])],
        ):
            sightings = await check_for_rare_birds()
            assert len(sightings) == 0

    @pytest.mark.asyncio
    async def test_deduplicates_across_polls(self):
        notable = make_notable("tufduc", "Tufted Duck")
        with patch(
            "cloaca.piper.rare_bird_alert.fetch_all_nyc_notables",
            new_callable=AsyncMock,
            return_value=[("US-NY-047", "Kings (Brooklyn)", [notable])],
        ):
            sightings1 = await check_for_rare_birds()
            assert len(sightings1) == 1

            # Second poll should return nothing (already alerted)
            sightings2 = await check_for_rare_birds()
            assert len(sightings2) == 0

    @pytest.mark.asyncio
    async def test_no_notable_returns_empty(self):
        with patch(
            "cloaca.piper.rare_bird_alert.fetch_all_nyc_notables",
            new_callable=AsyncMock,
            return_value=[("US-NY-047", "Kings (Brooklyn)", [])],
        ):
            sightings = await check_for_rare_birds()
            assert len(sightings) == 0
