"""Tests for the check_year_lifers orchestration in piper/main.py.

All year_lifers functions are mocked so we test only the wiring:
which messages get sent to which channels, and how confirmed vs.
provisional vs. pending results are handled.
"""

import datetime
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from cloaca.piper.year_lifers import PendingProvisional
from cloaca.scripts.fetch_yearly_hotspot_data import eBirdHistoricFullObservation


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

HOTSPOT_ID = "L1814508"
CHANNEL_ID = 1492237397700776128


def make_obs(
    species_code: str = "yetwar",
    com_name: str = "Yellow-throated Warbler",
    obs_reviewed: bool = False,
    checklist_id: str = "S100001",
) -> eBirdHistoricFullObservation:
    return eBirdHistoricFullObservation(
        speciesCode=species_code,
        comName=com_name,
        sciName="Setophaga dominica",
        locId=HOTSPOT_ID,
        locName="Franz Sigel Park",
        obsDt=datetime.datetime(2026, 4, 11, 8, 30),
        lat=40.83,
        lng=-73.92,
        obsValid=True,
        obsReviewed=obs_reviewed,
        locationPrivate=False,
        subId="S100001",
        subnational2Code="US-NY-005",
        subnational2Name="Bronx",
        subnational1Code="US-NY",
        subnational1Name="New York",
        countryCode="US",
        countryName="United States",
        userDisplayName="Jane Doe",
        obsId="OBS1",
        checklistId=checklist_id,
        presenceNoted=False,
        hasComments=False,
        firstName="Jane",
        lastName="Doe",
        hasRichMedia=False,
    )


def make_pending(
    species_code: str = "yetwar",
    common_name: str = "Yellow-throated Warbler",
    lifer_type: str = "year",
) -> PendingProvisional:
    return PendingProvisional(
        hotspot_id=HOTSPOT_ID,
        species_code=species_code,
        common_name=common_name,
        scientific_name="Setophaga dominica",
        obs_date=datetime.date(2026, 4, 11),
        observer_name="Jane Doe",
        sub_id="S100001",
        lifer_type=lifer_type,
        year=2026 if lifer_type == "year" else None,
    )


# ---------------------------------------------------------------------------
# Fixture: patch all external dependencies on cloaca.piper.main
# ---------------------------------------------------------------------------

MODULE = "cloaca.piper.main"


@pytest.fixture
def mock_channel():
    ch = AsyncMock()
    ch.send = AsyncMock()
    return ch


@pytest.fixture
def patches(mock_channel):
    """Patch everything the check_year_lifers loop touches and return the
    mocks as a dict for easy assertion."""
    mocks = {}

    p = patch.multiple(
        MODULE,
        fetch_recent_observations=AsyncMock(return_value=[]),
        check_for_new_all_time_lifers=MagicMock(return_value=([], [])),
        check_for_new_year_lifers=MagicMock(return_value=([], [])),
        check_pending_provisionals=AsyncMock(return_value=([], [])),
        get_all_time_total=MagicMock(return_value=100),
        get_year_total=MagicMock(return_value=42),
        format_all_time_lifer_message=MagicMock(return_value="AT_MSG"),
        format_year_lifer_message=MagicMock(return_value="YR_MSG"),
        format_tentative_all_time_lifer_message=MagicMock(return_value="TENT_AT_MSG"),
        format_tentative_year_lifer_message=MagicMock(return_value="TENT_YR_MSG"),
        format_confirmed_all_time_lifer_message=MagicMock(return_value="CONF_AT_MSG"),
        format_confirmed_year_lifer_message=MagicMock(return_value="CONF_YR_MSG"),
        format_invalidated_lifer_message=MagicMock(return_value="INVAL_MSG"),
        all_time_list_link_view=MagicMock(return_value=None),
        year_list_link_view=MagicMock(return_value=None),
        checklist_link_view=MagicMock(return_value=None),
    )
    with p as patched:
        mocks.update(patched)

    # We also need to patch bot.get_channel
    bot_patch = patch(f"{MODULE}.bot")
    with bot_patch as mock_bot:
        mock_bot.get_channel = MagicMock(return_value=mock_channel)
        mocks["bot"] = mock_bot

    # We need the _last_year_lifer_check to be set so night check doesn't skip
    lylc_patch = patch(f"{MODULE}._last_year_lifer_check", None)
    with lylc_patch:
        pass

    return mocks


async def _run_check(mock_channel, **overrides):
    """Run check_year_lifers.coro() with all dependencies mocked."""
    default_mocks = dict(
        fetch_recent_observations=AsyncMock(return_value=[make_obs()]),
        check_for_new_all_time_lifers=MagicMock(return_value=([], [])),
        check_for_new_year_lifers=MagicMock(return_value=([], [])),
        check_pending_provisionals=AsyncMock(return_value=([], [])),
        get_all_time_total=MagicMock(return_value=100),
        get_year_total=MagicMock(return_value=42),
        format_all_time_lifer_message=MagicMock(return_value="AT_MSG"),
        format_year_lifer_message=MagicMock(return_value="YR_MSG"),
        format_tentative_all_time_lifer_message=MagicMock(return_value="TENT_AT_MSG"),
        format_tentative_year_lifer_message=MagicMock(return_value="TENT_YR_MSG"),
        format_confirmed_all_time_lifer_message=MagicMock(return_value="CONF_AT_MSG"),
        format_confirmed_year_lifer_message=MagicMock(return_value="CONF_YR_MSG"),
        format_invalidated_lifer_message=MagicMock(return_value="INVAL_MSG"),
        all_time_list_link_view=MagicMock(return_value=None),
        year_list_link_view=MagicMock(return_value=None),
        checklist_link_view=MagicMock(return_value=None),
    )
    default_mocks.update(overrides)

    mock_bot = MagicMock()
    mock_bot.get_channel = MagicMock(return_value=mock_channel)

    with (
        patch.multiple(MODULE, **default_mocks),
        patch(f"{MODULE}.bot", mock_bot),
        patch(f"{MODULE}._last_year_lifer_check", None),
    ):
        from cloaca.piper.main import check_year_lifers

        await check_year_lifers.coro()


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


class TestCheckYearLifersConfirmedAlerts:
    @pytest.mark.asyncio
    async def test_confirmed_all_time_lifer_sends_full_alert(self, mock_channel):
        obs = make_obs(obs_reviewed=True)
        await _run_check(
            mock_channel,
            check_for_new_all_time_lifers=MagicMock(return_value=([obs], [])),
        )
        messages = [call.args[0] for call in mock_channel.send.call_args_list]
        assert "AT_MSG" in messages

    @pytest.mark.asyncio
    async def test_confirmed_year_lifer_sends_full_alert(self, mock_channel):
        obs = make_obs("amrob", "American Robin", obs_reviewed=True)
        await _run_check(
            mock_channel,
            check_for_new_year_lifers=MagicMock(return_value=([obs], [])),
        )
        messages = [call.args[0] for call in mock_channel.send.call_args_list]
        assert "YR_MSG" in messages


class TestCheckYearLifersTentativeAlerts:
    @pytest.mark.asyncio
    async def test_provisional_all_time_sends_tentative(self, mock_channel):
        obs = make_obs(obs_reviewed=False)
        await _run_check(
            mock_channel,
            check_for_new_all_time_lifers=MagicMock(return_value=([], [obs])),
        )
        messages = [call.args[0] for call in mock_channel.send.call_args_list]
        assert "TENT_AT_MSG" in messages

    @pytest.mark.asyncio
    async def test_provisional_year_sends_tentative(self, mock_channel):
        obs = make_obs(obs_reviewed=False)
        await _run_check(
            mock_channel,
            check_for_new_year_lifers=MagicMock(return_value=([], [obs])),
        )
        messages = [call.args[0] for call in mock_channel.send.call_args_list]
        assert "TENT_YR_MSG" in messages


class TestCheckYearLifersYearExcludedWhenAllTime:
    @pytest.mark.asyncio
    async def test_year_lifers_excluded_when_also_all_time(self, mock_channel):
        """If a species is both a year lifer and an all-time lifer,
        only the all-time alert should fire."""
        obs = make_obs(obs_reviewed=True)
        await _run_check(
            mock_channel,
            check_for_new_all_time_lifers=MagicMock(return_value=([obs], [])),
            # Year check also returns the same species
            check_for_new_year_lifers=MagicMock(return_value=([obs], [])),
        )
        messages = [call.args[0] for call in mock_channel.send.call_args_list]
        assert "AT_MSG" in messages
        # Year message should NOT be sent because the species overlaps
        assert "YR_MSG" not in messages

    @pytest.mark.asyncio
    async def test_provisional_year_excluded_when_also_provisional_all_time(
        self, mock_channel
    ):
        obs = make_obs(obs_reviewed=False)
        await _run_check(
            mock_channel,
            check_for_new_all_time_lifers=MagicMock(return_value=([], [obs])),
            check_for_new_year_lifers=MagicMock(return_value=([], [obs])),
        )
        messages = [call.args[0] for call in mock_channel.send.call_args_list]
        assert "TENT_AT_MSG" in messages
        assert "TENT_YR_MSG" not in messages


class TestCheckYearLifersPendingFollowUp:
    @pytest.mark.asyncio
    async def test_pending_confirmed_sends_celebratory(self, mock_channel):
        pending = make_pending(lifer_type="all_time")
        await _run_check(
            mock_channel,
            check_pending_provisionals=AsyncMock(return_value=([pending], [])),
        )
        messages = [call.args[0] for call in mock_channel.send.call_args_list]
        assert "CONF_AT_MSG" in messages

    @pytest.mark.asyncio
    async def test_pending_confirmed_year(self, mock_channel):
        pending = make_pending(lifer_type="year")
        await _run_check(
            mock_channel,
            check_pending_provisionals=AsyncMock(return_value=([pending], [])),
        )
        messages = [call.args[0] for call in mock_channel.send.call_args_list]
        assert "CONF_YR_MSG" in messages

    @pytest.mark.asyncio
    async def test_pending_invalidated_sends_update(self, mock_channel):
        pending = make_pending()
        await _run_check(
            mock_channel,
            check_pending_provisionals=AsyncMock(return_value=([], [pending])),
        )
        messages = [call.args[0] for call in mock_channel.send.call_args_list]
        assert "INVAL_MSG" in messages


class TestCheckYearLifersNoActivity:
    @pytest.mark.asyncio
    async def test_no_lifers_sends_no_messages(self, mock_channel):
        await _run_check(mock_channel)
        mock_channel.send.assert_not_called()
