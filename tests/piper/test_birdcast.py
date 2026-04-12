"""Tests for piper birdcast forecast polling and posting."""

import datetime
from unittest.mock import AsyncMock, MagicMock, patch
from zoneinfo import ZoneInfo

import pytest

from cloaca.piper.birdcast import (
    BirdcastForecast,
    ForecastNight,
    format_forecast_message,
    is_forecast_posted,
    is_todays_forecast,
    mark_forecast_posted,
)

EASTERN = ZoneInfo("America/New_York")
MODULE = "cloaca.piper.main"
BIRDCAST_MODULE = "cloaca.piper.birdcast"


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def make_forecast(
    *,
    first_date: datetime.datetime | None = None,
    nights: int = 3,
) -> BirdcastForecast:
    """Build a BirdcastForecast with `nights` consecutive nights starting at first_date."""
    if first_date is None:
        first_date = datetime.datetime.now(EASTERN).replace(
            hour=0, minute=0, second=0, microsecond=0
        )
    forecast_nights = []
    for i in range(nights):
        forecast_nights.append(
            ForecastNight(
                date=first_date + datetime.timedelta(days=i),
                total=1000 * (i + 1),
                code=i + 1,
            )
        )
    return BirdcastForecast(
        generatedDate=first_date,
        forecastNights=forecast_nights,
    )


# ---------------------------------------------------------------------------
# is_todays_forecast (pure function)
# ---------------------------------------------------------------------------


class TestIsTodaysForecast:
    def test_true_when_first_night_is_today(self):
        now = datetime.datetime.now(EASTERN)
        today = now.replace(hour=0, minute=0, second=0, microsecond=0)
        forecast = make_forecast(first_date=today)
        assert is_todays_forecast(forecast) is True

    def test_false_when_first_night_is_yesterday(self):
        now = datetime.datetime.now(EASTERN)
        yesterday = (now - datetime.timedelta(days=1)).replace(
            hour=0, minute=0, second=0, microsecond=0
        )
        forecast = make_forecast(first_date=yesterday)
        assert is_todays_forecast(forecast) is False

    def test_false_when_no_nights(self):
        forecast = BirdcastForecast(
            generatedDate=datetime.datetime.now(EASTERN),
            forecastNights=[],
        )
        assert is_todays_forecast(forecast) is False


# ---------------------------------------------------------------------------
# format_forecast_message (pure function)
# ---------------------------------------------------------------------------


class TestFormatForecastMessage:
    def test_contains_migration_update_header(self):
        forecast = make_forecast()
        msg = format_forecast_message(forecast)
        assert "**Migration Update**" in msg

    def test_contains_tonight_and_tomorrow(self):
        forecast = make_forecast()
        msg = format_forecast_message(forecast)
        assert "Tonight" in msg
        assert "Tomorrow" in msg

    def test_three_nights_shows_weekday_for_third(self):
        now = datetime.datetime.now(EASTERN)
        today = now.replace(hour=0, minute=0, second=0, microsecond=0)
        forecast = make_forecast(first_date=today, nights=3)
        msg = format_forecast_message(forecast)
        day_after_tomorrow = today + datetime.timedelta(days=2)
        weekday_name = day_after_tomorrow.strftime("%A")
        assert weekday_name in msg

    def test_tier_labels_present(self):
        forecast = make_forecast(nights=3)
        msg = format_forecast_message(forecast)
        assert "Low" in msg
        assert "Medium" in msg
        assert "High" in msg


# ---------------------------------------------------------------------------
# DB helpers (is_forecast_posted / mark_forecast_posted)
# ---------------------------------------------------------------------------


class TestBirdcastPostLog:
    @pytest.mark.asyncio
    async def test_not_posted_initially(self):
        today = datetime.date.today()
        assert await is_forecast_posted(today) is False

    @pytest.mark.asyncio
    async def test_posted_after_mark(self):
        today = datetime.date.today()
        await mark_forecast_posted(today)
        assert await is_forecast_posted(today) is True

    @pytest.mark.asyncio
    async def test_different_dates_independent(self):
        today = datetime.date.today()
        yesterday = today - datetime.timedelta(days=1)
        await mark_forecast_posted(yesterday)
        assert await is_forecast_posted(yesterday) is True
        assert await is_forecast_posted(today) is False

    @pytest.mark.asyncio
    async def test_duplicate_mark_is_noop(self):
        today = datetime.date.today()
        await mark_forecast_posted(today)
        await mark_forecast_posted(today)  # should not raise
        assert await is_forecast_posted(today) is True


# ---------------------------------------------------------------------------
# post_birdcast_forecast task loop orchestration
# ---------------------------------------------------------------------------


async def _run_forecast(mock_channel, *, now, forecast=None):
    """Run post_birdcast_forecast.coro() with dependencies mocked."""
    mock_bot = MagicMock()
    mock_bot.get_channel = MagicMock(return_value=mock_channel)

    with (
        patch(f"{MODULE}.bot", mock_bot),
        patch(f"{MODULE}.datetime") as mock_dt,
        patch(
            f"{MODULE}.fetch_birdcast_forecast",
            AsyncMock(return_value=forecast),
        ),
        patch(
            f"{MODULE}.format_forecast_message",
            MagicMock(return_value="FORECAST_MSG"),
        ),
        patch(f"{MODULE}.birdcast_link_view", MagicMock(return_value=None)),
    ):
        mock_dt.datetime.now.return_value = now
        mock_dt.time = datetime.time

        from cloaca.piper.main import post_birdcast_forecast

        await post_birdcast_forecast.coro()


class TestPostBirdcastForecast:
    @pytest.fixture
    def mock_channel(self):
        ch = AsyncMock()
        ch.send = AsyncMock()
        return ch

    @pytest.mark.asyncio
    async def test_posts_when_fresh_and_not_yet_posted(self, mock_channel):
        now = datetime.datetime(2026, 4, 12, 8, 0, tzinfo=EASTERN)
        today = now.replace(hour=0, minute=0, second=0, microsecond=0)
        forecast = make_forecast(first_date=today)
        await _run_forecast(mock_channel, now=now, forecast=forecast)
        mock_channel.send.assert_called_once_with("FORECAST_MSG", view=None)

    @pytest.mark.asyncio
    async def test_skips_before_7am(self, mock_channel):
        now = datetime.datetime(2026, 4, 12, 6, 30, tzinfo=EASTERN)
        today = now.replace(hour=0, minute=0, second=0, microsecond=0)
        forecast = make_forecast(first_date=today)
        await _run_forecast(mock_channel, now=now, forecast=forecast)
        mock_channel.send.assert_not_called()

    @pytest.mark.asyncio
    async def test_skips_after_noon(self, mock_channel):
        now = datetime.datetime(2026, 4, 12, 12, 0, tzinfo=EASTERN)
        today = now.replace(hour=0, minute=0, second=0, microsecond=0)
        forecast = make_forecast(first_date=today)
        await _run_forecast(mock_channel, now=now, forecast=forecast)
        mock_channel.send.assert_not_called()

    @pytest.mark.asyncio
    async def test_skips_when_forecast_not_updated(self, mock_channel):
        now = datetime.datetime(2026, 4, 12, 8, 0, tzinfo=EASTERN)
        yesterday = now.replace(
            hour=0, minute=0, second=0, microsecond=0
        ) - datetime.timedelta(days=1)
        forecast = make_forecast(first_date=yesterday)
        await _run_forecast(mock_channel, now=now, forecast=forecast)
        mock_channel.send.assert_not_called()

    @pytest.mark.asyncio
    async def test_skips_when_no_forecast(self, mock_channel):
        now = datetime.datetime(2026, 4, 12, 8, 0, tzinfo=EASTERN)
        await _run_forecast(mock_channel, now=now, forecast=None)
        mock_channel.send.assert_not_called()

    @pytest.mark.asyncio
    async def test_skips_when_already_posted(self, mock_channel):
        now = datetime.datetime(2026, 4, 12, 8, 0, tzinfo=EASTERN)
        today_dt = now.replace(hour=0, minute=0, second=0, microsecond=0)
        forecast = make_forecast(first_date=today_dt)
        # Post once
        await _run_forecast(mock_channel, now=now, forecast=forecast)
        mock_channel.send.assert_called_once()
        mock_channel.send.reset_mock()
        # Second run should skip
        await _run_forecast(mock_channel, now=now, forecast=forecast)
        mock_channel.send.assert_not_called()

    @pytest.mark.asyncio
    async def test_records_post_in_db(self, mock_channel):
        now = datetime.datetime(2026, 4, 12, 8, 0, tzinfo=EASTERN)
        today_dt = now.replace(hour=0, minute=0, second=0, microsecond=0)
        forecast = make_forecast(first_date=today_dt)
        await _run_forecast(mock_channel, now=now, forecast=forecast)
        assert await is_forecast_posted(now.date()) is True
