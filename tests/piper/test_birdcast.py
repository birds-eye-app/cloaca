"""Tests for piper birdcast forecast polling and posting."""

import datetime
from unittest.mock import AsyncMock, MagicMock, patch
from zoneinfo import ZoneInfo

import pytest

from cloaca.piper.birdcast import (
    BirdcastForecast,
    ForecastNight,
    MigrationSeason,
    MigrationTraffic,
    NightSeriesEntry,
    format_forecast_message,
    format_migration_traffic_message,
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
    def test_contains_forecast_update_header(self):
        forecast = make_forecast()
        msg = format_forecast_message(forecast)
        assert "**Forecast Update**" in msg

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


# ---------------------------------------------------------------------------
# Migration traffic helpers
# ---------------------------------------------------------------------------


def make_night_series(
    *,
    start_hour: int = 19,
    start_minute: int = 30,
    intervals: int = 6,
    peak_idx: int = 3,
    peak_aloft: int = 500,
    base_date: str = "2026-04-11",
) -> list[NightSeriesEntry]:
    entries = []
    for i in range(intervals):
        hour = start_hour + (start_minute + i * 10) // 60
        minute = (start_minute + i * 10) % 60
        aloft = (
            peak_aloft
            if i == peak_idx
            else max(10, peak_aloft // (abs(i - peak_idx) + 1))
        )
        entries.append(
            NightSeriesEntry(
                localTime=f"{base_date}T{hour:02d}:{minute:02d}:00",
                utc=f"{base_date}T{hour + 4:02d}:{minute:02d}:00Z",
                numAloft=aloft,
                avgDirection=55.0 if i == peak_idx else None,
                avgSpeed=12.5 if i == peak_idx else None,
                meanHeight=1500.0 if i == peak_idx else None,
            )
        )
    return entries


def make_traffic(
    *,
    cumulative: int = 1200,
    night_series: list[NightSeriesEntry] | None = None,
) -> MigrationTraffic:
    if night_series is None:
        night_series = make_night_series()
    return MigrationTraffic(
        lastUpdated="2026-04-12T10:20:00Z",
        regionCode="US-NY-047",
        timezoneName="America/New_York",
        cumulativeBirds=cumulative,
        isHigh=False,
        season=MigrationSeason(code="SP", startDate="20260301", endDate="20260615"),
        nightSeries=night_series,
    )


# ---------------------------------------------------------------------------
# format_migration_traffic_message (pure function)
# ---------------------------------------------------------------------------


class TestFormatMigrationTrafficMessage:
    def test_contains_header(self):
        traffic = make_traffic()
        msg = format_migration_traffic_message(traffic, datetime.date(2026, 4, 11))
        assert "**Last Night's Migration**" in msg

    def test_contains_total_birds(self):
        traffic = make_traffic(cumulative=1200)
        msg = format_migration_traffic_message(traffic, datetime.date(2026, 4, 11))
        assert "1,200 birds" in msg

    def test_contains_peak_info(self):
        traffic = make_traffic()
        msg = format_migration_traffic_message(traffic, datetime.date(2026, 4, 11))
        assert "**Peak:**" in msg
        assert "500 birds" in msg

    def test_peak_includes_direction_when_available(self):
        traffic = make_traffic()
        msg = format_migration_traffic_message(traffic, datetime.date(2026, 4, 11))
        assert "heading NE" in msg

    def test_contains_night_timespan(self):
        traffic = make_traffic()
        msg = format_migration_traffic_message(traffic, datetime.date(2026, 4, 11))
        assert "7:30 PM" in msg

    def test_quiet_night_message(self):
        series = [
            NightSeriesEntry(
                localTime="2026-04-11T19:30:00",
                utc="2026-04-11T23:30:00Z",
                numAloft=0,
            )
        ]
        traffic = make_traffic(cumulative=0, night_series=series)
        msg = format_migration_traffic_message(traffic, datetime.date(2026, 4, 11))
        assert "quiet night" in msg

    def test_no_direction_when_null(self):
        series = [
            NightSeriesEntry(
                localTime="2026-04-11T22:00:00",
                utc="2026-04-12T02:00:00Z",
                numAloft=50,
                avgDirection=None,
            )
        ]
        traffic = make_traffic(night_series=series)
        msg = format_migration_traffic_message(traffic, datetime.date(2026, 4, 11))
        assert "heading" not in msg


# ---------------------------------------------------------------------------
# post_migration_traffic task loop orchestration
# ---------------------------------------------------------------------------


async def _run_traffic(mock_channel, *, now, traffic=None):
    mock_bot = MagicMock()
    mock_bot.get_channel = MagicMock(return_value=mock_channel)

    with (
        patch(f"{MODULE}.bot", mock_bot),
        patch(f"{MODULE}.datetime") as mock_dt,
        patch(
            f"{MODULE}.fetch_migration_traffic",
            AsyncMock(return_value=traffic),
        ),
        patch(
            f"{MODULE}.format_migration_traffic_message",
            MagicMock(return_value="TRAFFIC_MSG"),
        ),
        patch(f"{MODULE}.migration_dashboard_link_view", MagicMock(return_value=None)),
    ):
        mock_dt.datetime.now.return_value = now
        mock_dt.time = datetime.time
        mock_dt.timedelta = datetime.timedelta

        from cloaca.piper.main import post_migration_traffic

        await post_migration_traffic.coro()


class TestPostMigrationTraffic:
    @pytest.fixture
    def mock_channel(self):
        ch = AsyncMock()
        ch.send = AsyncMock()
        return ch

    @pytest.mark.asyncio
    async def test_posts_when_data_available(self, mock_channel):
        now = datetime.datetime(2026, 4, 12, 6, 0, tzinfo=EASTERN)
        traffic = make_traffic()
        await _run_traffic(mock_channel, now=now, traffic=traffic)
        mock_channel.send.assert_called_once_with("TRAFFIC_MSG", view=None)

    @pytest.mark.asyncio
    async def test_skips_when_no_data(self, mock_channel):
        now = datetime.datetime(2026, 4, 12, 6, 0, tzinfo=EASTERN)
        await _run_traffic(mock_channel, now=now, traffic=None)
        mock_channel.send.assert_not_called()

    @pytest.mark.asyncio
    async def test_uses_yesterday_as_night_date(self, mock_channel):
        """The traffic API date should be yesterday (the night that started)."""
        now = datetime.datetime(2026, 4, 12, 6, 0, tzinfo=EASTERN)
        traffic = make_traffic()

        mock_fetch = AsyncMock(return_value=traffic)
        mock_bot = MagicMock()
        mock_bot.get_channel = MagicMock(return_value=mock_channel)

        with (
            patch(f"{MODULE}.bot", mock_bot),
            patch(f"{MODULE}.datetime") as mock_dt,
            patch(f"{MODULE}.fetch_migration_traffic", mock_fetch),
            patch(
                f"{MODULE}.format_migration_traffic_message",
                MagicMock(return_value="TRAFFIC_MSG"),
            ),
            patch(
                f"{MODULE}.migration_dashboard_link_view",
                MagicMock(return_value=None),
            ),
        ):
            mock_dt.datetime.now.return_value = now
            mock_dt.time = datetime.time
            mock_dt.timedelta = datetime.timedelta

            from cloaca.piper.main import post_migration_traffic

            await post_migration_traffic.coro()

        mock_fetch.assert_called_once_with(datetime.date(2026, 4, 11))
