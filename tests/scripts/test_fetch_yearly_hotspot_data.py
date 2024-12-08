from datetime import datetime
import pytest
from cloaca.scripts.fetch_yearly_hotspot_data import (
    backfill_hotspot_observations,
    parse_historic_observation_csv,
)


@pytest.mark.asyncio
@pytest.mark.vcr
async def test_backfill_hotspot_observations():
    await backfill_hotspot_observations()


def test_parse_historic_observation_csv():
    parsed = parse_historic_observation_csv()

    assert len(parsed) > 0

    first = parsed[0]
    assert first.speciesCode == "amhgul1"
    assert first.firstName == "June"
    assert first.howMany is None
    assert first.obsDt == datetime(2024, 1, 1, 10, 38)
