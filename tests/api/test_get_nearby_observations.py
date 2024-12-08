import pytest
from cloaca.api.get_nearby_observations import get_nearby_observations
from cloaca.parsing.parsing_helpers import Lifer
from cloaca.types import set_lifers_to_cache

DEFAULT_LONGITUDE = -74.0242
DEFAULT_LATITUDE = 40.6941


@pytest.mark.asyncio
@pytest.mark.vcr
async def test_regional_lifers():
    set_lifers_to_cache(
        "key",
        [
            Lifer(
                common_name="Northern Harrier",
                latitude=29.819019,
                longitude=-89.61216,
                date="2022-11-21",
                taxonomic_order=8228,
                location="Hopedale",
                location_id="L21909958",
                scientific_name="Circus hudsonius",
                species_code="norhar2",
            )
        ],
    )

    nearby_observations = await get_nearby_observations(
        DEFAULT_LATITUDE, DEFAULT_LONGITUDE, "key"
    )

    assert len(nearby_observations) == 1429
