import pytest
from cloaca.api.get_lifers_by_location import get_lifers_by_location
from cloaca.parsing.parsing_helpers import Lifer, Location
from cloaca.types import set_lifers_to_cache

DEFAULT_LONGITUDE = -74.0242
DEFAULT_LATITUDE = 40.6941


@pytest.mark.asyncio
@pytest.mark.vcr
async def test_get_lifers_by_location():
    location_id = "L21909958"
    lifer = Lifer(
        common_name="Northern Harrier",
        latitude=29.819019,
        longitude=-89.61216,
        date="2022-11-21",
        taxonomic_order=8228,
        location="Hopedale",
        location_id=location_id,
        scientific_name="Circus hudsonius",
        species_code="norhar2",
    )

    set_lifers_to_cache(
        "key",
        [lifer],
    )

    response = await get_lifers_by_location(DEFAULT_LATITUDE, DEFAULT_LONGITUDE, "key")

    expected_location = Location(
        location_id=location_id,
        location_name="Hopedale",
        latitude=29.819019,
        longitude=-89.61216,
    )
    assert response[location_id].location == expected_location
    assert response[location_id].lifers == [lifer]
