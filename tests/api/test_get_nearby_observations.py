import pytest
from cloaca.api.get_nearby_observations import get_nearby_observations
from cloaca.parsing.parsing_helpers import Lifer, Location
from cloaca.types import set_lifers_to_cache

DEFAULT_LONGITUDE = -74.0242
DEFAULT_LATITUDE = 40.6941


@pytest.mark.asyncio
@pytest.mark.vcr
async def test_get_nearby_observations():
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

    assert len(nearby_observations) > 1

    # northern harrier code should be filtered out
    for obs in nearby_observations.values():
        # map through obs.lifers to find the one with the same species code
        lifer = next(
            (lifer for lifer in obs.lifers if lifer.species_code == "norhar2"),
            None,
        )

        assert lifer is None

    central_park = nearby_observations["L191106"]
    expected = Location(
        location_name="Central Park",
        latitude=40.7715482,
        longitude=-73.9724819,
        location_id="L191106",
    )
    assert central_park.location == expected
    assert len(central_park.lifers) > 1
    example_house_sparrow = next(
        (lifers for lifers in central_park.lifers if lifers.species_code == "houspa"),
        None,
    )

    assert example_house_sparrow is not None

    expected_observation = Lifer(
        common_name="House Sparrow",
        latitude=40.7715482,
        longitude=-73.9724819,
        date=example_house_sparrow.date,
        location="Central Park",
        location_id="L191106",
        scientific_name="Passer domesticus",
        species_code="houspa",
        taxonomic_order=31261,
    )

    assert example_house_sparrow == expected_observation
