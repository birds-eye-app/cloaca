from fastapi.testclient import TestClient
import pytest
from cloaca.api.get_new_lifers_by_region import (
    fetch_observations_for_regions_from_phoebe,
    get_filtered_lifers_for_region,
    get_lifers_for_region,
    get_regional_mapping,
)
from cloaca.parsing.parsing_helpers import Lifer
from cloaca.types import set_lifers_to_cache
from cloaca.main import Cloaca_App


@pytest.mark.asyncio
@pytest.mark.vcr
async def test_get_regional_mapping():
    await get_regional_mapping()

    ny_obs = get_lifers_for_region("US-NY")

    assert len(ny_obs) == 277


@pytest.mark.asyncio
@pytest.mark.vcr
async def test_fetch_observations_for_regions_from_phoebe():
    ny_obs = await fetch_observations_for_regions_from_phoebe("US-NY")

    assert len(ny_obs) == 277
    first_obs = ny_obs[0]
    assert first_obs.species_code == "bkcchi"


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
                species_code="norhar2",
            )
        ],
    )

    lifers = await get_filtered_lifers_for_region(72, 71, "key")

    assert len(lifers) == 11311

    # make sure the lifer is not in the list
    assert all(lifer.species_code != "norhar2" for lifer in lifers)
