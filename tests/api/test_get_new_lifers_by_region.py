from fastapi.testclient import TestClient
import pytest
from cloaca.api.get_new_lifers_by_region import (
    fetch_observations_for_regions_from_phoebe,
    get_lifers_for_region,
    get_regional_mapping,
)
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


# @pytest.mark.asyncio
# @pytest.mark.vcr
# async def test_regional_lifers():
#     client = TestClient(Cloaca_App)
#     set_lifers_to_cache("test-key", [])
#     response = client.get("/v1/regional_new_potential_lifers")

#     assert response.status_code == 200

#     data = response.json()
