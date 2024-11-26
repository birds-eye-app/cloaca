from dataclasses import dataclass
from typing import Dict

from cloaca.parsing.parse_ebird_regional_list import (
    SubnationalRegion,
    parse_subnational1_file,
)
from cloaca.parsing.parsing_helpers import Lifer
from cloaca.phoebe_wrapper import get_phoebe_client
from cloaca.types import phoebe_observation_to_lifer
from phoebe_bird.types.data.observation import Observation as PhoebeObservation


@dataclass
class SubRegionAndObservations:
    subnational_region: SubnationalRegion
    observations: list[Lifer]


regional_mapping: Dict[str, SubRegionAndObservations] = {}


async def fetch_observations_for_regions_from_phoebe(
    subnational_code: str,
) -> list[PhoebeObservation]:
    return await get_phoebe_client().data.observations.recent.list(
        back=30,
        cat="species",
        hotspot=True,
        region_code=subnational_code,
    )


def get_lifers_for_region(subnational_code: str) -> list[Lifer]:
    return regional_mapping[subnational_code].observations


# go through subnational codes from ebird and prepare the mapping
# by setting a key for each subnational code
async def get_regional_mapping():
    sub_regions = parse_subnational1_file()

    filtered_sub_regions = [
        sub_region
        for sub_region in sub_regions
        if sub_region.country_code == "US" and sub_region.subnational1_code
    ]

    for sub_region in filtered_sub_regions:
        phoebe_observations = await fetch_observations_for_regions_from_phoebe(
            sub_region.subnational1_code
        )
        print(
            f"Found {len(phoebe_observations)} observations for {sub_region.subnational1_name}"
        )
        lifers = [
            phoebe_observation_to_lifer(observation)
            for observation in phoebe_observations
        ]

        regional_mapping[sub_region.subnational1_code] = SubRegionAndObservations(
            subnational_region=sub_region, observations=lifers
        )

    print("Finished fetching observations for all subnational regions")
    print(f"Found {len(regional_mapping)} subnational regions")


async def get_regional_lifers() -> list[Lifer]:
    if not regional_mapping:
        print("Performing initial fetch of regional mapping")
        await get_regional_mapping()
    return [
        lifer for region in regional_mapping.values() for lifer in region.observations
    ]
