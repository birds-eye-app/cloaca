import asyncio
import time
from typing import List

from cloaca.api.shared import (
    fetch_nearby_observations_from_ebird_with_cache,
    fetch_nearby_observations_of_species_from_ebird_with_cache,
    round_to_nearest_half,
    shared_clear_nearby_observations_cache,
)
from cloaca.parsing.parsing_helpers import Lifer
from cloaca.types import (
    filter_lifers_from_nearby_observations,
    get_lifers_from_cache,
    group_lifers_by_location,
    phoebe_observation_to_lifer,
)


async def clear_nearby_observations_cache():
    await shared_clear_nearby_observations_cache()


async def get_nearby_observations(latitude: float, longitude: float, file_id: str):
    request_start_time = time.time()
    nearby_observations = await fetch_nearby_observations_from_ebird_with_cache(
        round_to_nearest_half(latitude), round_to_nearest_half(longitude)
    )

    lifers_from_csv = get_lifers_from_cache(file_id)

    unseen_species = await filter_lifers_from_nearby_observations(
        nearby_observations, lifers_from_csv
    )

    unseen_species_codes = list(
        set([species.species_code for species in unseen_species])
    )

    print(f"unseen species codes: {unseen_species_codes}")

    # use async.gather to make multiple requests at once
    unseen_lifers: List[Lifer] = []

    async def fetch_task(species_code):
        unseen_observations_for_species = await (
            fetch_nearby_observations_of_species_from_ebird_with_cache(
                species_code,
                round_to_nearest_half(latitude),
                round_to_nearest_half(longitude),
            )
        )

        for observation in unseen_observations_for_species:
            unseen_lifers.append(await phoebe_observation_to_lifer(observation))

    fetch_tasks = [
        fetch_task(unseen_species_code) for unseen_species_code in unseen_species_codes
    ]

    await asyncio.gather(*fetch_tasks)

    lifers_by_location = group_lifers_by_location(unseen_lifers)

    print("returning", len(lifers_by_location), "locations")

    duration = time.time() - request_start_time
    print(f"request took {duration} seconds")

    return lifers_by_location
