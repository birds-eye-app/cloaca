import os
from typing import Dict, List
from phoebe_bird import AsyncPhoebe

from dotenv import load_dotenv

from phoebe_bird.types.data.observation import Observation as PhoebeObservation

load_dotenv()

phoebe_client = AsyncPhoebe(
    api_key=os.environ.get("EBIRD_API_KEY"),
)

requests: Dict[str, List[PhoebeObservation]] = {}
cached_species_obs: Dict[str, List[PhoebeObservation]] = {}

unwanted_scientific_names = [
    "Columba livia",  # non feral rock pigeon
]


def get_phoebe_client():
    return phoebe_client


async def fetch_nearby_observations_of_species_from_ebird_with_cache(
    species: str, latitude: float, longitude: float
) -> List[PhoebeObservation]:
    key = f"{species}-{latitude}-{longitude}"
    cache_result = cached_species_obs.get(key, None)
    if cache_result:
        print("hit cache!")
        return cache_result

    print("fetching nearby observations of species", species)

    observations = await phoebe_client.data.observations.nearest.geo_species.list(
        species_code=species,
        lat=latitude,
        lng=longitude,
        dist=50,
        include_provisional=False,
    )

    print(f"Fetched this many obs of species: {species}", len(observations))

    cached_species_obs[key] = observations

    return observations


def filter_out_unwanted_observations(observations: List[PhoebeObservation]):
    for observation in observations:
        if observation.sci_name in unwanted_scientific_names:
            print("removing unwanted observation", observation)
            observations.remove(observation)


async def fetch_nearby_observations_from_ebird_with_cache(
    latitude: float, longitude: float
):
    key = f"{latitude}-{longitude}"
    print(f"fetching nearby observations for {latitude}, {longitude}")
    cache_result = requests.get(key, None)
    if cache_result:
        print("hit cache!")
        return cache_result

    observations = await phoebe_client.data.observations.geo.recent.list(
        lat=latitude, lng=longitude, dist=50, cat="species", include_provisional=False
    )

    filter_out_unwanted_observations(observations)

    print("Fetched this many obs:", len(observations))

    requests[key] = observations

    return observations


def round_to_nearest_half(num):
    return round(num * 2) / 2
