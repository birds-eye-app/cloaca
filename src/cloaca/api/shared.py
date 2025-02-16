import os
from typing import Dict, List
from phoebe_bird import AsyncPhoebe

from dotenv import load_dotenv

from phoebe_bird.types.data.observation import Observation as PhoebeObservation
from phoebe_bird.types.ref.taxonomy.ebird_retrieve_response import (
    EbirdRetrieveResponseItem as EbirdTaxonomyItem,
)

load_dotenv()

phoebe_client = AsyncPhoebe(
    api_key=os.environ.get("EBIRD_API_KEY"),
)

nearby_observation_cache: Dict[str, List[PhoebeObservation]] = {}
cached_species_obs: Dict[str, List[PhoebeObservation]] = {}
ebirdTaxonomy: Dict[str, EbirdTaxonomyItem] = {}

unwanted_scientific_names = [
    "Columba livia",  # non feral rock pigeon
]


def get_phoebe_client():
    return phoebe_client


async def shared_clear_nearby_observations_cache():
    nearby_observation_cache.clear()
    cached_species_obs.clear()


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
    cache_result = nearby_observation_cache.get(key, None)
    if cache_result:
        print("hit cache!")
        return cache_result

    observations = await phoebe_client.data.observations.geo.recent.list(
        lat=latitude, lng=longitude, dist=50, cat="species", include_provisional=False
    )

    filter_out_unwanted_observations(observations)

    print("Fetched this many obs:", len(observations))

    nearby_observation_cache[key] = observations

    return observations


def round_to_nearest_half(num):
    return round(num * 2) / 2


async def fetch_ebird_taxonomy_with_cache():
    if len(ebirdTaxonomy.keys()) == 0:
        taxonomy_items = await phoebe_client.ref.taxonomy.ebird.retrieve(fmt="json")

        print(f"Fetched {len(taxonomy_items)} taxonomy items")

        # loop through each taxonomy item and set the cache to its species code
        for taxonomy_item in taxonomy_items:
            if taxonomy_item.species_code in ebirdTaxonomy.keys():
                # this shouldn't happpen!
                print("Warning, species code reported multiple times in taxonomy!")
            else:
                if taxonomy_item.species_code is None:
                    print(
                        f"Warning, species code is none for ${taxonomy_item.sci_name}"
                    )
                    continue
                ebirdTaxonomy[taxonomy_item.species_code] = taxonomy_item

    return ebirdTaxonomy


async def get_taxonomy_info_for_species_code(species_code: str) -> EbirdTaxonomyItem:
    return (await fetch_ebird_taxonomy_with_cache())[species_code]
