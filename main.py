import asyncio
import time
from typing import Dict, List
from uuid import uuid4

from cloaca.parse import Lifer, parse_csv_from_file_to_lifers
from cloaca.types import (
    filter_lifers_from_nearby_observations,
    get_lifers_from_cache,
    group_lifers_by_location,
    phoebe_observation_to_lifer,
    set_lifers_to_cache,
)

import os
from phoebe_bird import AsyncPhoebe
from phoebe_bird.types.data.observation import Observation as PhoebeObservation
from fastapi.middleware.cors import CORSMiddleware
from fastapi import FastAPI, Request, UploadFile

from dotenv import load_dotenv

load_dotenv()
INITIAL_CENTER = {"lng": -74.0242, "lat": 40.6941}


app = FastAPI()

phoebe_client = AsyncPhoebe(
    api_key=os.environ.get("EBIRD_API_KEY"),
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # todo: dont do this
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.middleware("http")
async def add_process_time_header(request: Request, call_next):
    start_time = time.time()
    response = await call_next(request)
    print(
        "Time took to process the request and return response is {} sec".format(
            time.time() - start_time
        )
    )
    return response


@app.get("/v1/health")
def health_check():
    return {"status": "SQUAWK"}


requests: Dict[str, List[PhoebeObservation]] = {}
cached_species_obs: Dict[str, List[PhoebeObservation]] = {}

unwanted_scientific_names = [
    "Columba livia",  # non feral rock pigeon
]


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


def round_to_nearest_half(num):
    return round(num * 2) / 2


@app.get("/v1/nearby_observations")
async def get_nearby_observations(latitude: float, longitude: float, file_id: str):
    request_start_time = time.time()
    nearby_observations = await fetch_nearby_observations_from_ebird_with_cache(
        round_to_nearest_half(latitude), round_to_nearest_half(longitude)
    )

    lifers_from_csv = get_lifers_from_cache(file_id)

    unseen_species = filter_lifers_from_nearby_observations(
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
            unseen_lifers.append(phoebe_observation_to_lifer(observation))

    fetch_tasks = [
        fetch_task(unseen_species_code) for unseen_species_code in unseen_species_codes
    ]

    await asyncio.gather(*fetch_tasks)

    lifers_by_location = group_lifers_by_location(unseen_lifers)

    print("returning", len(lifers_by_location), "locations")

    duration = time.time() - request_start_time
    print(f"request took {duration} seconds")

    return lifers_by_location


@app.get("/v1/lifers_by_location")
def get_lifers(latitude: float, longitude: float, file_id: str):
    lifers_from_csv = get_lifers_from_cache(file_id)

    lifers_by_location = group_lifers_by_location(lifers_from_csv)

    return lifers_by_location


@app.post("/v1/upload_lifers_csv")
def upload_lifers_csv(file: UploadFile):
    print("Uploading file", file.filename)

    # turn the file into a pandas dataframe
    csv = parse_csv_from_file_to_lifers(file)
    uuid4_str = str(uuid4())

    set_lifers_to_cache(uuid4_str, csv)

    print(f"parsed csv with key {uuid4_str} and length {len(csv)}")

    return {"key": uuid4_str}
