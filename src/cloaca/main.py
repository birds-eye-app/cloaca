import asyncio
import time
from typing import List
from uuid import uuid4

from cloaca.api.get_new_lifers_by_region import (
    get_filtered_lifers_for_region,
)
from cloaca.api.shared import round_to_nearest_half
from cloaca.parsing.parse_ebird_personal_export import parse_csv_from_file_to_lifers
from cloaca.parsing.parsing_helpers import Lifer
from cloaca.api.shared import (
    fetch_nearby_observations_from_ebird_with_cache,
    fetch_nearby_observations_of_species_from_ebird_with_cache,
)
from cloaca.types import (
    filter_lifers_from_nearby_observations,
    get_lifers_from_cache,
    group_lifers_by_location,
    phoebe_observation_to_lifer,
    set_lifers_to_cache,
)

import os
from phoebe_bird import AsyncPhoebe
from fastapi.middleware.cors import CORSMiddleware
from fastapi import FastAPI, Request, UploadFile

from dotenv import load_dotenv

load_dotenv()
INITIAL_CENTER = {"lng": -74.0242, "lat": 40.6941}


Cloaca_App = FastAPI()

phoebe_client = AsyncPhoebe(
    api_key=os.environ.get("EBIRD_API_KEY"),
)

Cloaca_App.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # todo: dont do this
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


@Cloaca_App.middleware("http")
async def add_process_time_header(request: Request, call_next):
    start_time = time.time()
    response = await call_next(request)
    print(
        "Time took to process the request and return response is {} sec".format(
            time.time() - start_time
        )
    )
    return response


@Cloaca_App.get("/v1/health")
def health_check():
    return {"status": "SQUAWK"}


@Cloaca_App.get("/v1/nearby_observations")
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


@Cloaca_App.get("/v1/lifers_by_location")
def get_lifers(latitude: float, longitude: float, file_id: str):
    lifers_from_csv = get_lifers_from_cache(file_id)

    lifers_by_location = group_lifers_by_location(lifers_from_csv)

    return lifers_by_location


@Cloaca_App.post("/v1/upload_lifers_csv")
def upload_lifers_csv(file: UploadFile):
    print("Uploading file", file.filename)

    # turn the file into a pandas dataframe
    csv = parse_csv_from_file_to_lifers(file)
    uuid4_str = str(uuid4())

    set_lifers_to_cache(uuid4_str, csv)

    print(f"parsed csv with key {uuid4_str} and length {len(csv)}")

    return {"key": uuid4_str}


@Cloaca_App.get("/v1/regional_new_potential_lifers")
async def regional_lifers(
    latitude: float, longitude: float, file_id: str
) -> list[Lifer]:
    regional_lifers = await get_filtered_lifers_for_region(latitude, longitude, file_id)

    print("returning", len(regional_lifers), "regional lifers")

    return regional_lifers
