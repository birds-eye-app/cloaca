import time
from uuid import uuid4

from cloaca.api.get_new_lifers_by_region import (
    get_filtered_lifers_for_region,
)
from cloaca.parsing.parse_ebird_personal_export import parse_csv_from_file_to_lifers
from cloaca.parsing.parsing_helpers import Lifer
from cloaca.types import (
    get_lifers_from_cache,
    group_lifers_by_location,
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
    return get_nearby_observations(latitude, longitude, file_id)


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
