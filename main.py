from typing import Dict
from uuid import uuid4

from cloaca.parse import parse_csv_from_file_to_lifers
from cloaca.types import (
    filter_lifers_from_nearby_observations,
    get_lifers_from_cache,
    group_lifers_by_location,
    set_lifers_to_cache,
)

import os
from phoebe_bird import Phoebe
from phoebe_bird.types.data.observations.geo.recent_list_response import (
    RecentListResponse,
)
from fastapi.middleware.cors import CORSMiddleware
from fastapi import FastAPI, UploadFile

from dotenv import load_dotenv

load_dotenv()
INITIAL_CENTER = {"lng": -74.0242, "lat": 40.6941}


app = FastAPI()

phoebe_client = Phoebe(
    api_key=os.environ.get("EBIRD_API_KEY"),
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # todo: dont do this
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.get("/health")
def health_check():
    return {"status": "SQUAWK"}


requests: Dict[str, RecentListResponse] = {}


def fetch_nearby_observations_from_ebird_with_cache(latitude: float, longitude: float):
    key = f"{latitude}-{longitude}"
    cache_result = requests.get(key, None)
    if cache_result:
        print("hit cache!")
        return cache_result

    observations = phoebe_client.data.observations.geo.recent.list(
        lat=latitude, lng=longitude
    )
    print("Fetched this many obs:", len(observations))

    requests[key] = observations

    return observations


@app.get("/v1/nearby_observations")
def get_nearby_observations(latitude: float, longitude: float, file_id: str):
    nearby_observations = fetch_nearby_observations_from_ebird_with_cache(
        round(latitude, 2), round(longitude, 2)
    )

    lifers_from_csv = get_lifers_from_cache(file_id)

    unseen_observations = filter_lifers_from_nearby_observations(
        nearby_observations, lifers_from_csv
    )

    lifers_by_location = group_lifers_by_location(unseen_observations)

    return lifers_by_location


@app.get("/v1/lifers_by_location")
def get_lifers(latitude: float, longitude: float, file_id: str):
    lifers_from_csv = get_lifers_from_cache(file_id)

    lifers_by_location = group_lifers_by_location(lifers_from_csv)

    print(lifers_by_location)

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
