from typing import Dict
from uuid import uuid4

from scripts.parse_data import (
    Lifer,
    Location,
    LocationToLifers,
    parse_csv_from_file_to_lifers,
)

from fastapi import FastAPI
import os
from phoebe_bird import Phoebe
from phoebe_bird.types.data.observation import Observation as PhoebeObservation
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


@app.get("/")
def read_root():
    return {"Hello": "World"}


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


def filter_lifers_from_nearby_observations(
    nearby_observations: RecentListResponse, lifers: list[Lifer]
) -> list[Lifer]:
    # map through response and convert to lifers
    nearby_observations_to_lifers: list[Lifer] = list()
    for observation in nearby_observations:
        nearby_observations_to_lifers.append(phoebe_observation_to_lifer(observation))
    lifer_commons_names = [lifer.common_name for lifer in lifers]

    unseen_observations: list[Lifer] = list()
    already_seen_observations: list[Lifer] = list()
    for observation in nearby_observations_to_lifers:
        if observation.common_name in lifer_commons_names:
            already_seen_observations.append(observation)
        else:
            unseen_observations.append(observation)

    print("Already seen obs:", len(already_seen_observations))
    print("Unseen obs:", len(unseen_observations))

    return unseen_observations


def phoebe_observation_to_lifer(
    ebird_observation: PhoebeObservation,
) -> Lifer:
    return Lifer(
        common_name=ebird_observation.com_name or "",
        latitude=ebird_observation.lat or 0,
        longitude=ebird_observation.lng or 0,
        date=ebird_observation.obs_dt or "",
        taxonomic_order=0,
        location=ebird_observation.loc_name or "",
        location_id=ebird_observation.loc_id or "",
    )


def group_lifers_by_location(lifers: list[Lifer]) -> Dict[str, LocationToLifers]:
    lifers_by_location: Dict[str, LocationToLifers] = {}
    lifers.reverse()  # reverse to make sure we use the most recent location details!
    for lifer in lifers:
        key = lifer.location_id
        if key not in lifers_by_location:
            lifers_by_location[key] = LocationToLifers()
            lifers_by_location[key].location = Location(
                lifer.location, lifer.latitude, lifer.longitude, lifer.location_id
            )
            lifers_by_location[key].lifers = [lifer]
        else:
            lifers_by_location[key].lifers.append(lifer)

    return lifers_by_location


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


csv_upload_cache: Dict[str, list[Lifer]] = {}


def get_lifers_from_cache(key: str) -> list[Lifer]:
    observations = csv_upload_cache.get(key, None)

    if not observations:
        raise Exception("No observations found for key", key)

    return observations


@app.post("/v1/upload_lifers_csv")
def upload_lifers_csv(file: UploadFile):
    print("Uploading file", file.filename)

    # turn the file into a pandas dataframe
    csv = parse_csv_from_file_to_lifers(file)
    uuid4_str = str(uuid4())

    csv_upload_cache[uuid4_str] = csv

    print(f"parsed csv with key {uuid4_str} and length {len(csv)}")

    return {"key": uuid4_str}
