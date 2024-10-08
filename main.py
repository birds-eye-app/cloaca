from typing import Dict, Union

from fastapi import FastAPI
import os
from phoebe_bird import Phoebe
from phoebe_bird.types.data.observation import Observation
from phoebe_bird.types.data.observations.geo.recent_list_response import RecentListResponse
from fastapi.middleware.cors import CORSMiddleware

from scripts.parse_data import Lifer, lifers_to_json


app = FastAPI()

phoebe_client = Phoebe(
    api_key=os.environ.get("EBIRD_API_KEY"),
)

origins = [
    "http://localhost",
    "http://localhost:8000",
    "http://localhost:8000/"
]

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"], # todo: dont do this 
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

@app.get("/")
def read_root():
    return {"Hello": "World"}


# def convert_observation(ebird_observation: Observation):
#     return 

requests: Dict[str, RecentListResponse] = {}

def fetch_nearby_observations_from_ebird_with_cache(latitude: float, longitude: float): 
    key = f"{latitude}-{longitude}"
    cache_result = requests.get(key, None)
    if cache_result:
        print('hit cache!')
        return cache_result

    observations = phoebe_client.data.observations.geo.recent.list(lat=latitude, lng=longitude)
    print("Fetched this many obs:", len(observations))

    requests[key] = observations

    return observations

def filter_lifers_from_nearby_observations(nearby_observations: RecentListResponse, lifers_json: list[Lifer]) -> list[Observation]:
    lifer_commons_names = [lifer["common_name"] for lifer in lifers_json]

    unseen_observations: list[Observation] = list()
    already_seen_observations: list[Observation] = list()
    for observation in nearby_observations:
        if observation.com_name in lifer_commons_names:
            already_seen_observations.append(observation)
        else:
            unseen_observations.append(observation)
            
    print("Already seen obs:", len(already_seen_observations))
    print("Unseen obs:", len(unseen_observations))

    return unseen_observations

@app.get("/v1/nearby_observations")
def get_nearby_observations(latitude: float, longitude: float): 
    nearby_observations = fetch_nearby_observations_from_ebird_with_cache(round(latitude, 2), round(longitude, 2))

    lifers_from_csv = lifers_to_json()

    unseen_observations: list[Observation] = filter_lifers_from_nearby_observations(nearby_observations, lifers_from_csv)

    return unseen_observations
        