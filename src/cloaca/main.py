import time

from cloaca.api.get_lifers_by_location import get_lifers_by_location
from cloaca.api.get_nearby_observations import get_nearby_observations
from cloaca.api.get_new_lifers_by_region import (
    get_filtered_lifers_for_region,
)

from cloaca.api.upload_lifers_csv import upload_lifers_csv
from cloaca.parsing.parsing_helpers import Lifer

from fastapi.middleware.cors import CORSMiddleware
from fastapi import FastAPI, Request, UploadFile

Cloaca_App = FastAPI()

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
async def get_nearby_observations_api(latitude: float, longitude: float, file_id: str):
    return get_nearby_observations(latitude, longitude, file_id)


@Cloaca_App.get("/v1/lifers_by_location")
def get_lifers_by_location_api(latitude: float, longitude: float, file_id: str):
    return get_lifers_by_location(latitude, longitude, file_id)


@Cloaca_App.post("/v1/upload_lifers_csv")
def upload_lifers_csv_api(file: UploadFile):
    return upload_lifers_csv(file)


@Cloaca_App.get("/v1/regional_new_potential_lifers")
async def regional_lifers(
    latitude: float, longitude: float, file_id: str
) -> list[Lifer]:
    regional_lifers = await get_filtered_lifers_for_region(latitude, longitude, file_id)

    print("returning", len(regional_lifers), "regional lifers")

    return regional_lifers
