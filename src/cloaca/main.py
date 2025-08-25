import os
import time
from typing import Dict, List, Any

from cloaca.api.get_lifers_by_location import get_lifers_by_location
from cloaca.api.get_nearby_observations import (
    clear_nearby_observations_cache,
    get_nearby_observations,
)
from cloaca.api.get_new_lifers_by_region import (
    get_filtered_lifers_for_region,
    get_regional_mapping,
)
from cloaca.api.get_popular_hotspots import get_popular_hotspots_api

from cloaca.api.upload_lifers_csv import UploadLifersResponse, upload_lifers_csv
from cloaca.parsing.parsing_helpers import Lifer, LocationToLifers

from fastapi.middleware.cors import CORSMiddleware
from fastapi_utilities import repeat_every


from fastapi import FastAPI, Request, UploadFile


Cloaca_App = FastAPI()

Cloaca_App.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # todo: dont do this
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

is_dev = False
if env := os.getenv("ENVIRONMENT"):
    if env == "development":
        is_dev = True
    else:
        is_dev = False


@Cloaca_App.middleware("http")
async def add_process_time_header(request: Request, call_next):
    start_time = time.time()
    response = await call_next(request)
    excluded_paths = [
        "/v1/health",
    ]
    if request.url.path not in excluded_paths:
        print(
            "Time took to process the request and return response is {} sec".format(
                time.time() - start_time
            )
        )
    return response


@Cloaca_App.get("/v1/health")
def health_check() -> Dict[str, str]:
    return {"status": "SQUAWK"}


@Cloaca_App.get("/v1/nearby_observations")
async def get_nearby_observations_api(
    latitude: float, longitude: float, file_id: str
) -> Dict[str, LocationToLifers]:
    return await get_nearby_observations(latitude, longitude, file_id)


@Cloaca_App.get("/v1/lifers_by_location")
async def get_lifers_by_location_api(
    latitude: float, longitude: float, file_id: str
) -> Dict[str, LocationToLifers]:
    return await get_lifers_by_location(latitude, longitude, file_id)


@Cloaca_App.post("/v1/upload_lifers_csv")
async def upload_lifers_csv_api(file: UploadFile) -> UploadLifersResponse:
    return await upload_lifers_csv(file)


@Cloaca_App.get("/v1/regional_new_potential_lifers")
async def regional_lifers(
    latitude: float, longitude: float, file_id: str
) -> list[Lifer]:
    regional_lifers = await get_filtered_lifers_for_region(latitude, longitude, file_id)

    print("returning", len(regional_lifers), "regional lifers")

    return regional_lifers


@Cloaca_App.get("/v1/popular_hotspots")
async def get_popular_hotspots_endpoint(
    latitude: float, longitude: float, radius_km: float, month: int
) -> List[Dict[str, Any]]:
    return await get_popular_hotspots_api(latitude, longitude, radius_km, month)


# this is deprecated but I can't find another way to use the "repeat every" util without it
@Cloaca_App.on_event("startup")
@repeat_every(seconds=60 * 60 * 1)  # every hour
async def refresh_regional_lifers():
    # dont do this on startup in local dev to not spam ebird
    if is_dev:
        print("not refreshing regional lifers in dev mode")
        return
    print("refreshing regional lifers")
    await get_regional_mapping()


@Cloaca_App.on_event("startup")
@repeat_every(seconds=60 * 30 * 1)  # every 30 minutes
async def refresh_observations_cache():
    print("clearing nearby observations cache")
    await clear_nearby_observations_cache()
