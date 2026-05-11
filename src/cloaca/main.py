import asyncio
import os
import time
from typing import Dict, List, Any

import duckdb

from cloaca.api.bird_calls.get_audio_file import get_audio_file
from cloaca.api.bird_calls.get_bird_call import get_bird_call
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

from cloaca.db.db import get_db_connection_with_env


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
    print("Environment:", env)
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


# === EBD R2 DuckDB query endpoints (experimental) ============================
# Holds a single DuckDB connection ATTACHed to a sample EBD database on R2.
# Per-request handlers create child cursors so concurrent requests don't
# corrupt each other. Designed to validate cold/warm latency from Render's
# host before committing to building the full-scale .duckdb on R2.

_ebd_r2_con: duckdb.DuckDBPyConnection | None = None
_ebd_r2_attach_ms: int | None = None


@Cloaca_App.on_event("startup")
async def _attach_ebd_r2() -> None:
    """Open a long-lived DuckDB connection and ATTACH the R2 .duckdb file."""
    global _ebd_r2_con, _ebd_r2_attach_ms
    required = (
        "R2_ACCOUNT_ID",
        "R2_ACCESS_KEY_ID",
        "R2_SECRET_ACCESS_KEY",
        "R2_BUCKET",
    )
    if not all(os.environ.get(k) for k in required):
        print("[duck-query] R2 env vars missing; /v1/duck-query/* will 503")
        return
    release = os.environ.get("EBD_R2_NATIVE_RELEASE", "sample-30")
    bucket = os.environ["R2_BUCKET"]
    attach_path = f"r2://{bucket}/duckdb/{release}.duckdb"
    print(f"[duck-query] ATTACH {attach_path}")
    t0 = time.monotonic()
    try:
        con = duckdb.connect()
        con.execute("INSTALL httpfs")
        con.execute("LOAD httpfs")
        con.execute("SET http_timeout = 300000")
        con.execute("SET http_retries = 5")
        # Render Starter is 512 MB; bound DuckDB to leave room for Python/FastAPI.
        con.execute("PRAGMA memory_limit='256MB'")
        con.execute(
            """
            CREATE OR REPLACE SECRET ebd_r2 (
                TYPE r2,
                KEY_ID $key,
                SECRET $secret,
                ACCOUNT_ID $account
            )
            """,
            {
                "key": os.environ["R2_ACCESS_KEY_ID"],
                "secret": os.environ["R2_SECRET_ACCESS_KEY"],
                "account": os.environ["R2_ACCOUNT_ID"],
            },
        )
        con.execute(f"ATTACH '{attach_path}' AS ebd (READ_ONLY)")
        # Pre-warm catalog/metadata so the first user request doesn't pay it.
        con.execute("SELECT 1 FROM ebd.ebd LIMIT 1").fetchall()
        _ebd_r2_con = con
        _ebd_r2_attach_ms = int((time.monotonic() - t0) * 1000)
        print(f"[duck-query] ATTACH + prewarm done in {_ebd_r2_attach_ms} ms")
    except Exception as e:
        print(f"[duck-query] ATTACH failed: {e}")


def _ebd_run(query: str, params: Dict[str, Any] | None = None) -> tuple[list, int]:
    """Execute a query against the held DuckDB connection via a fresh cursor.
    Returns (rows, query_ms). Raises RuntimeError if not connected."""
    if _ebd_r2_con is None:
        raise RuntimeError("duck-query not connected")
    cur = _ebd_r2_con.cursor()
    t0 = time.monotonic()
    rows = cur.execute(query, params or {}).fetchall()
    return rows, int((time.monotonic() - t0) * 1000)


@Cloaca_App.get("/v1/duck-query/healthz")
async def duck_query_healthz() -> Dict[str, Any]:
    if _ebd_r2_con is None:
        return {"status": "disconnected"}
    try:
        rows, ms = await asyncio.to_thread(
            _ebd_run, "SELECT COUNT(*) AS n FROM ebd.ebd"
        )
        return {
            "status": "ok",
            "row_count": rows[0][0],
            "query_ms": ms,
            "attach_ms": _ebd_r2_attach_ms,
        }
    except Exception as e:
        return {"status": "error", "error": str(e)}


@Cloaca_App.get("/v1/duck-query/state-aggregate")
async def duck_query_state_aggregate(state_code: str) -> Dict[str, Any]:
    """Top 10 localities by distinct species for a given state. E.g. state_code=US-NY."""
    if _ebd_r2_con is None:
        return {"error": "duck-query disconnected"}
    country = state_code.split("-")[0] if "-" in state_code else state_code
    rows, ms = await asyncio.to_thread(
        _ebd_run,
        """
        SELECT locality, COUNT(DISTINCT scientific_name) AS species_count
        FROM ebd.ebd
        WHERE country_code = $country AND state_code = $state
          AND all_species_reported = TRUE
          AND approved = TRUE
          AND category NOT IN ('slash', 'spuh')
          AND (exotic_code IS NULL OR exotic_code = 'N')
        GROUP BY locality
        ORDER BY species_count DESC
        LIMIT 10
        """,
        {"country": country, "state": state_code},
    )
    return {
        "query_ms": ms,
        "rows": [{"locality": r[0], "species_count": r[1]} for r in rows],
    }


@Cloaca_App.get("/v1/duck-query/hotspot")
async def duck_query_hotspot(locality_id: str) -> Dict[str, Any]:
    """Aggregate stats for a single locality_id (eBird hotspot or personal location)."""
    if _ebd_r2_con is None:
        return {"error": "duck-query disconnected"}
    rows, ms = await asyncio.to_thread(
        _ebd_run,
        """
        SELECT
            COUNT(*) AS rows,
            COUNT(DISTINCT scientific_name) AS species,
            COUNT(DISTINCT sampling_event_identifier) AS checklists,
            MIN(observation_date) AS earliest,
            MAX(observation_date) AS latest
        FROM ebd.ebd
        WHERE locality_id = $locality
        """,
        {"locality": locality_id},
    )
    if not rows:
        return {"query_ms": ms, "rows": 0}
    r = rows[0]
    return {
        "query_ms": ms,
        "rows": r[0],
        "species": r[1],
        "checklists": r[2],
        "earliest": str(r[3]) if r[3] else None,
        "latest": str(r[4]) if r[4] else None,
    }


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


@Cloaca_App.get("/v1/bird_calls/call")
async def get_bird_call_api(location_code: str | None, request: Request) -> str:
    domain = str(request.base_url)
    path = "v1/bird_calls/audio_file"
    return await get_bird_call(location_code, str(domain) + path)


@Cloaca_App.get("/v1/bird_calls/audio_file")
async def get_bird_call_audio_file(location_code: str | None):
    return await get_audio_file(location_code)


@Cloaca_App.get("/v1/regional_new_potential_lifers")
async def regional_lifers(
    latitude: float, longitude: float, file_id: str
) -> list[Lifer]:
    if is_dev:
        print("not fetching regional lifers in dev mode")
        return []
    regional_lifers = await get_filtered_lifers_for_region(latitude, longitude, file_id)

    print("returning", len(regional_lifers), "regional lifers")

    return regional_lifers


def get_duck_db_conn_from_state() -> duckdb.DuckDBPyConnection:
    return Cloaca_App.state.duck_db_conn


@Cloaca_App.get("/v1/popular_hotspots")
async def get_popular_hotspots_endpoint(
    latitude: float, longitude: float, radius_km: float, month: int
) -> List[Dict[str, Any]]:
    return await get_popular_hotspots_api(
        get_duck_db_conn_from_state(), latitude, longitude, radius_km, month
    )


# this is deprecated but I can't find another way to use the "repeat every" util without it
_piper_task: asyncio.Task | None = None


@Cloaca_App.on_event("startup")
async def start_piper():
    global _piper_task
    token = os.getenv("PIPER_DISCORD_BOT_TOKEN")
    if not token:
        print("PIPER_DISCORD_BOT_TOKEN not set, skipping piper")
        return
    from cloaca.piper.main import start as piper_start

    async def _run_piper():
        delay = 5
        while True:
            try:
                await piper_start()
            except asyncio.CancelledError:
                return
            except Exception as e:
                print(f"piper crashed: {e}, restarting in {delay}s")
                await asyncio.sleep(delay)
                delay = min(delay * 2, 300)
            else:
                return

    _piper_task = asyncio.create_task(_run_piper())
    print("piper started")


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


@Cloaca_App.on_event("startup")
@repeat_every(seconds=60 * 30 * 1)  # every 30 minutes
async def connect_to_duck_db():
    print("Connecting to DuckDB at startup...")
    duck_db_conn = get_db_connection_with_env()
    print("Connected to DuckDB at startup")
    Cloaca_App.state.duck_db_conn = duck_db_conn


@Cloaca_App.on_event("shutdown")
async def shutdown_event():
    if Cloaca_App.state.duck_db_conn:
        try:
            Cloaca_App.state.duck_db_conn.close()
            print("DuckDB connection closed.")
        except Exception as e:
            print("Error closing DuckDB connection:", e)
    if _piper_task is not None:
        from cloaca.piper.main import bot
        from cloaca.piper.bird_query import close_duck_conn

        if not bot.is_closed():
            await bot.close()
        _piper_task.cancel()
        try:
            await _piper_task
        except asyncio.CancelledError:
            pass
        await close_duck_conn()
        from cloaca.piper.db_pool import close_engine

        await close_engine()
        print("piper stopped")
