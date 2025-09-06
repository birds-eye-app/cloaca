import time
from urllib.parse import urlencode

from fastapi import Response
from cloaca.api.bird_calls.main import MCGOLRICK_PARK_HOTSPOT_ID, run_bird_calls_job

from twilio.twiml.voice_response import VoiceResponse

last_refresh_times = {}


async def get_bird_call(location_code: str | None, url: str) -> Response:
    if location_code is None:
        location_code = MCGOLRICK_PARK_HOTSPOT_ID

    last_refresh_time = last_refresh_times.get(location_code, 0)
    current_time = time.time()
    # Refresh if more than 15 minutes have passed since last refresh
    if current_time - last_refresh_time > 15 * 60:
        print("Refreshing bird call for location:", location_code)
        await run_bird_calls_job(location_code)
        last_refresh_times[location_code] = current_time
    else:
        print("Using cached bird call for location:", location_code)

    query_params = {"location_code": location_code}

    encoded_params = urlencode(query_params)
    complete_url = f"{url}?{encoded_params}"
    print("Sending Twilio to URL:", complete_url)

    response = VoiceResponse()
    response.play(complete_url)

    return Response(content=str(response), media_type="application/xml")
