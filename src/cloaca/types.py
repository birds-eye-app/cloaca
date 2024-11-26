from typing import Dict
from cloaca.parsing.parsing_helpers import Lifer, Location, LocationToLifers
from phoebe_bird.types.data.observation import Observation as PhoebeObservation
from phoebe_bird.types.data.observations.geo.recent_list_response import (
    RecentListResponse,
)

csv_upload_cache: Dict[str, list[Lifer]] = {}


def set_lifers_to_cache(key: str, lifers: list[Lifer]):
    csv_upload_cache[key] = lifers


def get_lifers_from_cache(key: str) -> list[Lifer]:
    observations = csv_upload_cache.get(key, None)

    if not observations:
        raise Exception("No observations found for key", key)

    return observations


def filter_lifers_from_observations(
    observations: list[Lifer], lifers: list[Lifer]
) -> list[Lifer]:
    print(f"Filtering obs: {len(observations)}. Lifers: {len(lifers)}")

    lifer_sci_names = [lifer.scientific_name for lifer in lifers]

    print(f"sample lifer: {lifers[0]}")
    print(f"sample obs: {observations[0]}")

    unseen_observations: list[Lifer] = list()
    already_seen_observations: list[Lifer] = list()
    for observation in observations:
        if observation.scientific_name in lifer_sci_names:
            already_seen_observations.append(observation)
        else:
            unseen_observations.append(observation)

    print(
        f"Already seen obs: {len(already_seen_observations)}. Unseen obs: {len(unseen_observations)}"
    )

    return unseen_observations


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
        scientific_name=ebird_observation.sci_name or "",
        species_code=ebird_observation.species_code,
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
