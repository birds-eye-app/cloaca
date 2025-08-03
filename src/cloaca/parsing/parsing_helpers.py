from dataclasses import dataclass


@dataclass
class Observation:
    submission_id: str
    common_name: str
    scientific_name: str
    taxonomic_order: int
    count: int
    state_province: str
    county: str
    location_id: str
    location: str
    latitude: float
    longitude: float
    date: str
    time: str
    protocol: str
    duration_min: int
    all_obs_reported: str
    distance_traveled_km: float
    area_covered_ha: float
    number_of_observers: int
    breeding_code: str
    observation_details: str
    checklist_comments: str
    ml_catalog_numbers: str


# remove:
# spuh (eg `gull sp.`)
# combo species (eg `long-billed/short-billed dowitcher`)
# hybrid species (eg `Mallard x American Black Duck`)
def is_singular_bird_species(observation: Observation) -> bool:
    scientific_name = observation.scientific_name
    common_name = observation.common_name

    if "sp." in scientific_name or "sp." in common_name:
        return False

    if "/" in scientific_name or "/" in common_name:
        return False

    if " x " in scientific_name or " x " in common_name:
        return False

    return True


# Get lifers (first observation of each species)
def get_lifers(observations):
    lifers: dict[str, Observation] = {}
    for obs in observations:
        if not is_singular_bird_species(obs):
            print(
                f"removing unwanted observation {obs.common_name}({obs.scientific_name})"
            )
            continue

        if obs.scientific_name not in lifers:
            lifers[obs.scientific_name] = obs
    return list(lifers.values())


@dataclass
class Lifer:
    common_name: str
    latitude: float
    longitude: float
    date: str
    taxonomic_order: int
    location: str
    location_id: str
    scientific_name: str
    species_code: str | None = None


@dataclass
class Location:
    location_name: str
    latitude: float
    longitude: float
    location_id: str


@dataclass
class LocationToLifers:
    location: Location
    lifers: list[Lifer]


def observations_to_lifers(observations: list[Observation]) -> list[Lifer]:
    return [
        Lifer(
            common_name=obs.common_name,
            latitude=obs.latitude,
            longitude=obs.longitude,
            date=obs.date,
            taxonomic_order=obs.taxonomic_order,
            location=obs.location,
            location_id=obs.location_id,
            scientific_name=obs.scientific_name,
        )
        for obs in observations
    ]


@dataclass
class HomeLocation:
    location_id: str
    location_name: str
    latitude: float
    longitude: float
    checklist_count: int


def calculate_home_location(observations: list[Observation]) -> HomeLocation | None:
    """Calculate home location as the hotspot with the most checklists"""
    if not observations:
        return None

    # Group by location and count unique submission IDs (checklists)
    location_checklist_counts: dict[str, dict] = {}

    for obs in observations:
        if obs.location_id not in location_checklist_counts:
            location_checklist_counts[obs.location_id] = {
                "location_name": obs.location,
                "latitude": obs.latitude,
                "longitude": obs.longitude,
                "submission_ids": set(),
            }

        location_checklist_counts[obs.location_id]["submission_ids"].add(
            obs.submission_id
        )

    # Find location with most checklists
    max_checklists = 0
    home_location_data = None

    for location_id, data in location_checklist_counts.items():
        checklist_count = len(data["submission_ids"])
        if checklist_count > max_checklists:
            max_checklists = checklist_count
            home_location_data = {
                "location_id": location_id,
                "location_name": data["location_name"],
                "latitude": data["latitude"],
                "longitude": data["longitude"],
                "checklist_count": checklist_count,
            }

    if home_location_data:
        return HomeLocation(**home_location_data)

    return None
