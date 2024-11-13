from fastapi import UploadFile
import pandas as pd
from dataclasses import dataclass

# Define the CSV headers
expected_headers = [
    "Submission ID",
    "Common Name",
    "Scientific Name",
    "Taxonomic Order",
    "Count",
    "State/Province",
    "County",
    "Location ID",
    "Location",
    "Latitude",
    "Longitude",
    "Date",
    "Time",
    "Protocol",
    "Duration (Min)",
    "All Obs Reported",
    "Distance Traveled (km)",
    "Area Covered (ha)",
    "Number of Observers",
    "Breeding Code",
    "Observation Details",
    "Checklist Comments",
    "ML Catalog Numbers",
]


# Define a data class for each row
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


def parse_csv_from_file(file: UploadFile) -> list[Observation]:
    df = pd.read_csv(file.file)

    return parse_csv_data_frame(df)


def parse_csv_from_file_to_lifers(file: UploadFile):
    df = pd.read_csv(file.file)

    observations = parse_csv_data_frame(df)

    lifers = get_lifers(observations)

    return observations_to_lifers(lifers)


def parse_csv_data_frame(data_frame: pd.DataFrame) -> list[Observation]:
    # Check that the headers match the expected headers
    if list(data_frame.columns) != expected_headers:
        raise ValueError("The CSV headers do not match the expected headers.")

    # Sort the dataframe by Date and Time columns
    data_frame.sort_values(by=["Date", "Time"], inplace=True)

    # Convert each row into an Observation data class
    observations = [
        Observation(
            submission_id=row["Submission ID"],
            common_name=row["Common Name"],
            scientific_name=row["Scientific Name"],
            taxonomic_order=row["Taxonomic Order"],
            count=row["Count"],
            state_province=row["State/Province"],
            county=row["County"],
            location_id=row["Location ID"],
            location=row["Location"],
            latitude=row["Latitude"],
            longitude=row["Longitude"],
            date=row["Date"],
            time=row["Time"],
            protocol=row["Protocol"],
            duration_min=row["Duration (Min)"],
            all_obs_reported=row["All Obs Reported"],
            distance_traveled_km=row["Distance Traveled (km)"],
            area_covered_ha=row["Area Covered (ha)"],
            number_of_observers=row["Number of Observers"],
            breeding_code=row["Breeding Code"],
            observation_details=row["Observation Details"],
            checklist_comments=row["Checklist Comments"],
            ml_catalog_numbers=row["ML Catalog Numbers"],
        )
        for _, row in data_frame.iterrows()
    ]
    return observations


# Get lifers (first observation of each species)
def get_lifers(observations):
    lifers: dict[str, Observation] = {}
    for obs in observations:
        scientific_name = obs.scientific_name
        # don't include spuh observations (eg `gull sp.`)
        if "sp." in scientific_name or "sp." in obs.common_name:
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
    species_code: str


@dataclass
class Location:
    location_name: str
    latitude: float
    longitude: float
    location_id: str


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
            species_code=obs.scientific_name,
        )
        for obs in observations
    ]
