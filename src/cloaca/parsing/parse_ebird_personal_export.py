from fastapi import UploadFile
import pandas as pd

from cloaca.parsing.parsing_helpers import (
    HomeLocation,
    Lifer,
    Observation,
    get_lifers,
    observations_to_lifers,
    calculate_home_location,
)

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


def parse_csv_from_file(file: UploadFile) -> list[Observation]:
    df = pd.read_csv(file.file)

    return parse_csv_data_frame(df)


def parse_csv_from_file_to_lifers(
    file: UploadFile,
) -> tuple[list[Lifer], HomeLocation | None]:
    df = pd.read_csv(file.file)

    observations = parse_csv_data_frame(df)

    lifers = get_lifers(observations)
    home_location = calculate_home_location(observations)

    return observations_to_lifers(lifers), home_location


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
