from pydantic import BaseModel
from cloaca.parsing.parse_ebird_personal_export import parse_csv_from_file_to_lifers
from cloaca.parsing.parsing_helpers import HomeLocation
from cloaca.types import set_lifers_to_cache


from fastapi import UploadFile


from uuid import uuid4


class UploadLifersResponse(BaseModel):
    key: str
    home_location: HomeLocation | None = None


async def upload_lifers_csv(file: UploadFile):
    print("Uploading file", file.filename)

    # Parse the CSV to get lifers and home location
    lifers, home_location = parse_csv_from_file_to_lifers(file)

    uuid4_str = str(uuid4())

    set_lifers_to_cache(uuid4_str, lifers)

    print(f"parsed csv with key {uuid4_str} and length {len(lifers)}")

    response = UploadLifersResponse(key=uuid4_str, home_location=home_location)
    response.key = uuid4_str

    if home_location:
        print(
            f"Home location: {home_location.location_name} with {home_location.checklist_count} checklists"
        )
    else:
        print("No home location found")

    return response
