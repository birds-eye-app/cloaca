from cloaca.parsing.parse_ebird_personal_export import parse_csv_from_file_to_lifers
from cloaca.types import set_lifers_to_cache


from fastapi import UploadFile


from uuid import uuid4


async def upload_lifers_csv(file: UploadFile):
    print("Uploading file", file.filename)

    # turn the file into a pandas dataframe
    csv = parse_csv_from_file_to_lifers(file)
    uuid4_str = str(uuid4())

    set_lifers_to_cache(uuid4_str, csv)

    print(f"parsed csv with key {uuid4_str} and length {len(csv)}")

    return {"key": uuid4_str}
