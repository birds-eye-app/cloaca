from fastapi.testclient import TestClient
import pandas as pd
from cloaca.parse import Lifer, get_lifers, parse_csv_data_frame
from cloaca.types import get_lifers_from_cache
from main import app


def test_parse_csv_from_file_route():
    client = TestClient(app)
    files = {"file": open("tests/test_data/MyEBirdData.csv", "rb")}
    response = client.post("/v1/upload_lifers_csv", files=files)

    assert response.status_code == 200

    data = response.json()

    key = data["key"]

    assert key

    cache_results = get_lifers_from_cache(key)

    assert cache_results
    assert len(cache_results) == 607

    first_lifer = cache_results[0]

    print(first_lifer)

    expected = Lifer(
        common_name="Northern Harrier",
        latitude=29.819019,
        longitude=-89.61216,
        date="2022-11-21",
        taxonomic_order=8228,
        location="Hopedale",
        location_id="L21909958",
        species_code="Circus hudsonius",
    )

    assert first_lifer == expected


def test_observations_to_lifers():
    df = pd.read_csv("tests/test_data/MyEBirdData.csv")

    observations = parse_csv_data_frame(df)

    assert len(observations) == 4663

    lifers = get_lifers(observations)

    assert len(lifers) == 607

    spuhs = [lifer for lifer in lifers if "sp." in lifer.scientific_name]

    assert len(spuhs) == 0
