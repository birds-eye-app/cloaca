from fastapi.testclient import TestClient
import pandas as pd
from cloaca.parsing.parse_ebird_personal_export import parse_csv_data_frame
from cloaca.parsing.parsing_helpers import Lifer, get_lifers
from cloaca.types import get_lifers_from_cache
from cloaca.main import Cloaca_App

expected_singular_results = 596


def upload_test_csv():
    client = TestClient(Cloaca_App)
    files = {"file": open("tests/test_data/MyEBirdData.csv", "rb")}
    response = client.post("/v1/upload_lifers_csv", files=files)

    assert response.status_code == 200

    data = response.json()

    key = data["key"]

    return key


def test_parse_csv_from_file_route():
    client = TestClient(Cloaca_App)
    files = {"file": open("tests/test_data/MyEBirdData.csv", "rb")}
    response = client.post("/v1/upload_lifers_csv", files=files)

    assert response.status_code == 200

    data = response.json()

    key = data["key"]

    assert key

    cache_results = get_lifers_from_cache(key)

    assert cache_results
    assert len(cache_results) == 596

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

    all_observations = parse_csv_data_frame(df)

    assert len(all_observations) == 4663

    lifers = get_lifers(all_observations)

    assert len(lifers) == 596

    spuhs = [lifer for lifer in lifers if "sp." in lifer.scientific_name]

    assert len(spuhs) == 0

    unique_lifers = list(set([lifer.scientific_name for lifer in lifers]))

    assert len(unique_lifers) == 596

    species_not_in_lifers = set(
        [
            obs.common_name
            for obs in all_observations
            if obs.scientific_name not in unique_lifers
        ]
    )

    assert species_not_in_lifers == {
        "tern sp.",
        "Louisiana/Northern Waterthrush",
        "passerine sp.",
        "Turdus sp.",
        "Glossy/White-faced Ibis",
        "Western/Eastern Wood-Pewee",
        "swift sp.",
        "Tropical Royal Flycatcher (Northern)",
        "Lesser/Greater Yellowlegs",
        "Short-billed/Long-billed Dowitcher",
        "thrush sp.",
        "hawk sp.",
        "Downy/Hairy Woodpecker",
        "parrot sp.",
        "Empidonax sp.",
        "Olive-throated Parakeet (Aztec)",
        "Lesser Greenlet (Northern)",
        "Bay-breasted/Blackpoll Warbler",
        "new world warbler sp.",
        "crow sp.",
        "Yellow-billed Cacique (Prevost's)",
        "gull sp.",
    }
