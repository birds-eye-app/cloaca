from pathlib import Path
from typing import List
import pandas as pd
from dataclasses import dataclass


@dataclass
class SubnationalRegion:
    country_code: str
    country_name: str
    subnational1_code: str
    subnational1_name: str


def parse_subnational1_file() -> List[SubnationalRegion]:
    base_path = Path(__file__).parent
    csv_path = (
        base_path
        / "data/eBird_regions_and_region_codes_18Apr2023/subnational1_regions_table.csv"
    )
    print(f"Reading subnational1 file from {csv_path}")
    df = pd.read_csv(csv_path)

    return [
        SubnationalRegion(
            country_code=row["country_code"],
            country_name=row["country_name"],
            subnational1_code=row["subnational1_code"],
            subnational1_name=row["subnational1_name"],
        )
        for _, row in df.iterrows()
    ]
