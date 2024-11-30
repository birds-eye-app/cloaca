# use historic obs API to fetch all observations
# fetch once per date

import asyncio
import calendar
import csv
from datetime import datetime
from typing import Optional

from pydantic import BaseModel, TypeAdapter, field_validator
from cloaca.phoebe_wrapper import get_phoebe_client
import pandas as pd


class eBirdHistoricFullObservation(BaseModel):
    speciesCode: str
    comName: str
    sciName: str
    locId: str
    locName: str
    obsDt: datetime
    howMany: Optional[float] = None
    lat: float
    lng: float
    obsValid: bool
    obsReviewed: bool
    locationPrivate: bool
    subId: str
    subnational2Code: str
    subnational2Name: str
    subnational1Code: str
    subnational1Name: str
    countryCode: str
    countryName: str
    userDisplayName: str
    obsId: str
    checklistId: str
    presenceNoted: bool
    hasComments: bool
    firstName: str
    lastName: str
    hasRichMedia: bool
    exoticCategory: Optional[str] = None

    @field_validator("howMany", mode="before")
    @classmethod
    def no_blank_strings(cls, v: float) -> Optional[float]:
        if isinstance(v, float):
            return v
        else:
            return None


ta = TypeAdapter(list[eBirdHistoricFullObservation])


def parse_historic_observation_csv() -> list[eBirdHistoricFullObservation]:
    with open("hotspot_observations.csv") as f:
        reader = csv.DictReader(f)
        return [eBirdHistoricFullObservation.model_validate(row) for row in reader]


def dump_historic_observation_csv(observations: list[eBirdHistoricFullObservation]):
    pd.DataFrame(ta.dump_python(observations)).to_csv(
        "hotspot_observations.csv", index=False
    )


def parse_historic_observations_json_text(
    text: str,
) -> list[eBirdHistoricFullObservation]:
    print(f"parsing {len(text)} bytes of json")
    return ta.validate_json(text)


async def backfill_hotspot_observations():
    year = 2024
    months = [i for i in range(1, 13)]

    parsed: list[eBirdHistoricFullObservation] = []

    for month in months:
        # we need to find the correct number of days in the month
        number_of_days = calendar.monthrange(year, month)[1]
        days = [i for i in range(1, number_of_days + 1)]
        for day in days:
            raw_response = await get_phoebe_client().data.with_raw_response.observations.recent.historic.list(
                region_code="US-NY",
                rank="create",
                y=year,
                m=month,
                d=day,
                cat="species",
                r=["L2987624"],
                detail="full",
            )

            daily_observations = parse_historic_observations_json_text(
                await raw_response.text()
            )
            print(f"Day {day} had {len(daily_observations)} observations")
            parsed.extend(daily_observations)

    print(f"Total observations: {len(parsed)}")

    dump_historic_observation_csv(parsed)


if __name__ == "__main__":
    loop = asyncio.get_event_loop()
    loop.run_until_complete(backfill_hotspot_observations())
