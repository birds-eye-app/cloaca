import datetime

import pandas as pd
from fastapi import UploadFile
from pydantic import BaseModel

from cloaca.achievements.engine import (
    compute_achievements,
    filter_since,
    summarize,
)
from cloaca.parsing.parse_ebird_personal_export import parse_csv_data_frame


class AchievementEvent(BaseModel):
    key: str
    type: str
    tier: str
    date: str
    title: str
    description: str
    species: str | None = None
    location: str | None = None
    context: dict


class AchievementsSummaryResponse(BaseModel):
    life_list_total: int
    total_checklists: int
    patch_totals: dict[str, int]
    region_totals: dict[str, int]
    biggest_day_date: str | None
    biggest_day_count: int | None
    longest_streak: int
    current_streak: int
    countries: int
    states: int
    events_by_tier: dict[str, int]


class AchievementsResponse(BaseModel):
    summary: AchievementsSummaryResponse
    recent_events: list[AchievementEvent]
    total_events: int


async def get_achievements(
    file: UploadFile, since: datetime.date | None = None
) -> AchievementsResponse:
    """Compute the achievement timeline for an uploaded MyEBirdData export.

    ``since`` bounds the returned events (default: last 30 days) so the
    response highlights what the newest export unlocked; the summary always
    covers the full history.
    """
    observations = parse_csv_data_frame(pd.read_csv(file.file))
    achievements = compute_achievements(observations)
    summary = summarize(observations, achievements)

    if since is None:
        since = datetime.date.today() - datetime.timedelta(days=30)
    recent = filter_since(achievements, since)

    return AchievementsResponse(
        summary=AchievementsSummaryResponse(
            life_list_total=summary.life_list_total,
            total_checklists=summary.total_checklists,
            patch_totals=summary.patch_totals,
            region_totals=summary.region_totals,
            biggest_day_date=summary.biggest_day[0] if summary.biggest_day else None,
            biggest_day_count=summary.biggest_day[1] if summary.biggest_day else None,
            longest_streak=summary.longest_streak,
            current_streak=summary.current_streak,
            countries=summary.countries,
            states=summary.states,
            events_by_tier=summary.events_by_tier,
        ),
        recent_events=[
            AchievementEvent(
                key=a.key,
                type=a.type.value,
                tier=a.tier.value,
                date=a.date,
                title=a.title,
                description=a.description,
                species=a.species,
                location=a.location,
                context=a.context,
            )
            for a in recent
        ],
        total_events=len(achievements),
    )
