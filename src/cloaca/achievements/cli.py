"""Print an achievement report for a personal eBird export.

Usage:
    uv run python -m cloaca.achievements.cli path/to/MyEBirdData.csv
    uv run python -m cloaca.achievements.cli path/to/MyEBirdData.csv --since 2025-06-01
    uv run python -m cloaca.achievements.cli path/to/MyEBirdData.csv --full

By default, prints the trophy case plus everything unlocked in the last 30
days. ``--since`` changes the recency window (use the date of your previous
export to see exactly what the new one unlocked); ``--full`` prints the whole
timeline.
"""

import argparse
import datetime
from collections import defaultdict

import pandas as pd

from cloaca.achievements.engine import (
    Achievement,
    Tier,
    compute_achievements,
    filter_since,
    summarize,
)
from cloaca.parsing.parse_ebird_personal_export import parse_csv_data_frame


def _print_event(a: Achievement) -> None:
    print(f"  {a.date}  {a.emoji}  {a.title}")
    print(f"              {a.description}")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("csv_path", help="Path to MyEBirdData.csv")
    parser.add_argument(
        "--since",
        type=datetime.date.fromisoformat,
        default=None,
        help="Show events after this date (default: 30 days ago)",
    )
    parser.add_argument(
        "--full", action="store_true", help="Print the entire achievement timeline"
    )
    args = parser.parse_args()

    observations = parse_csv_data_frame(pd.read_csv(args.csv_path))
    achievements = compute_achievements(observations)
    summary = summarize(observations, achievements)

    print("=" * 72)
    print("🏆  ACHIEVEMENT REPORT")
    print("=" * 72)
    print(f"Life list: {summary.life_list_total} species")
    print(f"Checklists: {summary.total_checklists}")
    for name, total in summary.patch_totals.items():
        print(f"{name} patch list: {total} species")
    for name, total in summary.region_totals.items():
        print(f"{name} list: {total} species")
    if summary.biggest_day:
        date, count = summary.biggest_day
        print(f"Biggest day: {count} species on {date}")
    print(
        f"Streak: {summary.current_streak} days current · "
        f"{summary.longest_streak} longest"
    )
    print(f"Countries: {summary.countries} · States/provinces: {summary.states}")
    tiers = summary.events_by_tier
    print(
        f"Unlocked all-time: 🏆 {tiers.get('gold', 0)} gold · "
        f"✨ {tiers.get('silver', 0)} silver · 🌱 {tiers.get('bronze', 0)} bronze"
    )

    if args.full:
        print("\n" + "=" * 72)
        print("FULL TIMELINE")
        print("=" * 72)
        for a in achievements:
            _print_event(a)
        return

    since = args.since or (datetime.date.today() - datetime.timedelta(days=30))
    recent = filter_since(achievements, since)
    print("\n" + "=" * 72)
    print(f"🎉  UNLOCKED SINCE {since.isoformat()}  ({len(recent)} events)")
    print("=" * 72)
    if not recent:
        print("  Nothing new — time to go birding!")
        return

    by_tier: dict[Tier, list[Achievement]] = defaultdict(list)
    for a in recent:
        by_tier[a.tier].append(a)
    for tier in (Tier.GOLD, Tier.SILVER, Tier.BRONZE):
        events = by_tier.get(tier)
        if not events:
            continue
        print(f"\n--- {tier.value.upper()} ---")
        for a in events:
            _print_event(a)


if __name__ == "__main__":
    main()
