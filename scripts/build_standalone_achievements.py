#!/usr/bin/env python3
"""Generate a single-file, stdlib-only build of the achievement engine.

The routine that emails achievement reports runs in whatever session the
scheduler gives it — which may not have this repo checked out, may not have
`uv`, and may not have pandas. Rather than teach that session to fetch the
repo (which needs repository permissions it often lacks), we ship one
self-contained script it can run with plain `python3`.

The bundle is GENERATED from the real modules rather than hand-copied, so it
cannot drift from the engine. Regenerate with:

    python3 scripts/build_standalone_achievements.py

`tests/achievements/test_standalone.py` fails if the checked-in bundle is
stale, so CI catches a forgotten regeneration.
"""

import ast
import pathlib

REPO = pathlib.Path(__file__).resolve().parent.parent
SRC = REPO / "src" / "cloaca"
OUT = REPO / "scripts" / "ebird_achievements_standalone.py"

# Modules inlined in order; their `from cloaca...` imports are dropped since
# everything ends up in one namespace.
MODULES = [
    SRC / "parsing" / "parsing_helpers.py",
    SRC / "achievements" / "config.py",
    SRC / "achievements" / "engine.py",
]


def split_module(path: pathlib.Path) -> tuple[list[str], str]:
    """Return (stdlib import statements, body with imports/docstring removed)."""
    source = path.read_text()
    tree = ast.parse(source)
    lines = source.splitlines()
    drop: set[int] = set()
    imports: list[str] = []

    for index, node in enumerate(tree.body):
        # Drop the module docstring — the bundle carries its own header.
        if (
            index == 0
            and isinstance(node, ast.Expr)
            and isinstance(node.value, ast.Constant)
            and isinstance(node.value.value, str)
        ):
            drop.update(range(node.lineno - 1, (node.end_lineno or node.lineno)))
            continue

        if not isinstance(node, (ast.Import, ast.ImportFrom)):
            continue

        drop.update(range(node.lineno - 1, (node.end_lineno or node.lineno)))
        module = getattr(node, "module", "") or ""
        # cloaca imports resolve within the bundle; __future__ is hoisted.
        if module.startswith("cloaca") or module == "__future__":
            continue
        segment = ast.get_source_segment(source, node)
        if segment:
            imports.append(segment)

    body = "\n".join(line for i, line in enumerate(lines) if i not in drop)
    return imports, body.strip("\n")


def extract_assignment(path: pathlib.Path, name: str) -> str:
    """Pull a single top-level assignment (by name) out of a module."""
    source = path.read_text()
    tree = ast.parse(source)
    for node in tree.body:
        targets = getattr(node, "targets", [])
        if any(isinstance(t, ast.Name) and t.id == name for t in targets):
            segment = ast.get_source_segment(source, node)
            if segment:
                return segment
    raise SystemExit(f"could not find {name} in {path}")


HEADER = '''#!/usr/bin/env python3
"""eBird achievement engine — single-file, stdlib-only build.

GENERATED FILE — do not edit by hand. Regenerate with:
    python3 scripts/build_standalone_achievements.py

Runs anywhere Python 3.11+ exists, with no repo checkout, no virtualenv, and
no third-party packages:

    python3 ebird_achievements_standalone.py MyEBirdData.csv --since 2026-08-13

Output is byte-identical to `python -m cloaca.achievements.cli`.
"""
'''

READER_AND_CLI = '''

# ---------------------------------------------------------------------------
# CSV reading (stdlib csv in place of pandas)
# ---------------------------------------------------------------------------


def read_observations(csv_path: str) -> list[Observation]:
    with open(csv_path, newline="", encoding="utf-8") as handle:
        reader = csv.DictReader(handle)
        if reader.fieldnames != EXPECTED_HEADERS:
            raise ValueError("The CSV headers do not match the expected headers.")
        rows = list(reader)

    # Mirror the pandas path, which sorts by Date then Time before parsing.
    rows.sort(key=lambda r: (r["Date"] or "", r["Time"] or ""))

    def to_int(value: str) -> int:
        try:
            return int(value)
        except (TypeError, ValueError):
            return 0

    def to_float(value: str) -> float:
        try:
            return float(value)
        except (TypeError, ValueError):
            return 0.0

    return [
        Observation(
            submission_id=row["Submission ID"],
            common_name=row["Common Name"],
            scientific_name=row["Scientific Name"],
            # int, matching pandas — the sort tiebreaker depends on it
            taxonomic_order=to_int(row["Taxonomic Order"]),
            count=row["Count"],
            state_province=row["State/Province"],
            county=row["County"],
            location_id=row["Location ID"],
            location=row["Location"],
            latitude=to_float(row["Latitude"]),
            longitude=to_float(row["Longitude"]),
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
        for row in rows
    ]


# ---------------------------------------------------------------------------
# CLI (mirrors cloaca.achievements.cli)
# ---------------------------------------------------------------------------


def _print_event(a: Achievement) -> None:
    print(f"  {a.date}  {a.emoji}  {a.title}")
    print(f"              {a.description}")


def main() -> None:
    parser = argparse.ArgumentParser(description="eBird achievement report")
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

    observations = read_observations(args.csv_path)
    achievements = compute_achievements(observations)
    summary = summarize(observations, achievements)

    print("=" * 72)
    print("\\U0001f3c6  ACHIEVEMENT REPORT")
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
        f"Streak: {summary.current_streak} days current \\u00b7 "
        f"{summary.longest_streak} longest"
    )
    print(f"Countries: {summary.countries} \\u00b7 States/provinces: {summary.states}")
    tiers = summary.events_by_tier
    print(
        f"Unlocked all-time: \\U0001f3c6 {tiers.get('gold', 0)} gold \\u00b7 "
        f"\\u2728 {tiers.get('silver', 0)} silver \\u00b7 "
        f"\\U0001f331 {tiers.get('bronze', 0)} bronze"
    )

    if args.full:
        print("\\n" + "=" * 72)
        print("FULL TIMELINE")
        print("=" * 72)
        for a in achievements:
            _print_event(a)
        return

    since = args.since or (datetime.date.today() - datetime.timedelta(days=30))
    recent = filter_since(achievements, since)
    print("\\n" + "=" * 72)
    print(f"\\U0001f389  UNLOCKED SINCE {since.isoformat()}  ({len(recent)} events)")
    print("=" * 72)
    if not recent:
        print("  Nothing new \\u2014 time to go birding!")
        return

    by_tier: dict[Tier, list[Achievement]] = defaultdict(list)
    for a in recent:
        by_tier[a.tier].append(a)
    for tier in (Tier.GOLD, Tier.SILVER, Tier.BRONZE):
        events = by_tier.get(tier)
        if not events:
            continue
        print(f"\\n--- {tier.value.upper()} ---")
        for a in events:
            _print_event(a)


if __name__ == "__main__":
    main()
'''


def build() -> str:
    all_imports: list[str] = ["import argparse", "import csv"]
    bodies: list[str] = []

    for path in MODULES:
        imports, body = split_module(path)
        all_imports.extend(imports)
        bodies.append(f"# --- from {path.relative_to(REPO)} ---\n\n{body}")

    # Preserve first-seen order while de-duplicating.
    seen: set[str] = set()
    unique_imports = [i for i in all_imports if not (i in seen or seen.add(i))]

    headers = extract_assignment(
        SRC / "parsing" / "parse_ebird_personal_export.py", "expected_headers"
    ).replace("expected_headers", "EXPECTED_HEADERS", 1)

    parts = [
        HEADER,
        "from __future__ import annotations",
        "",
        "\n".join(sorted(unique_imports)),
        "",
        "# Expected MyEBirdData.csv columns",
        headers,
        "",
        "\n\n\n".join(bodies),
        READER_AND_CLI,
    ]
    return "\n".join(parts).rstrip("\n") + "\n"


if __name__ == "__main__":
    OUT.write_text(build())
    print(f"wrote {OUT.relative_to(REPO)} ({len(OUT.read_text().splitlines())} lines)")
