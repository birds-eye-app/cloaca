"""Guards for the generated single-file build of the achievement engine.

The routine that emails reports runs `scripts/ebird_achievements_standalone.py`
directly, so it must stay in lockstep with the package.
"""

import csv
import importlib.util
import os
import pathlib
import subprocess
import sys

REPO = pathlib.Path(__file__).resolve().parents[2]
BUNDLE = REPO / "scripts" / "ebird_achievements_standalone.py"
BUILDER = REPO / "scripts" / "build_standalone_achievements.py"

HEADERS = [
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


def _load_builder():
    spec = importlib.util.spec_from_file_location("build_standalone", BUILDER)
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_checked_in_bundle_is_up_to_date():
    """Regenerating must be a no-op — otherwise someone edited the engine
    without running scripts/build_standalone_achievements.py."""
    expected = _load_builder().build()
    assert BUNDLE.read_text() == expected, (
        "scripts/ebird_achievements_standalone.py is stale — "
        "run: python3 scripts/build_standalone_achievements.py"
    )


def test_bundle_has_no_third_party_imports():
    imports = [
        line
        for line in BUNDLE.read_text().splitlines()
        if line.startswith(("import ", "from ")) and "__future__" not in line
    ]
    allowed = {"argparse", "csv", "datetime", "collections", "dataclasses", "enum"}
    for line in imports:
        module = line.split()[1].split(".")[0]
        assert module in allowed, f"non-stdlib import in bundle: {line}"


def _write_fixture(path: pathlib.Path) -> None:
    rows = [
        # (sub, common, sci, taxon, state, county, loc_id, loc, date)
        ("S1", "Snow Goose", "Anser caerulescens", 256, "US-NY", "Kings", "L2987624", "McGolrick Park", "2024-01-01"),
        ("S1", "Brant", "Branta bernicla", 296, "US-NY", "Kings", "L2987624", "McGolrick Park", "2024-01-01"),
        # spuh-only day: keeps the streak alive, earns no list events
        ("S2", "gull sp.", "Larus sp.", 900, "US-NY", "Kings", "L2987624", "McGolrick Park", "2024-01-02"),
        ("S3", "Osprey", "Pandion haliaetus", 400, "US-MA", "Dukes", "L784930", "Gay Head", "2024-01-03"),
        # subspecies form — must collapse to the species already seen
        ("S4", "Brant (Atlantic)", "Branta bernicla hrota", 297, "US-NY", "Kings", "L2987624", "McGolrick Park", "2025-06-01"),
    ]
    with open(path, "w", newline="", encoding="utf-8") as handle:
        writer = csv.writer(handle)
        writer.writerow(HEADERS)
        for sub, common, sci, taxon, state, county, loc_id, loc, date in rows:
            writer.writerow(
                [sub, common, sci, taxon, 1, state, county, loc_id, loc,
                 40.7, -73.9, date, "09:00 AM", "eBird - Traveling Count",
                 60, 1, 1.0, "", 1, "", "", "", ""]
            )


def test_bundle_output_matches_package_cli(tmp_path):
    fixture = tmp_path / "MyEBirdData.csv"
    _write_fixture(fixture)

    env = {**os.environ, "PYTHONPATH": str(REPO / "src")}
    package = subprocess.run(
        [sys.executable, "-m", "cloaca.achievements.cli", str(fixture), "--full"],
        capture_output=True, text=True, env=env, cwd=REPO,
    )
    standalone = subprocess.run(
        [sys.executable, str(BUNDLE), str(fixture), "--full"],
        capture_output=True, text=True, env=env, cwd=REPO,
    )

    assert package.returncode == 0, package.stderr
    assert standalone.returncode == 0, standalone.stderr
    assert standalone.stdout == package.stdout
    # sanity: the fixture actually exercised the engine
    assert "Lifer #1" in standalone.stdout
    assert "McGolrick Park" in standalone.stdout
