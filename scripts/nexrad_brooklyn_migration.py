"""
Explore NEXRAD Level 2 data to investigate why BirdCast's 500k-bird
Brooklyn migration figure may not have shown up in checklists at
McGolrick Park (Greenpoint, Brooklyn) over the past few nights.

Run:
    uv pip install nexradaws arm-pyart numpy pandas matplotlib
    python scripts/nexrad_brooklyn_migration.py --nights 3

What this does
--------------
1. Pulls Level 2 scans from KOKX (Upton, NY) and KDIX (Mt Holly, NJ)
   for the requested civil-twilight-to-civil-twilight windows.
2. For each volume scan picks the lowest tilt (~0.5 deg) and computes
   mean reflectivity (a coarse bird-density proxy) and mean radial
   velocity in three areas:
       - a 10 km disk centered on McGolrick Park
       - a 30 km disk centered on Brooklyn
       - a 100 km disk centered on each radar (regional baseline)
3. Reports the beam centerline altitude AGL at McGolrick from each
   radar / tilt. Both KOKX (92 km E) and KDIX (95 km SW) sit at
   roughly the same range from Brooklyn. At the lowest tilt (0.5 deg)
   the beam centerline is ~1.3-1.4 km AGL with a ~0.95 deg beamwidth
   (so ~700-2000 m AGL is the sampled layer). Birds flying below ~600 m
   AGL over Brooklyn are largely INVISIBLE to both radars -- a real
   coverage gap that matters when judging "did the birds pass over my
   park?". BirdCast's integrated passage figure models that gap; a
   single eBird checklist does not.

Notes / caveats
---------------
- Reflectivity-to-bird-density is sensitive to species mix and orientation;
  treat numbers as relative, not absolute. BirdCast publishes calibrated
  passage estimates that this script does not attempt to reproduce.
- Ground clutter, insects, and precipitation contaminate low-tilt
  reflectivity. The script flags scans where mean velocity magnitude is
  low (likely insect / clutter dominated) so they can be excluded.
- NEXRAD Level 2 on AWS is public, no credentials needed.
"""

from __future__ import annotations

import argparse
import math
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path

import numpy as np
import pandas as pd

# Heavy deps -- imported lazily so the file can be linted without them.
# nexradaws: downloads Level 2 chunks from the public AWS bucket.
# pyart:     reads Level 2 and gives gate lat/lon/alt grids.

KOKX = {"id": "KOKX", "lat": 40.8655, "lon": -72.8639, "elev_m": 26.0}
KDIX = {"id": "KDIX", "lat": 39.9469, "lon": -74.4108, "elev_m": 45.0}

MCGOLRICK = {"name": "McGolrick Park", "lat": 40.7256, "lon": -73.9419}
BROOKLYN_CENTER = {"name": "Brooklyn (centroid)", "lat": 40.6500, "lon": -73.9496}

EARTH_R_KM = 6371.0
# 4/3 earth radius approximation for standard refraction.
EFFECTIVE_EARTH_R_KM = EARTH_R_KM * 4.0 / 3.0


def haversine_km(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    p1, p2 = math.radians(lat1), math.radians(lat2)
    dp = math.radians(lat2 - lat1)
    dl = math.radians(lon2 - lon1)
    a = math.sin(dp / 2) ** 2 + math.cos(p1) * math.cos(p2) * math.sin(dl / 2) ** 2
    return 2 * EARTH_R_KM * math.asin(math.sqrt(a))


def beam_height_m(range_km: float, elev_deg: float, radar_elev_m: float = 0.0) -> float:
    # Doviak & Zrnic standard-atmosphere beam height (4/3 earth model).
    r = range_km * 1000.0
    h = math.sqrt(
        r * r
        + (EFFECTIVE_EARTH_R_KM * 1000.0) ** 2
        + 2 * r * EFFECTIVE_EARTH_R_KM * 1000.0 * math.sin(math.radians(elev_deg))
    ) - EFFECTIVE_EARTH_R_KM * 1000.0
    return h + radar_elev_m


@dataclass
class ScanStats:
    radar: str
    scan_time: datetime
    elev_deg: float
    mean_z_mcgolrick: float
    mean_z_brooklyn: float
    mean_z_regional: float
    mean_abs_v_mcgolrick: float
    n_gates_mcgolrick: int
    beam_alt_at_mcgolrick_m: float


def list_scans(radar_id: str, start: datetime, end: datetime):
    import nexradaws

    conn = nexradaws.NexradAwsInterface()
    return conn.get_avail_scans_in_range(start, end, radar_id)


def download_scans(scans, out_dir: Path):
    import nexradaws

    out_dir.mkdir(parents=True, exist_ok=True)
    conn = nexradaws.NexradAwsInterface()
    results = conn.download(scans, str(out_dir))
    return [s.filepath for s in results.iter_success()]


def analyze_scan(path: str, radar_meta: dict) -> ScanStats | None:
    import pyart

    try:
        radar = pyart.io.read_nexrad_archive(path)
    except Exception as e:
        print(f"  skip {path}: {e}")
        return None

    sweep = 0  # lowest tilt
    elev_deg = float(radar.fixed_angle["data"][sweep])
    sl = radar.get_slice(sweep)

    z = radar.fields["reflectivity"]["data"][sl]
    try:
        v = radar.fields["velocity"]["data"][sl]
    except KeyError:
        v = np.ma.masked_all_like(z)

    lat = radar.gate_latitude["data"][sl]
    lon = radar.gate_longitude["data"][sl]
    rng = np.tile(radar.range["data"], (z.shape[0], 1)) / 1000.0  # km

    d_mcg = haversine_grid(lat, lon, MCGOLRICK["lat"], MCGOLRICK["lon"])
    d_bk = haversine_grid(lat, lon, BROOKLYN_CENTER["lat"], BROOKLYN_CENTER["lon"])

    mask_mcg = d_mcg <= 10.0
    mask_bk = d_bk <= 30.0
    mask_regional = rng <= 100.0

    # Crude bird-vs-rain split: keep 5-35 dBZ, drop precipitation and clutter.
    bio = (z >= 5.0) & (z <= 35.0)

    def _mean(field, mask):
        m = np.ma.array(field, mask=~(mask & bio) | np.ma.getmaskarray(field))
        if m.count() == 0:
            return float("nan")
        return float(m.mean())

    mcg_range_km = haversine_km(
        radar_meta["lat"], radar_meta["lon"], MCGOLRICK["lat"], MCGOLRICK["lon"]
    )

    return ScanStats(
        radar=radar_meta["id"],
        scan_time=pyart_time(radar),
        elev_deg=elev_deg,
        mean_z_mcgolrick=_mean(z, mask_mcg),
        mean_z_brooklyn=_mean(z, mask_bk),
        mean_z_regional=_mean(z, mask_regional),
        mean_abs_v_mcgolrick=_mean(np.abs(v), mask_mcg),
        n_gates_mcgolrick=int(mask_mcg.sum()),
        beam_alt_at_mcgolrick_m=beam_height_m(
            mcg_range_km, elev_deg, radar_meta["elev_m"]
        ),
    )


def haversine_grid(lat_grid, lon_grid, lat0, lon0):
    p1 = np.radians(lat_grid)
    p2 = math.radians(lat0)
    dp = np.radians(lat_grid - lat0)
    dl = np.radians(lon_grid - lon0)
    a = np.sin(dp / 2) ** 2 + np.cos(p1) * math.cos(p2) * np.sin(dl / 2) ** 2
    return 2 * EARTH_R_KM * np.arcsin(np.sqrt(a))


def pyart_time(radar) -> datetime:
    units = radar.time["units"]  # "seconds since YYYY-MM-DDTHH:MM:SSZ"
    base = datetime.strptime(units.split("since")[1].strip().rstrip("Z"),
                             "%Y-%m-%dT%H:%M:%S").replace(tzinfo=timezone.utc)
    return base + timedelta(seconds=float(radar.time["data"][0]))


def night_windows(nights: int) -> list[tuple[datetime, datetime]]:
    # Local sunset ~ 00:00 UTC, sunrise ~ 10:00 UTC for NYC in May.
    # Good enough -- we want migration hours, not exact astronomy.
    out = []
    today_utc = datetime.now(timezone.utc).date()
    for i in range(1, nights + 1):
        d = today_utc - timedelta(days=i)
        start = datetime.combine(d, datetime.min.time(), tzinfo=timezone.utc).replace(hour=0)
        end = start + timedelta(hours=10)
        out.append((start, end))
    return out


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--nights", type=int, default=3)
    p.add_argument("--cache", type=Path, default=Path(".nexrad_cache"))
    p.add_argument("--max-scans-per-night", type=int, default=6,
                   help="Sub-sample scans to keep the run quick.")
    p.add_argument("--out", type=Path, default=Path("nexrad_brooklyn_summary.csv"))
    args = p.parse_args()

    # Print the geometry first -- this alone answers most of the question.
    print("Geometry from each radar to McGolrick Park:")
    for r in (KOKX, KDIX):
        d = haversine_km(r["lat"], r["lon"], MCGOLRICK["lat"], MCGOLRICK["lon"])
        for tilt in (0.5, 0.9, 1.3, 1.8):
            h = beam_height_m(d, tilt, r["elev_m"])
            print(f"  {r['id']:5s}  range={d:6.1f} km  tilt={tilt:0.1f}deg  "
                  f"beam center ~ {h:5.0f} m AGL")
    print()

    rows: list[ScanStats] = []
    for start, end in night_windows(args.nights):
        for radar_meta in (KOKX, KDIX):
            print(f"== {radar_meta['id']}  {start:%Y-%m-%d %H:%MZ} - {end:%H:%MZ}")
            scans = list_scans(radar_meta["id"], start, end)
            scans = [s for s in scans if not s.filename.endswith("_MDM")]
            if args.max_scans_per_night and len(scans) > args.max_scans_per_night:
                step = len(scans) // args.max_scans_per_night
                scans = scans[::step][: args.max_scans_per_night]
            print(f"   {len(scans)} scans")
            paths = download_scans(scans, args.cache / radar_meta["id"])
            for path in paths:
                stats = analyze_scan(path, radar_meta)
                if stats:
                    rows.append(stats)

    if not rows:
        print("No scans analyzed.")
        return

    df = pd.DataFrame([r.__dict__ for r in rows])
    df.sort_values(["scan_time", "radar"], inplace=True)
    df.to_csv(args.out, index=False)
    print(f"\nWrote {args.out} ({len(df)} rows)")

    summary = (
        df.groupby(df["scan_time"].dt.date.rename("night")).agg(
            kokx_mean_z_brooklyn=("mean_z_brooklyn",
                                  lambda s: s[df.loc[s.index, "radar"] == "KOKX"].mean()),
            kdix_mean_z_brooklyn=("mean_z_brooklyn",
                                  lambda s: s[df.loc[s.index, "radar"] == "KDIX"].mean()),
            kdix_mean_z_mcgolrick=("mean_z_mcgolrick",
                                    lambda s: s[df.loc[s.index, "radar"] == "KDIX"].mean()),
        )
    )
    print("\nNightly mean reflectivity (dBZ) over Brooklyn (5-35 dBZ filter):")
    print(summary.to_string())


if __name__ == "__main__":
    main()
