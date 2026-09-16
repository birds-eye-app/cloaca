"""Big Days — the biggest single-day species counts in eBird per country / state / county,
served to the personal site's /big-days page (beak-v2, src/big-days/).

The data is two Parquet files derived from the eBird Basic Dataset by an external build job
and placed in BIG_DAYS_DIR by the deployment; this process reads local files only:

  big_days.parquet         one row per (level, region, observation_date, party):
                           level ∈ country|state|county, region = the eBird code, year, month,
                           observer_id (pseudonymous, e.g. obsr59592), n_species, n_checklists,
                           n_localities, observers (max party size on the day's lists),
                           party_size + members (eBirders sharing exactly these checklists),
                           minutes, km, all_complete, checklists = list of structs
                           (id, locality, locality_id, hotspot, lat, lon, time, minutes, km,
                           n_species, complete, protocol) in start-time order.
                           Sorted by (level, region, n_species desc) and pruned upstream to the
                           top 50 within (region, year), (region, year, month) and the solo
                           variants, so every filter combination offered here is exact.
  big_day_regions.parquet  one row per region: level, region, name, parent, country_code,
                           days, checklists, best, best_date, first_year, last_year, lat, lon,
                           years = list of {year, days, observers, best, best_date}.

Observer ids never leave this module: `observer_id` and `members` exist in the files (they
key the rows and order ties) but no response carries them — a row shows `observers` (people
in the field) and `party_size` (accounts sharing the checklists) instead.

Fixed, parameterised queries only; region codes are regex-checked. The files may be absent
(new box, or a release not built yet): every endpoint answers 503 until they appear, and the
loader re-checks on cloaca's periodic timer so a new release is picked up without a restart.
"""

import math
import os
import re
import threading
from functools import lru_cache
from typing import Any, Dict, List, Optional

import duckdb
from fastapi import HTTPException

K = 50
REGION_RE = re.compile(r"^[A-Z]{2}(-[A-Z0-9]{1,8}){0,2}$")
REGION_COLS = (
    "level, region AS code, name, parent, country_code, days, checklists, best, "
    "best_date, first_year, last_year, lat, lon"
)
ORDER = "n_species DESC, n_checklists ASC, observation_date ASC, observer_id ASC"
# One person's day cannot hold more than 24 hours of birding. Accounts that upload many
# people's checklists (a club on Global Big Day: 371 lists, 155 h, 2021-05-08) otherwise top
# every board they touch. Days with no durations at all are kept (mostly historical).
PLAUSIBLE = "(minutes IS NULL OR minutes <= 1440)"
# Shared accounts — one eBird account used by people in different places at once (a club or
# tour company on a Big Day) — are hidden by default (`include_shared=false`). The evidence is
# in the day's own checklists: two lists that ran concurrently for >= SHARED_MIN_OVERLAP minutes
# while > SHARED_SLACK_KM apart (allowing the first list's traveled distance, since a traveling
# list's coordinates are its start). A day is "shared" at >= SHARED_PAIRS such pairs; one pair
# is treated as a time slip. Verified region by region 2026-09-16: New York's 2024-05-18 "party
# of 4" has 9 pairs (a 3h41m list in Sullivan County running while lists were filed 80-100 km
# away in Dutchess); every Kings County day and every ambitious solo day has 0. Speed between
# consecutive stops was NOT usable alone: minute-rounded times and hotspot-centroid pins put one
# "impossible" hop on most honest days, including single-van team runs.
SHARED_SLACK_KM = 5.0
SHARED_MIN_OVERLAP = 10
SHARED_PAIRS = 2


def _mins(t: str) -> int:
    h, m = t.split(":")[:2]
    return int(h) * 60 + int(m)


def _km(a_lat, a_lon, b_lat, b_lon) -> float:
    la1, lo1, la2, lo2 = map(math.radians, (a_lat, a_lon, b_lat, b_lon))
    h = (
        math.sin((la2 - la1) / 2) ** 2
        + math.cos(la1) * math.cos(la2) * math.sin((lo2 - lo1) / 2) ** 2
    )
    return 2 * 6371.0 * math.asin(math.sqrt(h))


def shared_pairs(checklists) -> int:
    """Pairs of the day's checklists that ran concurrently while far apart (see SHARED_*)."""
    cls = [
        c
        for c in checklists
        if c.get("lat") is not None and c.get("lon") is not None and c.get("time")
    ]
    cls.sort(key=lambda c: _mins(c["time"]))
    n = 0
    for i, a in enumerate(cls):
        a0 = _mins(a["time"])
        a1 = a0 + (a.get("minutes") or 0)
        for b in cls[i + 1 :]:
            b0 = _mins(b["time"])
            if b0 >= a1:
                break  # sorted by start: nothing later overlaps a
            b1 = b0 + (b.get("minutes") or 0)
            if min(a1, b1) - b0 < SHARED_MIN_OVERLAP:
                continue
            if (
                _km(float(a["lat"]), float(a["lon"]), float(b["lat"]), float(b["lon"]))
                - float(a.get("km") or 0.0)
                > SHARED_SLACK_KM
            ):
                n += 1
    return n


def annotate(row) -> dict:
    """Add shared_pairs / shared to a leaderboard row (in place) and return it."""
    row["shared_pairs"] = shared_pairs(row["checklists"])
    row["shared"] = row["shared_pairs"] >= SHARED_PAIRS
    return row


class BigDays:
    """One DuckDB connection over the two files, reloaded when they change."""

    def __init__(self, directory: Optional[str] = None):
        self.dir = directory or os.environ.get("BIG_DAYS_DIR", "/var/data/big-days")
        self._con: Optional[duckdb.DuckDBPyConnection] = None
        self._lock = threading.Lock()
        self._mtimes: tuple = ()
        self.meta: Dict[str, Any] = {"ready": False}
        self._region_row = lru_cache(maxsize=4096)(self._region_row_uncached)
        self._children = lru_cache(maxsize=4096)(self._children_uncached)
        self._leaderboard = lru_cache(maxsize=2048)(self._leaderboard_uncached)

    # --- loading ------------------------------------------------------------------------
    def paths(self):
        return (
            os.path.join(self.dir, "big_days.parquet"),
            os.path.join(self.dir, "big_day_regions.parquet"),
        )

    def reload_if_changed(self) -> bool:
        """(Re)open the files if they appeared or changed. Returns whether data is ready."""
        days, regions = self.paths()
        if not (os.path.exists(days) and os.path.exists(regions)):
            return self._con is not None
        mtimes = (os.path.getmtime(days), os.path.getmtime(regions))
        if self._con is not None and mtimes == self._mtimes:
            return True
        con = duckdb.connect()
        con.execute("SET memory_limit='256MB'; SET threads=4;")
        con.execute(f"CREATE VIEW big_days AS SELECT * FROM read_parquet('{days}')")
        con.execute(f"CREATE TABLE regions AS SELECT * FROM read_parquet('{regions}')")
        # The regions file's `best` was computed before the PLAUSIBLE rule; recompute it from
        # the days that pass. One scan of four narrow columns at load time.
        con.execute(f"""
            CREATE TABLE region_best AS
            SELECT level, region, max(n_species) AS best, arg_max(observation_date, n_species) AS best_date
            FROM big_days WHERE {PLAUSIBLE} GROUP BY 1, 2""")
        con.execute(
            "UPDATE regions SET best = b.best, best_date = b.best_date "
            "FROM region_best b WHERE regions.level = b.level AND regions.region = b.region"
        )
        levels = dict(
            con.execute("SELECT level, count(*) FROM regions GROUP BY 1").fetchall()
        )
        with self._lock:
            old, self._con, self._mtimes = self._con, con, mtimes
            self._region_row.cache_clear()
            self._children.cache_clear()
            self._leaderboard.cache_clear()
            self.meta = {
                "ready": True,
                "regions": sum(levels.values()),
                "levels": levels,
                "k": K,
                "release": os.environ.get("EBD_RELEASE"),
            }
        if old is not None:
            old.close()
        print(f"big days: loaded {sum(levels.values())} regions from {self.dir}")
        return True

    def q(self, sql: str, params=()) -> List[Dict[str, Any]]:
        with self._lock:
            if self._con is None:
                raise HTTPException(503, "big-day tables not loaded yet")
            cur = self._con.execute(sql, params)
            cols = [d[0] for d in cur.description]
            return [dict(zip(cols, r)) for r in cur.fetchall()]

    # --- regions --------------------------------------------------------------------------
    def _region_row_uncached(self, code: str):
        rows = self.q(
            f"SELECT {REGION_COLS}, years FROM regions WHERE region = ?", [code]
        )
        return rows[0] if rows else None

    def _children_uncached(self, code: str):
        return self.q(
            f"SELECT {REGION_COLS} FROM regions WHERE parent = ? ORDER BY best DESC, days DESC",
            [code],
        )

    def breadcrumb(self, row):
        trail = []
        while row:
            trail.append(
                {"code": row["code"], "name": row["name"], "level": row["level"]}
            )
            row = self._region_row(row["parent"]) if row["parent"] else None
        return list(reversed(trail))

    def countries(self):
        return {
            "regions": self.q(
                f"SELECT {REGION_COLS} FROM regions WHERE level = 'country' "
                "ORDER BY best DESC, days DESC"
            )
        }

    def region(self, code: str, include_shared: bool = False):
        check_code(code)
        row = dict(self._region_row(code) or {})
        if not row:
            raise HTTPException(404, "unknown region")
        # Top 3 per year (with their checklists) so the year's record can skip shared days.
        cands = self.q(
            f"""SELECT year, n_species, observation_date, checklists FROM (
                  SELECT year, n_species, observation_date, checklists,
                         row_number() OVER (PARTITION BY year ORDER BY {ORDER}) AS rn
                  FROM big_days WHERE level = ? AND region = ? AND {PLAUSIBLE}) WHERE rn <= 3
                ORDER BY year, rn""",
            [row["level"], code],
        )
        activity = {y["year"]: y for y in (row.pop("years") or [])}
        years = []
        for c in cands:
            if years and years[-1]["year"] == c["year"]:
                continue
            if not include_shared and shared_pairs(c["checklists"]) >= SHARED_PAIRS:
                continue
            years.append(
                {
                    "year": c["year"],
                    "best": c["n_species"],
                    "best_date": c["observation_date"],
                    "days": activity.get(c["year"], {}).get("days"),
                    "observers": activity.get(c["year"], {}).get("observers"),
                }
            )
        if years:
            top = max(years, key=lambda y: (y["best"], -y["year"]))
            row["best"], row["best_date"] = top["best"], top["best_date"]
        return {
            "region": row,
            "breadcrumb": self.breadcrumb(row),
            "children": self._children(code),
            "years": years,
        }

    def search(self, s: str):
        s = (s or "").strip()
        if not 1 <= len(s) <= 60:
            raise HTTPException(400, "q must be 1-60 characters")
        rows = self.q(
            f"""SELECT {REGION_COLS} FROM regions
                WHERE name ILIKE '%' || ? || '%' OR region ILIKE ? || '%'
                ORDER BY (lower(name) = lower(?)) DESC, days DESC LIMIT 20""",
            [s, s, s],
        )
        for r in rows:
            r["breadcrumb"] = self.breadcrumb(r)[:-1]
        return {"results": rows}

    # --- leaderboard ----------------------------------------------------------------------
    def _leaderboard_uncached(self, level, code, year, month, include_shared, limit):
        where, params = ["level = ?", "region = ?", PLAUSIBLE], [level, code]
        if year is not None:
            where.append("year = ?")
            params.append(year)
        if month is not None:
            where.append("month = ?")
            params.append(month)
        # Shared-account days are recognised from their checklists in Python, so fetch a window
        # of candidates and cut after filtering. 4x is far more than the flagged share anywhere
        # measured (Texas: 6 of 50); if a board still comes up short it is short, not wrong.
        window = limit if include_shared else min(400, limit * 4)
        rows = self.q(
            f"""SELECT observation_date AS date, year, month, n_species, n_checklists,
                       n_localities, observers, party_size, minutes, km,
                       all_complete, checklists
                FROM big_days WHERE {" AND ".join(where)}
                ORDER BY {ORDER} LIMIT {int(window)}""",
            params,
        )
        rows = [annotate(r) for r in rows]
        if not include_shared:
            rows = [r for r in rows if not r["shared"]]
        return rows[:limit]

    def top(
        self,
        code: str,
        year: Optional[int] = None,
        month: Optional[int] = None,
        include_shared: bool = False,
        limit: int = K,
    ):
        check_code(code)
        row = self._region_row(code)
        if not row:
            raise HTTPException(404, "unknown region")
        if year is not None and not 1800 <= year <= 2100:
            raise HTTPException(400, "year must be in 1800..2100")
        if month is not None and not 1 <= month <= 12:
            raise HTTPException(400, "month must be in 1..12")
        limit = max(1, min(int(limit), K))
        rows = [
            dict(r)
            for r in self._leaderboard(
                row["level"], code, year, month, bool(include_shared), limit
            )
        ]
        # Competition ranking: equal species counts share a rank (106, 106, 106 -> 1, 1, 1, 4).
        for i, r in enumerate(rows):
            r["rank"] = 1 + sum(1 for o in rows if o["n_species"] > r["n_species"])
        return {
            "region": {k: row[k] for k in ("code", "name", "level", "parent")},
            "filters": {
                "year": year,
                "month": month,
                "include_shared": bool(include_shared),
                "limit": limit,
            },
            "rows": rows,
        }


def check_code(code: str):
    if not REGION_RE.match(code or ""):
        raise HTTPException(400, "bad region code")


big_days = BigDays()
