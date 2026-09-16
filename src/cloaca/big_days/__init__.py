"""Big Days — the biggest single-day species counts in eBird per country / state / county,
served to the personal site's /big-days page (beak-v2, src/big-days/).

The data is two Parquet files derived from the eBird Basic Dataset by an external build job
and placed in BIG_DAYS_DIR by the deployment; this process reads local files only:

  big_days.parquet         one row per (level, region, observation_date, party):
                           level ∈ country|state|county, region = the eBird code, year, month,
                           observer_id (pseudonymous, e.g. obsr59592), n_species, n_checklists,
                           n_localities, observers (max party size on the day's lists), solo,
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

Fixed, parameterised queries only; region codes are regex-checked. The files may be absent
(new box, or a release not built yet): every endpoint answers 503 until they appear, and the
loader re-checks on cloaca's periodic timer so a new release is picked up without a restart.
"""

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

    def region(self, code: str):
        check_code(code)
        row = dict(self._region_row(code) or {})
        if not row:
            raise HTTPException(404, "unknown region")
        rec = self.q(
            f"""SELECT year, max(n_species) AS best, arg_max(observation_date, n_species) AS best_date,
                      arg_max(observer_id, n_species) AS observer_id
               FROM big_days WHERE level = ? AND region = ? AND {PLAUSIBLE} GROUP BY 1 ORDER BY 1""",
            [row["level"], code],
        )
        activity = {y["year"]: y for y in (row.pop("years") or [])}
        years = [
            {
                **r,
                "days": activity.get(r["year"], {}).get("days"),
                "observers": activity.get(r["year"], {}).get("observers"),
            }
            for r in rec
        ]
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
    def _leaderboard_uncached(self, level, code, year, month, solo, limit):
        where, params = ["level = ?", "region = ?", PLAUSIBLE], [level, code]
        if year is not None:
            where.append("year = ?")
            params.append(year)
        if month is not None:
            where.append("month = ?")
            params.append(month)
        if solo:
            where.append("solo")
        return self.q(
            f"""SELECT observation_date AS date, year, month, observer_id, n_species, n_checklists,
                       n_localities, observers, solo, party_size, members, minutes, km,
                       all_complete, checklists
                FROM big_days WHERE {" AND ".join(where)}
                ORDER BY {ORDER} LIMIT {int(limit)}""",
            params,
        )

    def top(
        self,
        code: str,
        year: Optional[int] = None,
        month: Optional[int] = None,
        solo: bool = False,
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
                row["level"], code, year, month, bool(solo), limit
            )
        ]
        for i, r in enumerate(rows):
            r["rank"] = i + 1
        return {
            "region": {k: row[k] for k in ("code", "name", "level", "parent")},
            "filters": {
                "year": year,
                "month": month,
                "solo": bool(solo),
                "limit": limit,
            },
            "rows": rows,
        }


def check_code(code: str):
    if not REGION_RE.match(code or ""):
        raise HTTPException(400, "bad region code")


big_days = BigDays()
