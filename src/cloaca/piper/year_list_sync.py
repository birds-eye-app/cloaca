"""
Gut-check reconciliation: compare hotspot species lists on the eBird website
against what's stored in Postgres.

Fetches eBird hotspot pages via aiohttp (non-browser UA bypasses Anubis bot
detection), extracts the embedded Nuxt SSR state, evaluates it with
py_mini_racer (embedded V8), and diffs against hotspot_year_species and
hotspot_all_time_species.
"""

import asyncio
import json
import logging
import re
from dataclasses import dataclass, field

import aiohttp
import py_mini_racer

from cloaca.piper.db.queries import AsyncQuerier
from cloaca.piper.db_pool import get_engine
from cloaca.piper.year_lifers import WATCHED_HOTSPOTS

logger = logging.getLogger(__name__)


@dataclass
class SpeciesEntry:
    code: str
    name: str


@dataclass
class HotspotSyncResult:
    hotspot_name: str
    year_website_total: int = 0
    year_missing_from_db: list[SpeciesEntry] = field(default_factory=list)
    year_missing_from_website: list[str] = field(default_factory=list)
    all_time_website_total: int = 0
    all_time_missing_from_db: list[SpeciesEntry] = field(default_factory=list)
    all_time_missing_from_website: list[str] = field(default_factory=list)
    error: str | None = None

    @property
    def in_sync(self) -> bool:
        return (
            self.error is None
            and not self.year_missing_from_db
            and not self.year_missing_from_website
            and not self.all_time_missing_from_db
            and not self.all_time_missing_from_website
        )


def _extract_species(bbl: dict, categories: list[str]) -> list[SpeciesEntry]:
    entries = []
    for cat in categories:
        for s in bbl.get(cat, []):
            entries.append(SpeciesEntry(code=s["speciesCode"], name=s["commonName"]))
    return entries


def _evaluate_nuxt(html: str) -> dict:
    match = re.search(r"window\.__NUXT__=(.+?)</script>", html, re.DOTALL)
    if not match:
        raise RuntimeError("__NUXT__ not found in eBird page")
    nuxt_js = match.group(1)

    ctx = py_mini_racer.MiniRacer()
    ctx.eval(f"const window = {{}}; window.__NUXT__ = {nuxt_js};")
    raw = ctx.eval("""
        JSON.stringify(
            window.__NUXT__.fetch['BirdList:0'].binnedBirdList
        );
    """)
    return json.loads(raw)


async def _fetch_bird_list_page(hotspot_id: str, yr: str | None) -> str:
    url = f"https://ebird.org/hotspot/{hotspot_id}/bird-list"
    if yr:
        url += f"?yr={yr}"
    async with aiohttp.ClientSession() as session:
        async with session.get(url) as resp:
            resp.raise_for_status()
            return await resp.text()


async def _fetch_bbl(hotspot_id: str, yr: str | None) -> dict:
    html = await _fetch_bird_list_page(hotspot_id, yr)
    loop = asyncio.get_running_loop()
    return await loop.run_in_executor(None, _evaluate_nuxt, html)


async def _get_db_year_species(hotspot_id: str, year: int) -> set[str]:
    async with get_engine().connect() as conn:
        return {
            code
            async for code in AsyncQuerier(conn).get_known_species(
                hotspot_id=hotspot_id, year=year
            )
        }


async def _get_db_all_time_species(hotspot_id: str) -> set[str]:
    async with get_engine().connect() as conn:
        return {
            code
            async for code in AsyncQuerier(conn).get_known_all_time_species(
                hotspot_id=hotspot_id
            )
        }


def _diff(
    website_entries: list[SpeciesEntry], db_codes: set[str]
) -> tuple[list[SpeciesEntry], list[str]]:
    website_by_code = {e.code: e for e in website_entries}
    website_codes = set(website_by_code)
    missing_from_db = sorted(
        [website_by_code[c] for c in website_codes - db_codes],
        key=lambda e: e.name,
    )
    missing_from_website = sorted(db_codes - website_codes)
    return missing_from_db, missing_from_website


async def check_hotspot_sync(hotspot_id: str, hotspot_name: str, year: int) -> HotspotSyncResult:
    result = HotspotSyncResult(hotspot_name=hotspot_name)
    try:
        year_bbl, all_time_bbl = await asyncio.gather(
            _fetch_bbl(hotspot_id, "cur"),
            _fetch_bbl(hotspot_id, None),
        )

        year_entries = _extract_species(
            year_bbl, ["nativeNaturalized", "provisional", "escapee", "hybrid"]
        )
        all_time_entries = _extract_species(
            all_time_bbl, ["nativeNaturalized", "escapee"]
        )

        db_year, db_all_time = await asyncio.gather(
            _get_db_year_species(hotspot_id, year),
            _get_db_all_time_species(hotspot_id),
        )

        result.year_website_total = len(year_entries)
        result.year_missing_from_db, result.year_missing_from_website = _diff(
            year_entries, db_year
        )
        result.all_time_website_total = len(all_time_entries)
        result.all_time_missing_from_db, result.all_time_missing_from_website = _diff(
            all_time_entries, db_all_time
        )
    except Exception as exc:
        result.error = str(exc)
        logger.exception("sync check failed for %s", hotspot_name)

    return result


def format_sync_message(results: list[HotspotSyncResult]) -> str:
    all_in_sync = all(r.in_sync for r in results)
    lines = []

    if all_in_sync:
        lines.append("✅ **Daily sync check — all clear**")
        for r in results:
            lines.append(
                f"  {r.hotspot_name}: "
                f"{r.year_website_total} year · "
                f"{r.all_time_website_total} all-time"
            )
        return "\n".join(lines)

    lines.append("**Daily sync check**")
    for r in results:
        if r.error:
            lines.append(f"❌ **{r.hotspot_name}**: fetch failed — `{r.error}`")
            continue

        status = "✅" if r.in_sync else "⚠️"
        lines.append(
            f"{status} **{r.hotspot_name}** "
            f"({r.year_website_total} year · {r.all_time_website_total} all-time)"
        )
        for e in r.year_missing_from_db:
            lines.append(f"  year: website has `{e.code}` ({e.name}), DB missing")
        for code in r.year_missing_from_website:
            lines.append(f"  year: DB has `{code}`, not on website")
        for e in r.all_time_missing_from_db:
            lines.append(f"  all-time: website has `{e.code}` ({e.name}), DB missing")
        for code in r.all_time_missing_from_website:
            lines.append(f"  all-time: DB has `{code}`, not on website")

    return "\n".join(lines)


async def run_sync_checks(year: int) -> list[HotspotSyncResult]:
    return [
        await check_hotspot_sync(hotspot.id, hotspot.name, year)
        for hotspot in WATCHED_HOTSPOTS
    ]
