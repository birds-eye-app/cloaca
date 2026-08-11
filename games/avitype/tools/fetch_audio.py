#!/usr/bin/env python3
"""Fetch real bird song recordings for AVITYPE from Xeno-Canto.

Discovery goes through GBIF's free occurrence API (which indexes the whole
Xeno-Canto dataset, including the XC quality rating, song/call type, length,
recordist, and license) so no API key is required. Audio downloads come from
xeno-canto.org's public per-recording download endpoint.

Each clip is trimmed to the most sound-active ~9 seconds, faded, loudness
normalized, and encoded as small mono MP3 (22.05 kHz / 48 kbps, ~55 KB), and
games/avitype/manifest.json is written with full attribution. Xeno-Canto
recordings are Creative Commons licensed — the recordist credit and license in
the manifest must stay with the game.

Usage:  python games/avitype/tools/fetch_audio.py [--only slug,slug,...]
Deps:   pip install imageio-ffmpeg   (bundles a static ffmpeg)
"""
from __future__ import annotations

import array
import json
import math
import os
import re
import subprocess
import sys
import time
import urllib.parse
import urllib.request

import imageio_ffmpeg

FFMPEG = imageio_ffmpeg.get_ffmpeg_exe()
HERE = os.path.dirname(os.path.abspath(__file__))
GAME_DIR = os.path.dirname(HERE)
AUDIO_DIR = os.path.join(GAME_DIR, "audio")
MANIFEST = os.path.join(GAME_DIR, "manifest.json")
RAW_DIR = os.environ.get("AVITYPE_RAW_DIR", "/tmp/avitype-raw")

XC_DATASET = "b1047888-ae52-4179-9dd5-5448ea342a24"  # Xeno-canto on GBIF
GBIF = "https://api.gbif.org/v1/occurrence/search"
CLIP_SECONDS = 9.0
MAX_DOWNLOAD_BYTES = 40 * 1024 * 1024
UA = "avitype-fetch/1.0 (personal bird-learning game; polite, low volume)"

# slug, display name, scientific name, preferred vocalization
SPECIES: list[tuple[str, str, str, str]] = [
    ("robin", "robin", "Turdus migratorius", "song"),
    ("blue-jay", "blue jay", "Cyanocitta cristata", "call"),
    ("cardinal", "cardinal", "Cardinalis cardinalis", "song"),
    ("chickadee", "chickadee", "Poecile atricapillus", "song"),
    ("carolina-wren", "carolina wren", "Thryothorus ludovicianus", "song"),
    ("wood-thrush", "wood thrush", "Hylocichla mustelina", "song"),
    ("ovenbird", "ovenbird", "Seiurus aurocapilla", "song"),
    ("scarlet-tanager", "scarlet tanager", "Piranga olivacea", "song"),
    ("heron", "heron", "Ardea herodias", "call"),
    ("mallard", "mallard", "Anas platyrhynchos", "call"),
    ("red-winged-blackbird", "red-winged blackbird", "Agelaius phoeniceus", "song"),
    ("kingfisher", "kingfisher", "Megaceryle alcyon", "call"),
    ("wood-duck", "wood duck", "Aix sponsa", "call"),
    ("egret", "egret", "Ardea alba", "call"),
    ("marsh-wren", "marsh wren", "Cistothorus palustris", "song"),
    ("bittern", "bittern", "Botaurus lentiginosus", "song"),
    ("meadowlark", "meadowlark", "Sturnella magna", "song"),
    ("goldfinch", "goldfinch", "Spinus tristis", "song"),
    ("red-tailed-hawk", "red-tailed hawk", "Buteo jamaicensis", "call"),
    ("killdeer", "killdeer", "Charadrius vociferus", "call"),
    ("bobolink", "bobolink", "Dolichonyx oryzivorus", "song"),
    ("kestrel", "kestrel", "Falco sparverius", "call"),
    ("dickcissel", "dickcissel", "Spiza americana", "song"),
    ("grasshopper-sparrow", "grasshopper sparrow", "Ammodramus savannarum", "song"),
    ("gull", "gull", "Leucophaeus atricilla", "call"),
    ("loon", "loon", "Gavia immer", "song"),
    ("osprey", "osprey", "Pandion haliaetus", "call"),
    ("willet", "willet", "Tringa semipalmata", "call"),
    ("tern", "tern", "Sterna hirundo", "call"),
    ("oystercatcher", "oystercatcher", "Haematopus palliatus", "call"),
    ("sanderling", "sanderling", "Calidris alba", "call"),
    ("plover", "plover", "Pluvialis squatarola", "call"),
    ("mourning-dove", "mourning dove", "Zenaida macroura", "song"),
    ("house-sparrow", "house sparrow", "Passer domesticus", "song"),
    ("house-finch", "house finch", "Haemorhous mexicanus", "song"),
    ("downy-woodpecker", "downy woodpecker", "Dryobates pubescens", "call"),
    ("tufted-titmouse", "titmouse", "Baeolophus bicolor", "song"),
    ("white-breasted-nuthatch", "nuthatch", "Sitta carolinensis", "call"),
    ("dark-eyed-junco", "junco", "Junco hyemalis", "song"),
    ("song-sparrow", "song sparrow", "Melospiza melodia", "song"),
]

SOUND_EXT = "http://rs.tdwg.org/ac/terms/Multimedia"


def http_json(url: str) -> dict:
    req = urllib.request.Request(url, headers={"User-Agent": UA})
    with urllib.request.urlopen(req, timeout=60) as r:
        return json.load(r)


def parse_len_seconds(desc: str) -> float | None:
    # "29 s" or "1:24" style descriptions
    if not desc:
        return None
    m = re.match(r"^\s*(\d+)\s*s\s*$", desc)
    if m:
        return float(m.group(1))
    m = re.match(r"^\s*(\d+):(\d\d)\s*$", desc)
    if m:
        return int(m.group(1)) * 60 + int(m.group(2))
    return None


def candidates_for(sci: str, want_type: str) -> list[dict]:
    """Rank Xeno-Canto recordings of a species via GBIF metadata."""
    out = []
    params = urllib.parse.urlencode({
        "datasetKey": XC_DATASET, "scientificName": sci,
        "mediaType": "Sound", "limit": 300,
    })
    data = http_json(f"{GBIF}?{params}")
    for rec in data.get("results", []):
        ref = rec.get("references") or ""
        m = re.search(r"XC(\d+)", ref)
        if not m:
            continue
        xc_id = m.group(1)
        behavior = (rec.get("behavior") or "").lower()
        sound = None
        for ext in (rec.get("extensions") or {}).get(SOUND_EXT, []):
            fmt = ext.get("http://purl.org/dc/terms/format", "")
            if fmt.startswith("audio"):
                sound = ext
                break
        if sound is None:
            continue
        rating = sound.get("http://ns.adobe.com/xap/1.0/Rating")
        length = parse_len_seconds(sound.get("http://purl.org/dc/terms/description", ""))
        score = 0.0
        if rating in ("5", "4"):
            score += {"5": 40, "4": 25}[rating]
        if want_type in behavior:
            score += 30
        elif behavior and want_type not in behavior:
            score -= 15
        if length is not None:
            if 8 <= length <= 40:
                score += 20
            elif 5 <= length < 8 or 40 < length <= 90:
                score += 8
            elif length > 240:
                score -= 20
        if rec.get("country") in ("United States of America", "Canada"):
            score += 8
        if want_type == "song" and rec.get("month") in (4, 5, 6, 7):
            score += 4
        out.append({
            "id": xc_id, "score": score, "behavior": behavior,
            "rating": rating, "length": length,
            "recordist": rec.get("recordedBy") or sound.get("http://purl.org/dc/elements/1.1/creator", ""),
            "license": sound.get("http://purl.org/dc/terms/rights", ""),
        })
    out.sort(key=lambda c: -c["score"])
    return out


def download_audio(xc_id: str, dest: str) -> bool:
    if os.path.exists(dest) and os.path.getsize(dest) > 10_000:
        return True
    url = f"https://xeno-canto.org/{xc_id}/download"
    req = urllib.request.Request(url, headers={"User-Agent": UA})
    try:
        with urllib.request.urlopen(req, timeout=120) as r:
            data = r.read(MAX_DOWNLOAD_BYTES + 1)
            if len(data) > MAX_DOWNLOAD_BYTES or len(data) < 10_000:
                return False
        with open(dest, "wb") as f:
            f.write(data)
        time.sleep(1.0)  # politeness
        return True
    except Exception:
        return False


def best_window(src: str) -> float:
    """Start (s) of the highest-RMS CLIP_SECONDS window in the recording."""
    wav = src + ".an.wav"
    subprocess.run(
        [FFMPEG, "-y", "-v", "error", "-t", "120", "-i", src,
         "-ac", "1", "-ar", "8000", "-f", "wav", wav],
        check=True, capture_output=True)
    with open(wav, "rb") as f:
        data = f.read()
    os.unlink(wav)
    i = data.find(b"data")
    samples = array.array("h")
    usable = (len(data) - i - 8) // 2 * 2
    samples.frombytes(data[i + 8: i + 8 + usable])
    sr, hop = 8000, 2000  # 0.25 s hops
    rms = []
    for h in range(max(1, (len(samples) - hop) // hop)):
        seg = samples[h * hop:(h + 1) * hop]
        if not seg:
            break
        acc = sum(s * s for s in seg[::4])
        rms.append(math.sqrt(acc / max(1, len(seg) // 4)))
    win = int(CLIP_SECONDS * 4)
    if len(rms) <= win:
        return 0.0
    best_i, run = 0, sum(rms[:win])
    best_v = run
    for h in range(1, len(rms) - win):
        run += rms[h + win - 1] - rms[h - 1]
        if run > best_v:
            best_v, best_i = run, h
    return max(0.0, best_i * 0.25 - 0.2)


def encode_clip(src: str, start: float, dest: str, dur: float = CLIP_SECONDS) -> None:
    fade_out = dur - 0.45
    subprocess.run(
        [FFMPEG, "-y", "-v", "error", "-ss", f"{start:.2f}", "-t", str(dur),
         "-i", src, "-ac", "1", "-ar", "22050",
         "-af", f"afade=t=in:d=0.15,afade=t=out:st={fade_out}:d=0.45,loudnorm=I=-18:TP=-1.5",
         "-b:a", "48k", dest],
        check=True, capture_output=True)


def main() -> None:
    only = None
    if "--only" in sys.argv:
        only = set(sys.argv[sys.argv.index("--only") + 1].split(","))
    os.makedirs(AUDIO_DIR, exist_ok=True)
    os.makedirs(RAW_DIR, exist_ok=True)
    manifest = []
    if os.path.exists(MANIFEST):
        with open(MANIFEST) as f:
            manifest = [m for m in json.load(f) if not (only and m["slug"] in only)]
    for slug, name, sci, want_type in SPECIES:
        if only and slug not in only:
            continue
        try:
            cands = candidates_for(sci, want_type)
        except Exception as e:  # noqa: BLE001
            print(f"FAIL {slug}: GBIF query failed: {e}", file=sys.stderr)
            continue
        done = False
        for cand in cands[:6]:
            raw = os.path.join(RAW_DIR, f"{slug}-XC{cand['id']}")
            if not download_audio(cand["id"], raw):
                continue
            try:
                start = best_window(raw)
                encode_clip(raw, start, os.path.join(AUDIO_DIR, f"{slug}.mp3"))
            except subprocess.CalledProcessError:
                continue
            kb = os.path.getsize(os.path.join(AUDIO_DIR, f"{slug}.mp3")) // 1024
            manifest.append({
                "slug": slug, "name": name, "sci": sci,
                "file": f"audio/{slug}.mp3", "xcId": cand["id"],
                "recordist": cand["recordist"], "license": cand["license"],
            })
            print(f"ok {slug}: XC{cand['id']} ({cand['behavior'] or '?'}, "
                  f"rating {cand['rating'] or '?'}, {cand['length'] or '?'}s src) "
                  f"start={start:.1f}s -> {kb}KB")
            done = True
            break
        if not done:
            print(f"FAIL {slug}: no usable recording in top candidates", file=sys.stderr)
        time.sleep(0.5)
    manifest.sort(key=lambda m: m["slug"])
    with open(MANIFEST, "w") as f:
        json.dump(manifest, f, indent=2)
    total = sum(os.path.getsize(os.path.join(AUDIO_DIR, p))
                for p in os.listdir(AUDIO_DIR)) // 1024
    print(f"\nmanifest: {len(manifest)} species, total audio {total}KB")


if __name__ == "__main__":
    main()
