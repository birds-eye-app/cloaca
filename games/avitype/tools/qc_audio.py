#!/usr/bin/env python3
"""BirdNET quality control for AVITYPE clips.

For every species in the manifest, finds a clip span where BirdNET (Cornell's
bird sound classifier) confirms:
  - the TARGET species is detected throughout (no dead gaps),
  - no COMPETING species is confidently detected (background birds would teach
    players the wrong song),
  - preferring a full 9 s clip but trimming down to 6 s when only a shorter
    clean span exists.

Crucially, acceptance is judged on the PROCESSED clip (trimmed, faded,
loudness-normalized) — normalization can boost a quiet background bird into a
competitor, so verifying the raw recording is not enough. Candidate spans are
ranked on the raw analysis, then each is encoded and re-verified until one
passes. If no span of the current recording passes, alternate XC recordings
are fetched (fetch_audio.candidates_for) and put through the same loop.

Run with a Python that has birdnetlib + tflite-runtime + numpy<2 (see README):
    qc-venv/bin/python games/avitype/tools/qc_audio.py [--only slug,...]

Writes an audit to qc-report.json next to the manifest and updates
manifest.json when a clip's source recording changes.
"""
from __future__ import annotations

import json
import os
import subprocess
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import fetch_audio as fa  # noqa: E402

from birdnetlib import Recording  # noqa: E402
from birdnetlib.analyzer import Analyzer  # noqa: E402

GAME = fa.GAME_DIR
RAW = fa.RAW_DIR
REPORT = os.path.join(GAME, "qc-report.json")

SPAN_TRIES = [9.0, 7.5, 6.0]      # prefer longest clean span
TARGET_OK = 0.25                   # window counts as "target singing"
COMPETITOR_BAD = 0.35              # any window with a rival this loud fails
GAP_RUN = 3                        # max consecutive 1s steps without target
MAX_SPANS_PER_SOURCE = 5
MAX_ALT_RECORDINGS = 6
ANALYZE_SECONDS = 120

_analyzer = None


def analyzer() -> Analyzer:
    global _analyzer
    if _analyzer is None:
        _analyzer = Analyzer()
    return _analyzer


def to_wav(src: str, dest: str) -> None:
    subprocess.run(
        [fa.FFMPEG, "-y", "-v", "error", "-t", str(ANALYZE_SECONDS), "-i", src,
         "-ac", "1", "-ar", "48000", dest],
        check=True, capture_output=True)


def detections_for(path: str) -> list[dict]:
    wav = path + ".qc.wav"
    to_wav(path, wav)
    try:
        try:
            rec = Recording(analyzer(), wav, min_conf=0.1, overlap=2.0)
        except TypeError:  # older birdnetlib without overlap kwarg
            rec = Recording(analyzer(), wav, min_conf=0.1)
        rec.analyze()
        return rec.detections
    finally:
        if os.path.exists(wav):
            os.unlink(wav)


def timeline(dets: list[dict], sci: str, total: float):
    """Per-second max confidence for target vs best competitor (+ rival name)."""
    n = int(total) + 1
    tgt = [0.0] * n
    rival = [0.0] * n
    rival_names: dict[int, str] = {}
    sci_l = sci.lower()
    for d in dets:
        is_target = d["scientific_name"].lower() == sci_l
        for s in range(int(d["start_time"]), min(n, int(d["end_time"]) + 1)):
            if is_target:
                tgt[s] = max(tgt[s], d["confidence"])
            elif d["confidence"] > rival[s]:
                rival[s] = d["confidence"]
                rival_names[s] = d["common_name"]
    return tgt, rival, rival_names


def span_report(tgt, rival, rival_names, start: int, dur: int) -> dict:
    seg_t = tgt[start:start + dur]
    seg_r = rival[start:start + dur]
    singing = sum(1 for v in seg_t if v >= TARGET_OK)
    worst_rival, rival_name = 0.0, ""
    for i, v in enumerate(seg_r):
        if v > worst_rival:
            worst_rival = v
            rival_name = rival_names.get(start + i, "")
    gap, worst_gap = 0, 0
    for v in seg_t:
        gap = gap + 1 if v < 0.12 else 0
        worst_gap = max(worst_gap, gap)
    return {
        "coverage": round(singing / max(1, dur), 3),
        "peak": round(max(seg_t, default=0.0), 3),
        "mean": round(sum(seg_t) / max(1, dur), 3),
        "worst_rival": round(worst_rival, 3),
        "rival_name": rival_name,
        "worst_gap": worst_gap,
    }


def final_passes(r: dict) -> bool:
    return (r["coverage"] >= 0.55 and r["peak"] >= 0.5
            and r["worst_rival"] < COMPETITOR_BAD and r["worst_gap"] < GAP_RUN)


def span_score(r: dict, dur: float) -> float:
    return (r["mean"] * 10 + r["peak"] * 4 + dur * 0.2
            - r["worst_rival"] * 12 - r["worst_gap"] * 2)


def ranked_spans(path: str, sci: str) -> list[dict]:
    """Candidate spans from a raw recording, best first (raw-analysis ranking)."""
    dets = detections_for(path)
    if not dets:
        return []
    total = max(d["end_time"] for d in dets)
    tgt, rival, rn = timeline(dets, sci, total)
    spans = []
    for dur in SPAN_TRIES:
        d = int(dur)
        for start in range(0, max(1, len(tgt) - d)):
            r = span_report(tgt, rival, rn, start, d)
            if r["peak"] < 0.4:  # target barely present — not worth encoding
                continue
            spans.append({"start": float(start), "dur": dur, **r,
                          "score": span_score(r, dur)})
    spans.sort(key=lambda s: -s["score"])
    # drop near-duplicate starts, keep variety
    seen, out = set(), []
    for s in spans:
        key = (int(s["start"]) // 3, s["dur"])
        if key in seen:
            continue
        seen.add(key)
        out.append(s)
        if len(out) >= MAX_SPANS_PER_SOURCE:
            break
    return out


def verify_clip(path: str, sci: str) -> dict:
    """Analyze the PROCESSED clip; skip the last faded second for gap logic."""
    dets = detections_for(path)
    total = max((d["end_time"] for d in dets), default=0)
    tgt, rival, rn = timeline(dets, sci, total)
    dur = max(1, len(tgt) - 2)  # ignore fade-out tail
    return span_report(tgt, rival, rn, 0, dur)


def try_source(path: str, sci: str, clip: str):
    """Encode candidate spans from one source until one verifies clean."""
    tmp = clip + ".qc-tmp.mp3"
    best_fallback = None
    for span in ranked_spans(path, sci):
        fa.encode_clip(path, span["start"], tmp, span["dur"])
        final = verify_clip(tmp, sci)
        cand = {"span": span, "final": final}
        if final_passes(final):
            os.replace(tmp, clip)
            return cand, best_fallback
        if best_fallback is None or span_score(final, span["dur"]) > span_score(
                best_fallback["final"], best_fallback["span"]["dur"]):
            best_fallback = {**cand, "src": path}
    if os.path.exists(tmp):
        os.unlink(tmp)
    return None, best_fallback


def qc_species(entry: dict, sci_by_slug: dict, want_by_slug: dict) -> dict:
    slug = entry["slug"]
    sci = sci_by_slug[slug]
    out = {"slug": slug, "sci": sci, "xcId": entry["xcId"], "action": "none"}
    clip = os.path.join(GAME, entry["file"])
    raw = os.path.join(RAW, f"{slug}-XC{entry['xcId']}")

    if os.path.exists(raw):
        picked, fallback = try_source(raw, sci, clip)
        if picked:
            out.update(action="recut", **picked)
            return out
    else:
        fallback = None

    try:
        alts = fa.candidates_for(sci, want_by_slug.get(slug, "song"))
    except Exception as e:  # noqa: BLE001
        alts = []
        out["note"] = f"alt lookup failed: {e}"
    for alt in alts[:MAX_ALT_RECORDINGS]:
        if str(alt["id"]) == str(entry["xcId"]):
            continue
        apath = os.path.join(RAW, f"{slug}-XC{alt['id']}")
        if not fa.download_audio(alt["id"], apath):
            continue
        picked, fb = try_source(apath, sci, clip)
        if picked:
            out.update(action=f"replaced XC{entry['xcId']} -> XC{alt['id']}", **picked)
            entry["xcId"] = str(alt["id"])
            entry["recordist"] = alt["recordist"]
            entry["license"] = alt["license"]
            return out
        if fb and (fallback is None or span_score(fb["final"], fb["span"]["dur"])
                   > span_score(fallback["final"], fallback["span"]["dur"])):
            fallback = {**fb, "alt": alt}

    if fallback:
        fa.encode_clip(fallback["src"], fallback["span"]["start"], clip,
                       fallback["span"]["dur"])
        if "alt" in fallback:
            entry["xcId"] = str(fallback["alt"]["id"])
            entry["recordist"] = fallback["alt"]["recordist"]
            entry["license"] = fallback["alt"]["license"]
        out.update(action="recut-warn", span=fallback["span"],
                   final=fallback["final"])
    else:
        out["action"] = "unresolved"
    return out


def main() -> None:
    only = None
    if "--only" in sys.argv:
        only = set(sys.argv[sys.argv.index("--only") + 1].split(","))
    with open(fa.MANIFEST) as f:
        manifest = json.load(f)
    sci_by_slug = {s[0]: s[2] for s in fa.SPECIES}
    want_by_slug = {s[0]: s[3] for s in fa.SPECIES}
    report = []
    for entry in manifest:
        if only and entry["slug"] not in only:
            continue
        try:
            r = qc_species(entry, sci_by_slug, want_by_slug)
        except Exception as e:  # noqa: BLE001
            r = {"slug": entry["slug"], "action": "error", "note": str(e)}
        report.append(r)
        f_ = r.get("final") or {}
        print(f"{entry['slug']:24s} {r['action']:34s} "
              f"cov={f_.get('coverage', 0):.2f} peak={f_.get('peak', 0):.2f} "
              f"rival={f_.get('worst_rival', 0):.2f}({f_.get('rival_name','')[:18]}) "
              f"gap={f_.get('worst_gap', 0)} dur={r.get('span', {}).get('dur', '?')}",
              flush=True)
        with open(fa.MANIFEST, "w") as mf:
            json.dump(manifest, mf, indent=2)
        with open(REPORT, "w") as rf:
            json.dump(report, rf, indent=2)
    bad = [r for r in report if r["action"] in ("recut-warn", "unresolved", "error")]
    print(f"\n{len(report)} species QC'd; {len(bad)} need attention: "
          f"{[r['slug'] for r in bad]}")


if __name__ == "__main__":
    main()
