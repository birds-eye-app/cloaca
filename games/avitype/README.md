# AVITYPE — Bird Song Academy

A bird song typing game in the spirit of the classic 90s edutainment
typing/math "Blaster" games: bright chunky pixel art, beveled panels, big
friendly buttons — and **real bird songs**. The point of the game is to make
learning birds by ear fun.

## How it plays

- You fly through a daytime pixel landscape. Birds appear as **dark
  silhouettes** in the right ecosystem at roughly the right height (ovenbirds
  on the ground, tanagers in the canopy, kestrels hovering, ospreys way up)
  and sing **real recordings** from xeno-canto.org.
- Type a bird's name to reveal its colored pixel art (its song replays as a
  reward). The first letter locks on. A wrong key **scares the bird away**;
  a bird that escapes unnamed costs a life.
- **Levels teach progressively**: level 1 of each flyway has only that
  habitat's common, iconic birds; levels 2 and 3 mix in trickier species.
  Before each level, a field-guide briefing shows the new birds — tap a card
  to hear its song. Clear all three levels of every flyway to unlock the
  Migration finale (all flyways, all birds).
- Stars (lives kept) and unlocks persist in localStorage.

Flyways: FOREST, WETLAND, GRASSLAND, COAST — 8 species each, 32 total.

## Running

Serve the directory statically (audio + manifest load over HTTP):

    python -m http.server -d games/avitype

Opened as a bare file (no server), the game still works and falls back to
synthesized song motifs. Touch devices work — tapping the screen summons the
keyboard.

## Audio: real recordings from Xeno-Canto

`tools/fetch_audio.py` fetches everything, no API key required:

- **Discovery** via GBIF's free occurrence API, which indexes the Xeno-Canto
  dataset including quality rating, song/call type, length, recordist, and
  license. The best-rated, right-type, right-length recording wins.
- **Download** from xeno-canto.org's public per-recording endpoint (polite:
  one request per species with delays).
- **Processing**: each clip is trimmed to its most sound-active ~9 s window,
  faded, loudness-normalized, and encoded as mono 22.05 kHz / 48 kbps MP3
  (~55 KB each) using a static ffmpeg from `pip install imageio-ffmpeg`.
- `manifest.json` records the XC number, recordist, and license for every
  clip; the game shows these credits after each level. The recordings are
  Creative Commons licensed — keep the credits with the game.

Re-run anytime (`--only slug,slug` to refresh specific species). To swap in
a hand-picked recording, edit the manifest/audio and skip the fetch.

`tools/build_artifact.py` bundles the game into a single self-contained HTML
file (font, manifest, and all audio inlined as data URIs) for hosting where
external requests aren't possible.
