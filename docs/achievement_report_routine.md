# Achievement report routine

The daily routine that emails an eBird achievement report. Paste the prompt
below into a Claude Routine (claude.ai → Routines), or use it with
`create_trigger`.

## The live routine

- Trigger: `trig_01QQbtpD5uL4DLk7s9rvn5ad`, name "eBird achievement report"
- Schedule: `22 13 * * *` (UTC)
- Fires a **fresh session** each time (`persistent_session_id` is unset), so it
  starts with no repo checkout and no prior context.

That last point is the whole reason for the standalone build below: a
fresh session has no repository permissions, and the `add_repo` call it
would otherwise attempt is what silently broke runs (they finished
"successfully" in ~20 seconds having sent nothing).

## Required connectors

The routine's sessions **must** have these attached, or it cannot run:

| Connector | Used for |
|---|---|
| Gmail | finding the eBird export email, and the sent-mail dedup check |
| patch town | `google_gmail_send` (the report itself), Drive access |

Connectors can only be attached to fresh-session routines from the claude.ai
Routines UI — the API rejects the `connectors` parameter for this org.

## Running the engine without a repo checkout

`scripts/ebird_achievements_standalone.py` is a generated, stdlib-only build
of the engine. It needs no checkout, no `uv`, and no pandas — just
`python3 ebird_achievements_standalone.py MyEBirdData.csv --since YYYY-MM-DD`,
and its output is byte-identical to `python -m cloaca.achievements.cli`.

A copy lives in Google Drive so a session with no repository access can still
produce a report. Fetch it with the Drive connector's `download_file_content`
(returns base64) and decode it to a local `.py` — `read_file_content` does
**not** support `text/x-python` and will fail. When the base64 is too large
for the tool result, the harness saves it to a file on disk; decode from that
file rather than pulling it through context. Verified end to end: the Drive
copy decodes byte-identical and runs under plain `python3`.

Locations:

- Folder: `cloaca-achievements` (`1y8wfGoQY9UFGEnmzsePUT15a8dO8_S8U`)
- Engine: `ebird_achievements_standalone.py` (`1heyhsM4SeaKNPueI458ejGfHrVBn2BfT`)
- This document: `achievement_report_routine.md`

Regenerate after any engine change with
`python3 scripts/build_standalone_achievements.py` (tests fail if the
checked-in copy is stale), then re-upload the result to that Drive folder —
the Drive copy does not update itself.

**The routine must never call `add_repo`.** Sessions fired by a routine
generally lack repository permissions, and the attempt is what breaks the run.

---

## Prompt

The routine's prompt is kept in **`achievement_report_routine_prompt.txt`**
next to this file — one canonical copy, matching the live trigger verbatim.

Do not paste a second copy into this document. An earlier duplicate here went
stale within a day of the trigger being updated, which is the same class of
bug the generated engine bundle exists to prevent.

To change the routine, edit that file and apply it with `update_trigger`
(trigger id above), then re-upload it to the Drive folder.
