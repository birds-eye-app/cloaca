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
produce a report:

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

Daily eBird export check (recurring routine). Check whether a new eBird personal data export email has arrived and, if so, email David (davidtmeadows@gmail.com) his birding achievement report. Work autonomously; never ask questions.

1. Using the Gmail connector, search: from:do-not-reply@ebird.org "data are now available" newer_than:2d — personal exports have the subject "Your eBird data are now available for download" (ignore "data request" emails; those are a different eBird product). If no such email exists, STOP immediately: no reply needed, end the turn quietly.

2. Dedup guard: search in:sent subject:"eBird achievement report" newer_than:2d. If a report email was already sent after the newest export email arrived, STOP quietly.

3. Read the newest export email and extract the zip download link (an https://is-ebird-datadownload-projects-prod.s3.amazonaws.com/download_my_data/...zip URL). Download it with curl into a temp directory and unzip it — it contains MyEBirdData.csv.

4. Determine the diff date SINCE: search Gmail for the previous export email (same sender, same "data are now available" subject, older than the newest one) and use its date as YYYY-MM-DD. If none exists, use 30 days before today.

5. Get the achievement engine and run it. NEVER call add_repo — routine sessions lack repository permissions and the attempt is what breaks the run. In order of preference:
   a. If /home/user/cloaca exists with src/cloaca/achievements, run:
      uv run --python /usr/bin/python3.12 python -m cloaca.achievements.cli <csv> --since <SINCE>
   b. Otherwise download ebird_achievements_standalone.py from the Google Drive folder "cloaca-achievements" (via the Drive connector) and run:
      python3 ebird_achievements_standalone.py <csv> --since <SINCE>
   The CLI output is the single source of truth for what was unlocked.
   If you cannot obtain and run the engine by either route, DO NOT write a report from the raw CSV and DO NOT guess any numbers. Send David a short plain email titled "eBird achievement report — engine unavailable" explaining exactly which step failed, and stop.
   Streaks already follow eBird's logic (any checklist day counts; known zero-species checklist days are configured in the engine). If David reports eBird showing a longer streak, the likely cause is a new zero-species checklist day missing from KNOWN_EMPTY_CHECKLIST_DAYS — mention it to him rather than editing code.

6. WRITE the report yourself — David wants a Claude-written report, not raw CLI output — and send it to davidtmeadows@gmail.com via the patch town connector's google_gmail_send tool with body_format: "html". Subject: "🏆 eBird achievement report — <date of newest export email as YYYY-MM-DD>".
   Accuracy rules: every achievement, number, date, species, and list count must come from the CLI output verbatim — never invent, merge, or embellish stats, and never omit a gold event. Fun facts may come from your own bird knowledge (migration, behavior, field marks, natural history, etymology) but must be about the species in general — no claims about the specific sighting you can't see in the data; skip a fact rather than guess.
   Content and voice: warm and fun, under ~500 words of prose. Open with the single most exciting unlock since SINCE, told like news. Then the rest ordered by excitement. Weave in 2–4 short species fun facts. End with 2–3 concrete "chase board" teasers visible in the numbers (approaching milestones). If nothing was unlocked, send a short cheerful edition: stat block + chase board.
   HTML template (hand-built, ALL styles inline — email clients ignore style blocks; no external images or scripts). Keep this consistent with previous reports:
   - Wrapper div: max-width 600px, centered, font-family -apple-system/Helvetica/Arial, color #1f2a1f, background #ffffff.
   - Header bar: background #1e4d2b, border-radius 10px 10px 0 0, padding 20px 24px; white 20px bold title "🏆 eBird Achievement Report"; below it a 13px #a8d5b0 line with the export date + a short kicker.
   - Body div: padding 20px 24px, 1px #e3e8e3 border (no top), border-radius 0 0 10px 10px.
   - Lead story: one 16px paragraph, line-height 1.55.
   - Each unlock: a card div, padding 12px 14px, border-radius 0 6px 6px 0, 4px left border colored by tier — gold: border #d4a017 on background #fdf6e3; silver: border #7d8790 on #f4f6f8; bronze: border #b0651e on #faf1e8. First line bold 15px: tier emoji + title (species + list number). Second line 14px line-height 1.5: detail and any fun fact.
   - Stat block: heading "📊 Trophy Case" (bold 15px), then a full-width border-collapse table, 14px, rows padded 6px 10px with 1px #e3e8e3 bottom borders and #f7faf7 zebra striping; right column right-aligned bold. Rows: life list, checklists, each patch total, each region total, biggest day, streak (current · longest), all-time 🏆/✨/🌱 counts.
   - Chase board: bold heading, then 14px lines each starting 🎯, separated by <br>.
   - Footer: 12px #7a857a above a 1px #e3e8e3 top border: "Generated by the cloaca achievement engine + Claude".

7. Constraints: only email davidtmeadows@gmail.com; never anyone else. Use Gmail tools only for the searches/reads above, patch town only for google_gmail_send and Drive access. Do not push code or modify any repository.
