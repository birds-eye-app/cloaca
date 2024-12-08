# notes on what the cron will need to do:
# pull observations from the last 7? days and upsert them to observations DB
# compute interesting stats:
# - were there any FOYs/patch lifers in the last 7 days?
# - - if so, check to see if we've notified about the species code before
# - - if not, send notification! (batch them in one notification) and add to the list of notified species codes
# - - (note, for tracking if we've sent them, we could just have a table for foy's and use presence there to indicate notification?)

# on first run, we'll need to:
# - backfill all observations for the hotspot (api for year and then dataset for prior years)
# - compute the FOY / life list for the hotspot
# - add those to a "notified" list so we don't mistakenly send out a huge batch of notifications


# as end user:
# - I want to get notifications when a new species is observed in the hotspot for the first time that year
# - I want to get notifications when a new species is observed in the hotspot for the first time ever (with like lots of fanfare)
# - If we're in a friend group, I want to hear when a friend gets a new species for the year / life patch list
