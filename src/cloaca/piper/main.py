import datetime
import logging
import os
import re
from zoneinfo import ZoneInfo

import discord
from discord.ext import tasks

from cloaca.piper.bird_query import ask_bird_query
from cloaca.piper.birdcast import (
    BIRDCAST_CHANNEL_ID,
    birdcast_link_view,
    fetch_birdcast_forecast,
    format_forecast_message,
)
from cloaca.piper.year_lifers import (
    WATCHED_HOTSPOTS,
    all_time_list_link_view,
    backfill_all_time_species,
    backfill_year_species,
    check_for_new_all_time_lifers,
    check_for_new_year_lifers,
    check_pending_provisionals,
    checklist_link_view,
    fetch_notable_observations,
    fetch_recent_observations,
    format_all_time_lifer_message,
    format_confirmed_all_time_lifer_message,
    format_confirmed_year_lifer_message,
    format_invalidated_lifer_message,
    format_tentative_all_time_lifer_message,
    format_tentative_year_lifer_message,
    format_year_lifer_message,
    get_all_time_total,
    get_year_total,
    year_list_link_view,
)

logging.basicConfig(level=logging.INFO)
logging.getLogger("httpx").setLevel(logging.WARNING)

logger = logging.getLogger(__name__)

intents = discord.Intents.default()
intents.message_content = True

bot = discord.Client(intents=intents)

# Maps bot reply message ID -> full messages list for that conversation turn
CACHE_MAX_SIZE = 50
conversation_cache: dict[int, list[dict]] = {}


def cache_put(message_id: int, messages: list[dict]) -> None:
    if len(conversation_cache) >= CACHE_MAX_SIZE:
        oldest = next(iter(conversation_cache))
        del conversation_cache[oldest]
    conversation_cache[message_id] = messages


async def build_prior_context(ref_msg: discord.Message) -> str:
    """Walk reply chain up to 5 messages and return a plain-text summary."""
    turns = []
    msg = ref_msg
    depth = 0

    while msg is not None and depth < 5:
        if msg.author == bot.user:
            content = re.sub(r"\n\n-#.*$", "", msg.content, flags=re.DOTALL).strip()
            turns.insert(0, f"Piper: {content}")
        else:
            content = (
                msg.content.replace(f"<@{bot.user.id}>", "")
                .replace(f"<@!{bot.user.id}>", "")
                .strip()
            )
            turns.insert(0, f"User: {content}")

        if msg.reference is not None:
            try:
                msg = await msg.channel.fetch_message(msg.reference.message_id)
            except discord.NotFound:
                break
        else:
            msg = None
        depth += 1

    if not turns:
        return ""
    return (
        "[Prior conversation — reconstructed from Discord, not current session:]\n"
        + "\n\n".join(turns)
    )


PIPER_BOT_UPDATES_CHANNEL_ID = 1492206410711433397


_EASTERN = ZoneInfo("America/New_York")
_last_year_lifer_check: datetime.datetime | None = None


@tasks.loop(minutes=15)
async def check_year_lifers():
    global _last_year_lifer_check

    now = datetime.datetime.now(_EASTERN)
    is_night = now.hour < 6 or now.hour >= 22

    if is_night and _last_year_lifer_check is not None:
        elapsed = (now - _last_year_lifer_check).total_seconds()
        if elapsed < 3600:
            return

    _last_year_lifer_check = now

    for hotspot in WATCHED_HOTSPOTS:
        channel = bot.get_channel(hotspot.channel_id)
        if channel is None:
            logger.warning(
                "could not find channel %d for %s",
                hotspot.channel_id,
                hotspot.name,
            )
            continue

        try:
            observations = await fetch_recent_observations(hotspot.id)
        except Exception:
            logger.exception("failed to fetch observations for %s", hotspot.name)
            continue

        try:
            notable = await fetch_notable_observations(hotspot.id)
        except Exception:
            logger.exception(
                "failed to fetch notable observations for %s", hotspot.name
            )
            notable = []

        # ---- Detect new lifers (confirmed + provisional) ----

        # All-time lifers first (takes priority over year lifers)
        try:
            confirmed_at, provisional_at = await check_for_new_all_time_lifers(
                hotspot.id, observations, notable
            )
        except Exception:
            logger.exception("failed to check all-time lifers for %s", hotspot.name)
            confirmed_at, provisional_at = [], []

        if confirmed_at:
            total = await get_all_time_total(hotspot.id)
            message = format_all_time_lifer_message(confirmed_at, hotspot.name, total)
            await channel.send(message, view=all_time_list_link_view(hotspot.id))
            logger.info(
                "posted %d confirmed all-time lifer(s) for %s",
                len(confirmed_at),
                hotspot.name,
            )

        if provisional_at:
            message = format_tentative_all_time_lifer_message(
                provisional_at, hotspot.name
            )
            await channel.send(
                message,
                view=checklist_link_view(
                    [(o.comName, o.subId) for o in provisional_at]
                ),
            )
            logger.info(
                "posted %d tentative all-time lifer(s) for %s",
                len(provisional_at),
                hotspot.name,
            )

        # Year lifers, excluding any that were all-time lifers
        try:
            confirmed_yr, provisional_yr = await check_for_new_year_lifers(
                hotspot.id, observations, notable
            )
        except Exception:
            logger.exception("failed to check year lifers for %s", hotspot.name)
            confirmed_yr, provisional_yr = [], []

        all_time_codes = {o.speciesCode for o in confirmed_at + provisional_at}
        confirmed_yr = [o for o in confirmed_yr if o.speciesCode not in all_time_codes]
        provisional_yr = [
            o for o in provisional_yr if o.speciesCode not in all_time_codes
        ]

        if confirmed_yr:
            total = await get_year_total(hotspot.id)
            message = format_year_lifer_message(confirmed_yr, hotspot.name, total)
            await channel.send(message, view=year_list_link_view(hotspot.id))
            logger.info(
                "posted %d confirmed year lifer(s) for %s",
                len(confirmed_yr),
                hotspot.name,
            )

        if provisional_yr:
            message = format_tentative_year_lifer_message(provisional_yr, hotspot.name)
            await channel.send(
                message,
                view=checklist_link_view(
                    [(o.comName, o.subId) for o in provisional_yr]
                ),
            )
            logger.info(
                "posted %d tentative year lifer(s) for %s",
                len(provisional_yr),
                hotspot.name,
            )

        # ---- Check previously-pending provisionals for review updates ----

        try:
            pend_confirmed, pend_invalidated = await check_pending_provisionals(
                hotspot.id, notable
            )
        except Exception:
            logger.exception(
                "failed to check pending provisionals for %s", hotspot.name
            )
            pend_confirmed, pend_invalidated = [], []

        if pend_confirmed:
            at_confirmed = [p for p in pend_confirmed if p.lifer_type == "all_time"]
            yr_confirmed = [p for p in pend_confirmed if p.lifer_type == "year"]
            if at_confirmed:
                total = await get_all_time_total(hotspot.id)
                message = format_confirmed_all_time_lifer_message(
                    at_confirmed, hotspot.name, total
                )
                await channel.send(
                    message,
                    view=checklist_link_view(
                        [(p.common_name, p.sub_id) for p in at_confirmed]
                    ),
                )
            if yr_confirmed:
                total = await get_year_total(hotspot.id)
                message = format_confirmed_year_lifer_message(
                    yr_confirmed, hotspot.name, total
                )
                await channel.send(
                    message,
                    view=checklist_link_view(
                        [(p.common_name, p.sub_id) for p in yr_confirmed]
                    ),
                )

        if pend_invalidated:
            message = format_invalidated_lifer_message(pend_invalidated, hotspot.name)
            await channel.send(message)

        has_activity = (
            confirmed_at
            or provisional_at
            or confirmed_yr
            or provisional_yr
            or pend_confirmed
            or pend_invalidated
        )
        if not has_activity:
            logger.info("no new lifers at %s", hotspot.name)


@tasks.loop(time=datetime.time(hour=7, minute=0, tzinfo=_EASTERN))
async def post_birdcast_forecast():
    channel = bot.get_channel(BIRDCAST_CHANNEL_ID)
    if channel is None:
        logger.warning("could not find birdcast channel %d", BIRDCAST_CHANNEL_ID)
        return
    forecast = await fetch_birdcast_forecast()
    if forecast is None or not forecast.forecastNights:
        logger.info("no birdcast forecast available, skipping")
        return
    message = format_forecast_message(forecast)
    await channel.send(message, view=birdcast_link_view())
    logger.info("posted birdcast forecast")


@bot.event
async def on_ready():
    logger.info("online as %s", bot.user)
    channel = bot.get_channel(PIPER_BOT_UPDATES_CHANNEL_ID)
    if channel:
        await channel.send("I'm up!")
    else:
        logger.warning(
            "could not find startup channel %d", PIPER_BOT_UPDATES_CHANNEL_ID
        )
    if not post_birdcast_forecast.is_running():
        post_birdcast_forecast.start()
    if not check_year_lifers.is_running():
        for hotspot in WATCHED_HOTSPOTS:
            try:
                count = await backfill_year_species(hotspot.id)
                if count > 0:
                    logger.info(
                        "backfilled %d year species for %s",
                        count,
                        hotspot.name,
                    )
            except Exception:
                logger.exception("failed to backfill year species for %s", hotspot.name)
            try:
                count = await backfill_all_time_species(hotspot.id)
                if count > 0:
                    logger.info(
                        "backfilled %d all-time species for %s",
                        count,
                        hotspot.name,
                    )
            except Exception:
                logger.exception(
                    "failed to backfill all-time species for %s", hotspot.name
                )
        check_year_lifers.start()


@bot.event
async def on_message(message: discord.Message):
    if message.author == bot.user:
        return

    is_mention = bot.user in message.mentions
    is_reply = message.reference is not None

    if not is_mention and not is_reply:
        return

    # If it's a reply, check whether it's a reply to the bot
    ref_msg = None
    if is_reply:
        try:
            ref_msg = await message.channel.fetch_message(message.reference.message_id)
        except discord.NotFound:
            pass
        if ref_msg is not None and ref_msg.author != bot.user:
            ref_msg = None  # reply to someone else
            if not is_mention:
                return  # not mentioned either, ignore

    query = (
        message.content.replace(f"<@{bot.user.id}>", "")
        .replace(f"<@!{bot.user.id}>", "")
        .strip()
    )
    if not query:
        await message.reply("What birds are you curious about?")
        return

    if ref_msg is not None:
        logger.info("query (reply to %d): %s", ref_msg.id, query)
    else:
        logger.info("query: %s", query)

    # Build context from cache or reply chain
    prior_messages = None
    prior_context = None
    if ref_msg is not None:
        if ref_msg.id in conversation_cache:
            prior_messages = conversation_cache[ref_msg.id]
            logger.info(
                "cache hit for message %d (%d prior messages)",
                ref_msg.id,
                len(prior_messages),
            )
        else:
            prior_context = await build_prior_context(ref_msg)
            logger.info(
                "cache miss for message %d, built context from reply chain", ref_msg.id
            )

    async with message.channel.typing():
        response, stats, updated_messages = await ask_bird_query(
            query, prior_messages=prior_messages, prior_context=prior_context
        )

    footer = (
        f"-# {stats.elapsed_s:.0f}s · "
        f"{stats.input_tokens + stats.output_tokens:,} tokens · "
        f"${stats.cost_usd:.3f} · "
        f"{stats.tool_calls} tool call{'s' if stats.tool_calls != 1 else ''}"
    )
    body = response or "Sorry, I couldn't find anything on that."
    reply = await message.reply(f"{body}\n\n{footer}")
    cache_put(reply.id, updated_messages)


async def start():
    await bot.start(os.environ["PIPER_DISCORD_BOT_TOKEN"])


if __name__ == "__main__":
    import asyncio

    asyncio.run(start())
