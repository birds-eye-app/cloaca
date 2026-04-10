import datetime
import logging
import os
import re

import discord
from discord.ext import tasks

from cloaca.piper.bird_query import ask_bird_query
from cloaca.piper.birdcast import (
    BIRDCAST_CHANNEL_ID,
    birdcast_link_view,
    fetch_birdcast_forecast,
    format_forecast_message,
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


@tasks.loop(time=datetime.time(hour=22, minute=0, tzinfo=datetime.timezone.utc))
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
