import os

import discord

from cloaca.piper.bird_query import ask_bird_query

intents = discord.Intents.default()
intents.message_content = True

bot = discord.Client(intents=intents)


@bot.event
async def on_ready():
    print(f"[piper] online as {bot.user}")


@bot.event
async def on_message(message: discord.Message):
    if message.author == bot.user:
        return
    if bot.user not in message.mentions:
        return

    query = message.content.replace(f"<@{bot.user.id}>", "").replace(f"<@!{bot.user.id}>", "").strip()

    if not query:
        await message.reply("What birds are you curious about?")
        return

    async with message.channel.typing():
        response, stats = await ask_bird_query(query)

    footer = (
        f"-# {stats.elapsed_s:.0f}s · "
        f"{stats.input_tokens + stats.output_tokens:,} tokens · "
        f"${stats.cost_usd:.3f} · "
        f"{stats.tool_calls} tool call{'s' if stats.tool_calls != 1 else ''}"
    )

    body = response or "Sorry, I couldn't find anything on that."
    await message.reply(f"{body}\n\n{footer}")


bot.run(os.environ["PIPER_DISCORD_BOT_TOKEN"])
