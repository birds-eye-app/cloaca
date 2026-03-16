import os

import discord

from cloaca.piper.bird_query import stream_bird_query

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
        chunks = []
        async for chunk in stream_bird_query(query):
            chunks.append(chunk)
        response = "".join(chunks)

    await message.reply(response or "Sorry, I couldn't find anything on that.")


bot.run(os.environ["PIPER_DISCORD_BOT_TOKEN"])
