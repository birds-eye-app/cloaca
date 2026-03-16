import os
import re

import discord
from tabulate import tabulate

from cloaca.piper.bird_query import ask_bird_query

intents = discord.Intents.default()
intents.message_content = True

bot = discord.Client(intents=intents)


def format_for_discord(text: str) -> str:
    """Convert markdown tables to code blocks using tabulate."""
    lines = text.split("\n")
    out: list[str] = []
    table: list[str] = []

    def flush_table():
        if not table:
            return
        rows = []
        for line in table:
            cells = [c.strip() for c in line.strip().strip("|").split("|")]
            rows.append(cells)
        # Remove separator row (e.g. |---|---|)
        rows = [r for r in rows if not all(re.match(r"^-+$", c) for c in r)]
        if rows:
            formatted = tabulate(rows[1:], headers=rows[0], tablefmt="simple")
            out.append(f"```\n{formatted}\n```")
        table.clear()

    for line in lines:
        if re.match(r"^\s*\|", line):
            table.append(line)
        else:
            flush_table()
            out.append(line)

    flush_table()
    return "\n".join(out)


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

    body = format_for_discord(response) if response else "Sorry, I couldn't find anything on that."
    await message.reply(f"{body}\n\n{footer}")


bot.run(os.environ["PIPER_DISCORD_BOT_TOKEN"])
