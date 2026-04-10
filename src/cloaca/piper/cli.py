import asyncio
import sys

from dotenv import load_dotenv

load_dotenv(override=True)

from cloaca.piper.bird_query import ask_bird_query  # noqa: E402


async def main():
    query = " ".join(sys.argv[1:]) if len(sys.argv) > 1 else input("Query: ")
    print(f">>> {query}\n")
    response, stats, _ = await ask_bird_query(query)
    print(response)
    print(
        f"\n[{stats.elapsed_s:.1f}s · {stats.input_tokens + stats.output_tokens:,} tokens · ${stats.cost_usd:.3f} · {stats.tool_calls} tool calls]"
    )


asyncio.run(main())
