import logging
import os
import time
from dataclasses import dataclass

from anthropic import AsyncAnthropic
from anthropic.lib.tools.mcp import async_mcp_tool
from mcp import ClientSession
from mcp.client.sse import sse_client

EBIRD_MCP_URL = os.environ["EBIRD_MCP_URL"]

SYSTEM_PROMPT = """You are a birding assistant for New York City. You answer questions about bird sightings, eBird observations, hotspots, and birding topics — but only for NYC and the surrounding area (the five boroughs, Long Island, New Jersey, Connecticut, and the lower Hudson Valley).

If someone asks about birds or birding outside that region, politely decline and let them know you only cover the NYC area.

If a question is not about birds, bird sightings, eBird, or birding at all, decline politely in one sentence.

Use the eBird MCP tools to look up real, current data. Be concise and direct — your audience are birders who want facts.

## Formatting
Your response will be displayed in Discord. Use Discord-appropriate formatting:
- **bold** for species names and key highlights
- Bullet lists for multiple locations or species
- Avoid markdown tables — use bullet lists or bold/inline formatting for tabular data instead
- Keep responses focused and scannable

## Popular NYC hotspot IDs (use these directly — no need to look them up)
- Central Park: L778316
- Prospect Park: L109516
- Green-Wood Cemetery: L285884
- McGolrick Park: L2987624
- Franz Sigel Park: L1814508"""


@dataclass
class QueryStats:
    elapsed_s: float
    input_tokens: int
    output_tokens: int
    tool_calls: int

    @property
    def cost_usd(self) -> float:
        return (self.input_tokens * 5 + self.output_tokens * 25) / 1_000_000


logger = logging.getLogger(__name__)

client = AsyncAnthropic()


async def ask_bird_query(query: str) -> tuple[str, QueryStats]:
    start = time.monotonic()
    chunks: list[str] = []
    total_input_tokens = 0
    total_output_tokens = 0
    tool_call_count = 0

    async with sse_client(EBIRD_MCP_URL) as (read, write):
        async with ClientSession(read, write) as mcp_client:
            await mcp_client.initialize()
            tools_result = await mcp_client.list_tools()
            logger.info("Connected, %d tools", len(tools_result.tools))

            runner = client.beta.messages.tool_runner(
                model="claude-sonnet-4-6",
                max_tokens=4096,
                thinking={"type": "adaptive"},
                system=SYSTEM_PROMPT,
                tools=[async_mcp_tool(t, mcp_client) for t in tools_result.tools],
                messages=[{"role": "user", "content": query}],
                stream=True,
            )

            async for message_stream in runner:
                async for event in message_stream:
                    if event.type == "content_block_stop":
                        if event.content_block.type == "tool_use":
                            tool_call_count += 1
                            logger.info("tool_call: %s input=%s", event.content_block.name, event.content_block.input)
                    elif event.type == "text":
                        chunks.append(event.text)

                final = await message_stream.get_final_message()
                total_input_tokens += final.usage.input_tokens
                total_output_tokens += final.usage.output_tokens
                logger.info("turn done: stop_reason=%s, output_tokens=%d", final.stop_reason, final.usage.output_tokens)

    stats = QueryStats(
        elapsed_s=time.monotonic() - start,
        input_tokens=total_input_tokens,
        output_tokens=total_output_tokens,
        tool_calls=tool_call_count,
    )
    return "".join(chunks), stats
