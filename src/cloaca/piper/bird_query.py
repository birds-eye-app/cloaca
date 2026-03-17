import logging
import os
import time
from contextlib import AsyncExitStack
from dataclasses import dataclass

from anthropic import AsyncAnthropic
from anthropic.lib.tools.mcp import async_mcp_tool
from mcp import ClientSession, StdioServerParameters
from mcp.client.sse import sse_client
from mcp.client.stdio import stdio_client

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
- Franz Sigel Park: L1814508

## Local Bird Database (DuckDB)
You have tools to query a local database of bird observation data.

**`list_tables` / `list_columns`** — use these first to explore available tables and their schemas.

**`execute_query`** — run read-only SQL (DuckDB dialect). Results capped at 1024 rows.

**`localities_hotspots` table** — monthly hotspot stats per locality. Good for:
- Finding hotspots by name (fuzzy search with ILIKE)
- Ranking hotspots by diversity (`common_and_uncommon_species`) or activity (`avg_weekly_number_of_observations`) for a given month
- Looking up a locality_id when you only know the name

Columns: `locality_id`, `locality_name`, `latitude`, `longitude`, `locality_type`, `month` (1–12), `avg_weekly_number_of_observations`, `common_species`, `uncommon_species`, `common_and_uncommon_species`

**Do NOT use the local DB for recent sightings** — use the eBird API tools for that."""


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


async def ask_bird_query(
    query: str,
    prior_messages: list[dict] | None = None,
    prior_context: str | None = None,
) -> tuple[str, QueryStats, list[dict]]:
    start = time.monotonic()
    chunks: list[str] = []
    total_input_tokens = 0
    total_output_tokens = 0
    tool_call_count = 0

    if prior_messages:
        messages = prior_messages + [{"role": "user", "content": query}]
    elif prior_context:
        messages = [{"role": "user", "content": f"{prior_context}\n\nNew question: {query}"}]
    else:
        messages = [{"role": "user", "content": query}]

    duck_db_path = os.environ.get("DUCK_DB_PATH")

    async with AsyncExitStack() as stack:
        # eBird SSE connection
        ebird_read, ebird_write = await stack.enter_async_context(sse_client(EBIRD_MCP_URL))
        ebird_client = await stack.enter_async_context(ClientSession(ebird_read, ebird_write))
        await ebird_client.initialize()
        ebird_tools = await ebird_client.list_tools()
        logger.info("eBird connected, %d tools", len(ebird_tools.tools))

        tools = [async_mcp_tool(t, ebird_client) for t in ebird_tools.tools]

        # DuckDB stdio connection (optional)
        if duck_db_path:
            duck_params = StdioServerParameters(
                command="uvx",
                args=["mcp-server-motherduck", "--db-path", duck_db_path],
            )
            duck_read, duck_write = await stack.enter_async_context(stdio_client(duck_params))
            duck_client = await stack.enter_async_context(ClientSession(duck_read, duck_write))
            await duck_client.initialize()
            duck_tools = await duck_client.list_tools()
            logger.info("DuckDB connected, %d tools", len(duck_tools.tools))
            tools += [async_mcp_tool(t, duck_client) for t in duck_tools.tools]

        runner = client.beta.messages.tool_runner(
            model="claude-sonnet-4-6",
            max_tokens=4096,
            thinking={"type": "adaptive"},
            system=SYSTEM_PROMPT,
            tools=tools,
            messages=messages,
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
    response_text = "".join(chunks)
    updated_messages = messages + [{"role": "assistant", "content": response_text}]
    return response_text, stats, updated_messages
