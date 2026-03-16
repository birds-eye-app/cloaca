from typing import AsyncGenerator

from anthropic import AsyncAnthropic
from anthropic.lib.tools.mcp import async_mcp_tool
from mcp import ClientSession
from mcp.client.sse import sse_client

EBIRD_MCP_URL = "EBIRD_MCP_URL_REDACTED"

SYSTEM_PROMPT = """You are a birding assistant. You answer questions about bird sightings, eBird observations, hotspots, and birding topics only.

Use the eBird MCP tools to look up real, current data when answering questions. Be concise and direct — your audience are birders who want facts.

If a question is not about birds, bird sightings, eBird, or birding, decline politely in one sentence and offer to help with a birding question instead."""

client = AsyncAnthropic()


async def stream_bird_query(query: str) -> AsyncGenerator[str, None]:
    async with sse_client(EBIRD_MCP_URL) as (read, write):
        async with ClientSession(read, write) as mcp_client:
            await mcp_client.initialize()
            tools_result = await mcp_client.list_tools()
            print(f"[bird_query] Connected, {len(tools_result.tools)} tools")

            runner = client.beta.messages.tool_runner(
                model="claude-opus-4-6",
                max_tokens=4096,
                thinking={"type": "adaptive"},
                system=SYSTEM_PROMPT,
                tools=[async_mcp_tool(t, mcp_client) for t in tools_result.tools],
                messages=[{"role": "user", "content": query}],
                stream=True,
            )

            async for message_stream in runner:
                async for event in message_stream:
                    if (
                        event.type == "content_block_start"
                        and event.content_block.type == "tool_use"
                    ):
                        print(f"[bird_query] tool_call: {event.content_block.name}")
                    elif (
                        event.type == "content_block_delta"
                        and event.delta.type == "text_delta"
                    ):
                        yield event.delta.text

                final = await message_stream.get_final_message()
                print(
                    f"[bird_query] turn done: stop_reason={final.stop_reason}, output_tokens={final.usage.output_tokens}"
                )
