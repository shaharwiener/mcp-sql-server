#!/usr/bin/env python3
"""
Test MCP tools using the proper MCP protocol client.
This demonstrates how to call MCP tools via the protocol, not by direct function calls.
"""
import asyncio
import json
from mcp import ClientSession, StdioServerParameters
from mcp.client.stdio import stdio_client

async def test_mcp_tools():
    """Test MCP tools using the proper protocol."""
    # Configure server to run via stdio
    server_params = StdioServerParameters(
        command="python",
        args=["server.py"],
        env=None
    )
    
    async with stdio_client(server_params) as (read, write):
        async with ClientSession(read, write) as session:
            # Initialize the session
            await session.initialize()
            
            # List available tools
            tools = await session.list_tools()
            print("Available MCP Tools:")
            print("=" * 60)
            for tool in tools.tools:
                print(f"- {tool.name}: {tool.description}")
            print()
            
            # Test query_readonly tool
            print("Testing query_readonly tool:")
            print("=" * 60)
            result = await session.call_tool(
                "query_readonly",
                arguments={
                    "query": "SELECT username, first_name, last_name FROM dbo.Users ORDER BY username",
                    "env": "Int"
                }
            )
            
            if result.content:
                # Parse the result
                result_data = json.loads(result.content[0].text)
                
                if result_data.get("success"):
                    print("User Names:")
                    for row in result_data.get("data", []):
                        username = row.get("username", "N/A")
                        first_name = row.get("first_name", "")
                        last_name = row.get("last_name", "")
                        full_name = f"{first_name} {last_name}".strip() if first_name or last_name else "N/A"
                        print(f"  Username: {username:<20} Name: {full_name}")
                    print(f"\nTotal: {result_data.get('row_count', 0)} users")
                else:
                    print(f"Error: {result_data.get('error')}")

if __name__ == "__main__":
    asyncio.run(test_mcp_tools())

