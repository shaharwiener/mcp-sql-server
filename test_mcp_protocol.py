#!/usr/bin/env python3
"""
Test MCP tools using the proper MCP protocol via stdio.
This demonstrates calling MCP tools through the protocol, not direct function calls.
"""
import asyncio
import json
import sys
from mcp import ClientSession, StdioServerParameters
from mcp.client.stdio import stdio_client

async def call_mcp_tool():
    """Call MCP tool via protocol to get user names."""
    # Configure server to run via stdio (same as Cursor would)
    server_params = StdioServerParameters(
        command="python",
        args=["/app/server.py"],
        env=None
    )
    
    async with stdio_client(server_params) as (read, write):
        async with ClientSession(read, write) as session:
            # Initialize the session
            await session.initialize()
            
            # Call the query_readonly tool via MCP protocol
            print("Calling query_readonly via MCP protocol...")
            result = await session.call_tool(
                "query_readonly",
                arguments={
                    "query": "SELECT username, first_name, last_name FROM dbo.Users ORDER BY username",
                    "env": "Int"
                }
            )
            
            # Parse the result
            if result.content and len(result.content) > 0:
                result_text = result.content[0].text
                result_data = json.loads(result_text)
                
                if result_data.get("success"):
                    print("\nUser Names:")
                    print("=" * 60)
                    for row in result_data.get("data", []):
                        username = row.get("username", "N/A")
                        first_name = row.get("first_name", "")
                        last_name = row.get("last_name", "")
                        full_name = f"{first_name} {last_name}".strip() if first_name or last_name else "N/A"
                        print(f"  Username: {username:<20} Name: {full_name}")
                    print(f"\nTotal: {result_data.get('row_count', 0)} users")
                    print(f"Execution time: {result_data.get('execution_time_ms', 0):.2f}ms")
                else:
                    print(f"Error: {result_data.get('error')}")
            else:
                print("No result content received")

if __name__ == "__main__":
    # Run from inside container where server.py is accessible
    asyncio.run(call_mcp_tool())

