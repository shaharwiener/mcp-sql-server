#!/usr/bin/env python3
"""
Get tables using MCP protocol client (proper way to call MCP tools).
"""
import asyncio
import json
import sys
from mcp import ClientSession, StdioServerParameters
from mcp.client.stdio import stdio_client

async def get_tables_via_mcp():
    """Get tables using the schema_summary MCP tool."""
    print("Connecting to MCP server via SSE...")
    
    # For SSE transport, we need to use HTTP client, not stdio
    # But let's try using the server directly via HTTP
    import httpx
    
    try:
        async with httpx.AsyncClient() as client:
            # Connect to SSE endpoint
            async with client.stream("GET", "http://localhost:9303/sse") as response:
                if response.status_code != 200:
                    print(f"Failed to connect: {response.status_code}")
                    return
                
                # Read the endpoint from SSE
                async for line in response.aiter_lines():
                    if line.startswith("event: endpoint"):
                        continue
                    elif line.startswith("data: "):
                        endpoint = line[6:]  # Remove "data: " prefix
                        print(f"Got endpoint: {endpoint}")
                        break
                
                # Now use the messages endpoint
                messages_url = f"http://localhost:9303{endpoint}"
                print(f"Using messages endpoint: {messages_url}")
                
                # Send initialize request
                init_request = {
                    "jsonrpc": "2.0",
                    "method": "initialize",
                    "params": {
                        "protocolVersion": "2024-11-05",
                        "capabilities": {},
                        "clientInfo": {
                            "name": "table-lister",
                            "version": "1.0.0"
                        }
                    },
                    "id": 1
                }
                
                async with client.post(messages_url, json=init_request) as resp:
                    init_response = await resp.json()
                    print(f"Initialize response: {json.dumps(init_response, indent=2)}")
                
                # List tools
                list_tools_request = {
                    "jsonrpc": "2.0",
                    "method": "tools/list",
                    "id": 2
                }
                
                async with client.post(messages_url, json=list_tools_request) as resp:
                    tools_response = await resp.json()
                    print(f"\nAvailable tools: {json.dumps(tools_response, indent=2)}")
                
                # Call schema_summary tool
                call_tool_request = {
                    "jsonrpc": "2.0",
                    "method": "tools/call",
                    "params": {
                        "name": "schema_summary",
                        "arguments": {
                            "env": "Int"
                        }
                    },
                    "id": 3
                }
                
                async with client.post(messages_url, json=call_tool_request) as resp:
                    result = await resp.json()
                    print(f"\nSchema Summary Result:")
                    print(json.dumps(result, indent=2))
                    
                    if "result" in result and "content" in result["result"]:
                        content = result["result"]["content"][0]
                        if "text" in content:
                            data = json.loads(content["text"])
                            if data.get("success"):
                                print("\n" + "=" * 60)
                                print("Tables in database:")
                                print("=" * 60)
                                for table_info in data.get("summary", []):
                                    print(f"  {table_info}")
                                print(f"\nTotal: {data.get('count', 0)} tables")
                            else:
                                print(f"Error: {data.get('error')}")
    
    except Exception as e:
        print(f"Error: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    asyncio.run(get_tables_via_mcp())


