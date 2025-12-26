"""
Production-Ready SQL Server MCP
"""
import sys
from typing import Dict, Any, Optional

from mcp.server.fastmcp import FastMCP  # type: ignore[import-untyped]
from config.configuration import get_config
from services.core.execution_service import ExecutionService
from services.core.schema_service import SchemaService
from services.analysis.sql_analyzer import SqlAnalyzer
from services.analysis.review_service import ReviewService
from services.common.logging import configure_logging

# Load Config
try:
    config = get_config()
except Exception as e:
    print(f"FATAL: Config load failed: {e}", file=sys.stderr)
    sys.exit(1)

# Initialize Output Logging
# Forces JSON for Docker/K8s environments
configure_logging(log_level=config.server.log_level, json_format=True)

# Initialize MCP
mcp = FastMCP("sql-server-mcp")

# Initialize Services
# We treat them as singletons
execution_service = ExecutionService()
schema_service = SchemaService()
analyzer = SqlAnalyzer()
review_service = ReviewService(sql_analyzer=analyzer, execution_service=execution_service)

@mcp.tool()
def review_sql_script(script: str, env: Optional[str] = None) -> Dict[str, Any]:
    """
    Perform a comprehensive, production-safe review of an SQL script.
    
    Returns a structured QA report with:
    - Security analysis (AST)
    - Performance insights (Execution Plan)
    - Best practice violations
    - Schema validation
    """
    return review_service.review(script, env=env)

@mcp.tool()
def query_readonly(query: str, env: Optional[str] = None, database: Optional[str] = None, page_size: Optional[int] = None, page: Optional[int] = None) -> Dict[str, Any]:
    """
    Exectute a READ-ONLY SQL query (SELECT only).
    
    Strictly prohibits:
    - UPDATE, DELETE, INSERT, MERGE
    - DDL (CREATE, ALTER, DROP)
    - Multi-statement batches
    
    Args:
        query: The SQL SELECT statement.
        env: Optional environment override (Int, Stg, Prd).
        database: Optional database name override.
        page_size: Optional rows per page (max 1000). If provided, page must also be provided. If omitted, returns all rows up to max_rows limit.
        page: Optional page number (1-based). If provided, page_size must also be provided. If omitted, returns all rows up to max_rows limit.
    """
    return execution_service.execute_readonly(query, env, database, page_size=page_size, page=page)

@mcp.tool()
def schema_summary(env: Optional[str] = None, search_term: Optional[str] = None) -> Dict[str, Any]:
    """
    Get a token-efficient summary of the database schema.
    
    Args:
        env: Optional environment override.
        search_term: Optional filter for table or schema names.
    
    Returns:
        List of "TABLE schema.name: col1 (type), col2 (type)..."
    """
    return schema_service.get_summary(env, search_term)

@mcp.tool()
def explain(query: str, env: Optional[str] = None, database: Optional[str] = None) -> Dict[str, Any]:
    """
    Get the estimated execution plan (XML) for a query without executing it.
    """
    return execution_service.get_execution_plan(query, env, database)

@mcp.tool()
def config_info() -> Dict[str, Any]:
    """Return public configuration settings (sanitized)."""
    return {
        "environment": config.environment,
        "available_environments": config.available_environments,
        "safety": {
            "max_rows": config.safety.max_rows,
            "read_only_enforced": True
        }
    }

@mcp.tool()
def get_best_practices() -> Dict[str, Any]:
    """
    Get all SQL Server best practices defined by DBAs.
    Returns comprehensive guidelines for query optimization.
    """
    return analyzer.bp_engine.get_all_practices_documentation()

if __name__ == "__main__":
    import os
    # Get host and port from environment (docker-compose) or config
    server_host = os.getenv("HOST", config.server.host)
    server_port = int(os.getenv("PORT", str(config.server.port)))
    
    print(f"Starting SQL Server MCP ({config.server.transport}) on {server_host}:{server_port}", file=sys.stderr)
    if config.server.transport == "sse":
        # FastMCP uses uvicorn.Server(config) internally, which may hardcode host to 127.0.0.1
        # Monkey-patch uvicorn.Config to force host to 0.0.0.0 for Docker
        import uvicorn.config
        original_config_init = uvicorn.config.Config.__init__
        
        def patched_config_init(self, app, *args, host=None, port=None, **kwargs):
            # Force host to server_host if not explicitly set or if set to 127.0.0.1
            if host is None or host == "127.0.0.1":
                host = server_host
            # Force port to server_port if not explicitly set
            if port is None:
                port = server_port
            return original_config_init(self, app, *args, host=host, port=port, **kwargs)
        
        uvicorn.config.Config.__init__ = patched_config_init
        
        # Set environment variables as well
        os.environ["PORT"] = str(server_port)
        os.environ["HOST"] = server_host
        
        # FastMCP SSE transport issue:
        # - FastMCP normalizes message_path with mount_path to create absolute path
        # - But SseServerTransport uses root_path from scope + endpoint
        # - Cursor prepends base URL (/sse) to the endpoint, causing double /sse
        # Solution: Pass relative path to SseServerTransport and use Route (not Mount)
        # for messages endpoint to avoid root_path issues
        from mcp.server.fastmcp.server import FastMCP  # type: ignore[import-untyped]
        original_sse_app = FastMCP.sse_app
        
        def patched_sse_app(self, mount_path=None):
            """Patched sse_app that passes relative path to SseServerTransport."""
            from starlette.middleware import Middleware
            from starlette.routing import Route
            from mcp.server.sse import SseServerTransport
            from starlette.responses import Response
            from starlette.requests import Request
            from typing import TYPE_CHECKING
            
            if TYPE_CHECKING:
                from starlette.types import Scope, Receive, Send
            
            # Update mount_path in settings if provided
            if mount_path is not None:
                self.settings.mount_path = mount_path
            
            # Use relative path for SseServerTransport (what gets sent to client)
            # root_path will be empty (no mount_path in mcp.run()), so SseServerTransport sends: "" + "/messages/" = "/messages/"
            relative_message_path = self.settings.message_path  # e.g., "/messages/"
            # For route registration, we need absolute path: /sse/messages/
            # Since we're not using mount_path, we construct it manually
            absolute_message_path = "/sse" + relative_message_path  # e.g., "/sse/messages/"
            
            # Create SseServerTransport with relative path
            # When root_path is /sse (from mount_path), it will send /sse + /messages/ = /sse/messages/
            sse = SseServerTransport(
                relative_message_path,  # Pass relative path!
                security_settings=self.settings.transport_security,
            )
            
            # Create ASGI app classes for both endpoints
            class SseApp:
                def __init__(self, sse_transport, mcp_server):
                    self.sse = sse_transport
                    self.mcp_server = mcp_server
                
                async def __call__(self, scope, receive, send):
                    async with self.sse.connect_sse(scope, receive, send) as streams:
                        await self.mcp_server.run(
                            streams[0],
                            streams[1],
                            self.mcp_server.create_initialization_options(),
                        )
            
            class MessageApp:
                def __init__(self, sse_transport):
                    self.sse = sse_transport
                
                async def __call__(self, scope, receive, send):
                    await self.sse.handle_post_message(scope, receive, send)
            
            routes: list = []
            middleware: list = []
            
            # Register SSE endpoint - use Route with ASGI app class
            # Route can accept an ASGI app directly
            routes.append(Route(self.settings.sse_path, endpoint=SseApp(sse, self._mcp_server), methods=["GET"]))
            
            # Register POST endpoint for messages at both paths:
            # 1. /sse/messages/ (absolute path with /sse prefix)
            # 2. /messages/ (what Cursor actually uses - absolute from root)
            # Use Route (not Mount) to handle POST requests with query params
            routes.append(Route(absolute_message_path, endpoint=MessageApp(sse), methods=["POST"]))
            routes.append(Route(relative_message_path, endpoint=MessageApp(sse), methods=["POST"]))  # Also register at /messages/
            
            # Add custom routes
            routes.extend(self._custom_starlette_routes)
            
            from starlette.applications import Starlette
            return Starlette(debug=self.settings.debug, routes=routes, middleware=middleware)
        
        FastMCP.sse_app = patched_sse_app
        
        # Configure paths
        # Don't use mount_path - define absolute paths directly
        # This way root_path will be empty, and SseServerTransport will send: "" + "/messages/" = "/messages/"
        # Cursor will then prepend /sse to get /sse/messages/
        mcp.settings.sse_path = "/sse"
        mcp.settings.message_path = "/messages/"  # Relative - will be sent to client
        mcp.run(transport="sse")  # No mount_path
    else:
        mcp.run(transport=config.server.transport)