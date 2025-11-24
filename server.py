"""
Simple SQL Server MCP
Provides clean access to SQL Server databases with only 2 tools: get_query and get_scheme
Optimized for AI/LLM usage with minimal token consumption
"""
import os
import sys
import json
from typing import Dict, Any, List, Optional
from dotenv import load_dotenv
from mcp.server.fastmcp import FastMCP

# Import our services
from services.sql_review.execution_service import ExecutionService
from services.sql_review.schema_service import SchemaService
from services.sql_review.performance_service import PerformanceService
from services.infrastructure.auth_service import AuthService
from services.infrastructure.audit_service import AuditService

# Load environment variables from .env and .env.local
# Use absolute path to ensure .env.local is found regardless of working directory
from pathlib import Path
script_dir = Path(__file__).parent
load_dotenv(script_dir / '.env')  # Load .env first
load_dotenv(script_dir / '.env.local', override=True)  # Then .env.local (overrides .env)

# --- Utility Functions ---
def safe_log(msg: str):
    """Log a debug message as JSON to stderr"""
    print(json.dumps({"type": "debug", "message": msg}), file=sys.stderr)

# --- Initialize MCP ---
mcp = FastMCP("sql-server-mcp")

# --- Configuration ---
env = os.getenv('PANGO_ENV', 'Int')
safe_log(f"Initializing SQL Server MCP with environment: {env}")

# --- Initialize Services ---
try:
    execution_service = ExecutionService()
    schema_service = SchemaService()
    performance_service = PerformanceService()
    auth_service = AuthService()
    audit_service = AuditService()
    
    # Log loaded configuration
    from config.settings import settings
    safe_log(f"✅ Services initialized successfully")
    safe_log(f"📊 Loaded databases: {settings.allowed_databases}")
    safe_log(f"🔧 Connection strings configured: {len(settings.connection_strings)}")
except Exception as e:
    safe_log(f"❌ Failed to initialize services: {e}")
    # Use fallback initialization
    execution_service = None
    schema_service = None
    performance_service = None
    auth_service = None
    audit_service = None

# ==================== Query Tool ====================

@mcp.tool()
def get_query(query: str, database: Optional[str] = None, user_id: Optional[str] = None) -> Dict[str, Any]:
    """
    Execute SELECT queries on SQL Server databases with automatic safety validation.
    
    Args:
        query: Raw SQL SELECT statement to execute
        database: Optional database override (mcpay/mobydom5/billing). 
                 If not provided, auto-detects from query content.
        user_id: Optional user identifier for audit logging
        
    Returns:
        Dictionary containing:
        - success: Boolean indicating if query executed successfully
        - data: Query results as list of dictionaries
        - database_used: Which database was queried
        - row_count: Number of rows returned
        - error: Error message if query failed
        - warning: Optional warning message (e.g., for result truncation)
        
    Safety Features:
        - Only SELECT statements allowed (blocks UPDATE/INSERT/DELETE)
        - Automatic database detection from query content
        - Query length and complexity validation
        - Result set size limits (max 10,000 rows)
        - Comprehensive audit logging
        - Clean error handling optimized for AI consumption
        
    Examples:
        - get_query("SELECT TOP 5 * FROM accounts WHERE active = 1")
        - get_query("SELECT * FROM MyDB..Users WHERE email = 'user@example.com'")
        - get_query("SELECT COUNT(*) as total FROM MyDB..accounts", "MyDB")
    """
    if not execution_service:
        return {
            "success": False,
            "error": "Query service not initialized. Check configuration.",
            "columns": [],
            "rows": [],
            "row_count": 0
        }
    
    # Get user_id from auth service if available
    if user_id is None and auth_service:
        user_id = auth_service.get_user_id() if auth_service else "system"
    
    try:
        # Use configured database or the one specified in the call
        from config.settings import settings
        # If no database specified, use the first available database
        if not database:
            if settings.allowed_databases:
                target_db = settings.allowed_databases[0]
            else:
                return {
                    "success": False,
                    "error": "No databases configured. Please set DB_CONN_<NAME> environment variables.",
                    "columns": [],
                    "rows": [],
                    "row_count": 0
                }
        else:
            target_db = database
        return execution_service.execute_sql_query(target_db, query)
    except Exception as e:
        safe_log(f"Error in get_query: {str(e)}")
        # Log the error
        if audit_service:
            audit_service.log_query(
                database=database or "unknown",
                query=query,
                success=False,
                user_id=user_id,
                error=str(e)
            )
        return {
            "success": False,
            "error": "Query execution failed",
            "data": [],
            "database_used": database,
            "row_count": 0
        }

# ==================== Schema Tool ====================

@mcp.tool()
def get_scheme(database: str, scope: str = "tables", user_id: Optional[str] = None) -> Dict[str, Any]:
    """
    Retrieve database schema information for SQL Server databases.
    
    Args:
        database: Target database (mcpay/mobydom5/billing)
        scope: Schema scope to retrieve:
              - "tables": List all tables and views
              - "full": Complete schema with columns, types, constraints
              - "table:{name}": Detailed info for specific table (e.g., "table:accounts")
        user_id: Optional user identifier for audit logging
        
    Returns:
        Dictionary containing:
        - success: Boolean indicating if operation succeeded  
        - database: Database that was queried
        - scope: Scope that was requested
        - schema_info: Schema information based on scope:
          * tables: List of table/view names with basic info
          * full: Complete database schema structure
          * table:name: Detailed table structure (columns, types, constraints, indexes)
        - error: Error message if operation failed
        
    Safety Features:
        - Database name validation
        - Table name validation (prevents SQL injection)
        - Comprehensive audit logging
        - Generic error messages (details only in logs)
        
    Examples:
        - get_scheme("MyDB", "tables") - List all tables in database
        - get_scheme("MyDB", "full") - Complete database schema
        - get_scheme("MyDB", "table:accounts") - Detailed accounts table structure
        - get_scheme("MyDB", "table:Users") - Users table details
    """
    if not schema_service:
        return {
            "success": False,
            "error": "Schema service not initialized. Check configuration.",
            "database": database,
            "scope": scope,
            "schema_info": {}
        }
    
    # Set user_id for audit logging
    if not user_id:
        user_id = auth_service.get_user_id() if auth_service else "system"
    
    try:
        return schema_service.get_schema(database, scope, user_id=user_id)
    except Exception as e:
        safe_log(f"Error in get_scheme: {str(e)}")
        # Log the error
        if audit_service:
            audit_service.log_schema_access(
                database=database,
                scope=scope,
                success=False,
                user_id=user_id,
                error=str(e)
            )
        return {
            "success": False,
            "error": "Schema retrieval failed",
            "database": database,
            "scope": scope,
            "schema_info": {}
        }

# ==================== Performance Tool ====================

@mcp.tool()
def get_execution_plan(query: str, database: Optional[str] = None) -> Dict[str, Any]:
    """
    Get the SQL Server execution plan for a query.
    
    Args:
        query: The SQL query to analyze
        database: Target database. If not provided, uses the first available database.
        
    Returns:
        Dictionary containing:
        - success: Boolean indicating if plan retrieval succeeded
        - plan: XML execution plan as string
        - error: Error message if failed
    """
    safe_log(f"🔍 get_execution_plan called for query: {query[:50]}... database: {database}")
    
    if not performance_service:
        return {
            "success": False,
            "error": "Performance service not initialized. Check configuration.",
            "plan": None
        }
        
    try:
        # Use configured database or the one specified in the call
        from config.settings import settings
        # If no database specified, use the first available database
        if not database:
            if settings.allowed_databases:
                target_db = settings.allowed_databases[0]
            else:
                return {
                    "success": False,
                    "error": "No databases configured. Please set DB_CONN_<NAME> environment variables.",
                    "plan": None
                }
        else:
            target_db = database
            
        return performance_service.get_execution_plan(target_db, query)
    except Exception as e:
        safe_log(f"Error in get_execution_plan: {str(e)}")
        return {
            "success": False,
            "error": f"Failed to get execution plan: {str(e)}",
            "plan": None
        }

# ==================== Environment Info Tool ====================

@mcp.tool()
def get_environment_info() -> Dict[str, Any]:
    """
    Get information about current SQL Server MCP configuration.
    
    Returns:
        Environment and service status information
    """
    from config.settings import settings
    
    return {
        "current_environment": env,
        "services_initialized": {
            "query_service": execution_service is not None,
            "schema_service": schema_service is not None,
            "performance_service": performance_service is not None,
            "auth_service": auth_service is not None,
            "audit_service": audit_service is not None
        },
        "supported_databases": settings.allowed_databases,
        "supported_operations": {
            "get_query": "Execute SELECT queries with auto-detection and validation",
            "get_scheme": "Retrieve database schema information",
            "get_execution_plan": "Get SQL Server execution plan for a query"
        },
        "security_features": {
            "authentication_enabled": auth_service.enabled if auth_service else False,
            "audit_logging_enabled": settings.AUDIT_LOG_ENABLED,
            "max_query_length": settings.MAX_QUERY_LENGTH,
            "max_result_rows": settings.MAX_RESULT_ROWS,
            "query_validation": "Enabled (SELECT-only, pattern detection)"
        },
        "configuration": {
            "PANGO_ENV": "Environment to use (Int/Stg/Prd)",
            "AUTH_ENABLED": "Enable SSO authentication",
            "AUDIT_LOG_ENABLED": "Enable audit logging",
            "AUDIT_LOG_PATH": settings.AUDIT_LOG_PATH
        }
    }

# --- Main Entrypoint ---
if __name__ == "__main__":
    import time
    
    # Read transport config from environment variables
    transport = os.getenv("MCP_TRANSPORT", "stdio")
    host = os.getenv("MCP_HOST", "127.0.0.1")
    port = int(os.getenv("MCP_PORT", "9303"))
    
    safe_log(f"🚀 Starting SQL Server MCP server in '{transport}' mode")
    if transport in ["socket", "sse"]:
        safe_log(f"📡 Server will listen on {host}:{port}")
    
    
    # Track start time
    run_start = int(time.time())
    safe_log(f"SQL Server MCP started at {run_start}")
    
    # Run the server with proper configuration
    if transport == "sse":
        # For SSE mode, use FastMCP's sse_app with uvicorn
        import uvicorn
        
        # Get the SSE app from FastMCP
        app = mcp.sse_app(mount_path="/sse")
        
        # Run uvicorn with explicit host and port
        safe_log(f"Starting uvicorn on {host}:{port} with /sse endpoint")
        uvicorn.run(app, host=host, port=port, log_level="info")
    else:
        # For stdio mode, use the standard run method
        mcp.run(transport=transport)
    
    # Track end time
    run_end = int(time.time())
    duration = run_end - run_start
    safe_log(f"SQL Server MCP ended at {run_end} — duration: {duration} seconds")