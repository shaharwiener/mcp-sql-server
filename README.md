# SQL Server MCP

A secure Model Context Protocol (MCP) server for Microsoft SQL Server that provides safe, read-only database access with comprehensive protection layers.

## Features

- **Read-Only Query Execution** - Execute SELECT queries with automatic pagination support
- **SQL Script Review** - Comprehensive analysis with 42 automated best practice checks and risk scoring
- **Schema Discovery** - Get database metadata (tables, columns, types) in token-efficient format
- **Execution Plan Analysis** - Retrieve and analyze SQL Server query execution plans
- **Multi-Environment Support** - Switch between Int, Stg, and Prd environments
- **Best Practices Engine** - Access 50 DBA-defined SQL Server best practice rules

## How the MCP Protects the Database

The MCP implements **multi-layer protection** at both the **SQL Server (database) level** and the **MCP (application) level**:

### Database-Level Protections (SQL Server Query Hints)

These protections are enforced **directly by SQL Server** through query hints automatically injected into every query:

1. **CPU Control (MAXDOP)** - Limits CPU parallelism per query:
   - **Production**: `MAXDOP 1` (single-threaded, uses 1 CPU core)
   - **Staging**: `MAXDOP 2` (limited parallelism)
   - **Development**: `MAXDOP 4` (allows parallelism)
   - **Prevents**: A single query from consuming all CPU cores

2. **Memory Control (MAX_GRANT_PERCENT)** - Limits memory allocation per query:
   - **Production**: `MAX_GRANT_PERCENT = 10` (10% of available query memory)
   - **Staging**: `MAX_GRANT_PERCENT = 25` (25% of available query memory)
   - **Development**: `MAX_GRANT_PERCENT = 50` (50% of available query memory)
   - **Prevents**: A single query from consuming excessive SQL Server memory

3. **Lock Prevention (NOLOCK)** - Prevents read blocking in production:
   - Automatically adds `WITH (NOLOCK)` hints to all table references
   - **Production only**: Prevents queries from blocking write operations
   - **Configurable**: Can be enabled/disabled per environment

4. **Query Execution Timeout** - Enforces maximum query execution time:
   - Default: 60s (Int: 120s, Stg: 90s, Prd: 30s, Max: 300s)
   - Set via ODBC connection timeout (MCP sets the value)
   - **SQL Server automatically cancels queries that exceed timeout**
   - **Prevents**: Long-running queries from consuming resources indefinitely

**How it works:** 
- Query hints (MAXDOP, MAX_GRANT_PERCENT, NOLOCK): The MCP automatically appends `OPTION (MAXDOP n, MAX_GRANT_PERCENT = x)` or `WITH (NOLOCK)` to queries. SQL Server enforces these limits during query execution.
- Query timeout: The MCP sets the timeout value via ODBC connection properties. SQL Server enforces it by automatically canceling queries that exceed the timeout.

### MCP-Level Protections (Application-Level Limits)

These protections are enforced **by the MCP application** before and during query execution:

1. **Concurrency Throttling** - Limits concurrent queries per environment and user:
   - Max 5 concurrent queries per environment
   - Max 2 concurrent queries per user
   - **Prevents**: Resource exhaustion from too many simultaneous queries

2. **AST Validation** - Strict read-only enforcement using SQL parsing:
   - Blocks INSERT, UPDATE, DELETE, DDL operations
   - Blocks multi-statement batches
   - **Prevents**: Accidental or malicious write operations

3. **Query Cost Checking** - Blocks expensive queries before execution:
   - Analyzes SQL Server execution plan cost (estimates CPU, I/O, memory)
   - Default threshold: 50 (Int: 100, Stg: 50, Prd: 10)
   - **Prevents**: CPU-intensive queries from running

4. **Result Set Size Limits** - Prevents large result sets:
   - **Payload Size Limit**: Default 1MB per query result (configurable)
   - **Row Limit**: Default 1000 rows (Int: 10000, Stg: 5000, Prd: 500)
   - **Batch Fetching**: Uses `fetchmany` instead of `fetchall` to control memory
   - **Automatic Truncation**: Large text values (>1000 chars) are truncated
   - **Prevents**: Excessive memory consumption in the MCP application

5. **Allowed Databases** - Optional whitelist of databases that can be queried
   - **Prevents**: Access to unauthorized databases

**Additional MCP Protections:**
- **Linked Server Blocking** - Detects and blocks queries using OPENQUERY, OPENDATASOURCE, OPENROWSET, and four-part names
- **Connection Pooling** - Efficient connection management with configurable pool size
- **Structured Logging** - All operations logged in JSON format for audit and monitoring

### Protection Summary

| Protection Type | Level | What It Controls | How It Works |
|----------------|-------|------------------|--------------|
| **MAXDOP** | Database | CPU cores per query | SQL Server query hint (`OPTION (MAXDOP n)`) |
| **MAX_GRANT_PERCENT** | Database | Memory per query | SQL Server query hint (`OPTION (MAX_GRANT_PERCENT = x)`) |
| **NOLOCK** | Database | Read locks | SQL Server table hint (`WITH (NOLOCK)`) |
| **Query Timeout** | Database | Query execution time | ODBC timeout (MCP sets value, SQL Server enforces cancellation) |
| **Concurrency Throttling** | MCP | Concurrent queries | Application-level semaphore |
| **Query Cost Check** | MCP | Query complexity | Pre-execution plan analysis |
| **Result Size Limits** | MCP | Result set size | Application-level validation |
| **AST Validation** | MCP | Query type (read-only) | Pre-execution SQL parsing |

## How to Use

### Quick Start with Docker

1. **Start the services:**
   ```bash
   docker-compose up -d
   ```

2. **Configure Cursor MCP:**
   - Open Cursor Settings → Features → MCP
   - Add new MCP server:
     - **Command:** `docker`
     - **Args:** `exec -i mcp-sql-server python server.py`

3. **Use the MCP tools:**
   - Query data: "What tables exist in the database?"
   - Review SQL: "Review this query: SELECT * FROM Users"
   - Get schema: "Show me the database schema"

### MCP Tools

#### `query_readonly` - Execute Safe Queries
```python
query_readonly(
    query="SELECT id, username FROM dbo.Users WHERE is_active = 1",
    env="Int",  # Optional: Int, Stg, or Prd
    database="MyAppDB",  # Optional
    page_size=10,  # Optional: pagination
    page=1  # Optional: page number
)
```

#### `review_sql_script` - Analyze SQL Scripts
```python
review_sql_script(
    script="SELECT * FROM Users WHERE YEAR(created_date) = 2024",
    env="Int"
)
# Returns: risk_score, findings, best_practice_warnings
```

#### `schema_summary` - Get Database Schema
```python
schema_summary(
    env="Int",
    search_term="user"  # Optional: filter tables
)
```

#### `explain` - Get Execution Plan
```python
explain(
    query="SELECT * FROM dbo.Users WHERE id = 1",
    env="Int"
)
```

#### `get_best_practices` - List All Rules
```python
get_best_practices()
# Returns: Complete list of 50 best practice rules
```

#### `config_info` - Get Server Configuration
```python
config_info()
# Returns: Current MCP server settings
```

### Configuration

**Environment Variables (Required):**
```bash
DB_CONNECTION_STRING_INT="Driver={ODBC Driver 18...};Server=...;Database=...;Uid=...;Pwd=..."
DB_CONNECTION_STRING_STG="..."  # Optional
DB_CONNECTION_STRING_PRD="..."  # Optional
```

**Config File:** `config/config.yaml` - Defines limits, timeouts, and environment-specific settings.

### Database User Permissions

The database user should have **ONLY**:
- `db_datareader` on target databases
- `VIEW SERVER STATE` for execution plan analysis

**Never grant:** `db_datawriter`, `db_ddladmin`, or `sysadmin` roles.

---

**Requirements:** Python 3.11+, Docker (for local testing), ODBC Driver 18 for SQL Server
