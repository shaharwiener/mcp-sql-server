# SQL Server MCP Connector

A Model Context Protocol (MCP) server that provides safe, read-optimized access to SQL Server databases for AI assistants like Claude and Cursor.

## Features

- **Safe Query Execution**: Read-only access (SELECT) with automatic safety validation
- **Schema Inspection**: Retrieve database schemas, table definitions, and metadata
- **Performance Analysis**: Get execution plans, index suggestions, and performance metrics
- **Security**:
  - Prevents SQL injection and dangerous commands
  - Configurable row limits and timeouts
  - Comprehensive audit logging for all operations
- **Cloud Ready**: Dockerized and ready for AWS Fargate deployment
- **Multi-Database Support**: Connect to multiple databases simultaneously

## Architecture

### Configuration System

The project uses a centralized configuration approach with `pydantic-settings`:

- **Environment Variables**: All settings configurable via env vars
- **`.env` Files**: Support for `.env` and `.env.local` files
- **Settings Module**: Single source of truth in `config/settings.py`
- **No Hardcoded Values**: All defaults are configurable

### Available Tools (26 Total)

> **Security Note**: Tools that could execute write operations (UPDATE, DELETE, INSERT) are disabled by default. This includes `execute_stored_procedure` and `execute_sql_script_transactional`. These are commented out in the code and can be re-enabled if proper validation and authentication are implemented.

#### Execution Tools (3)

1. `execute_sql_query` - Execute SELECT queries with safety validation
   ~~2. `execute_stored_procedure` - Execute stored procedures~~ **DISABLED** (can perform writes)
   ~~3. `execute_sql_script_transactional` - Execute SQL scripts transactionally~~ **DISABLED** (can perform writes)
2. `get_execution_plan` - Get query execution plan
3. `parse_sql_script` - Parse and validate SQL syntax without executing (safe)

#### Performance Tools (5)

4. `get_slow_queries` - Identify slow-running queries
5. `get_query_statistics` - Get query performance statistics
6. `get_wait_statistics` - Analyze database wait statistics
7. `get_cache_hit_ratio` - Monitor buffer cache efficiency
8. `get_index_fragmentation` - Check index fragmentation levels

#### Schema Tools (6)

9. `get_schema` - Get database schema information
10. `get_table_metadata` - Get detailed table metadata
11. `get_index_metadata_for_table` - Get index information
12. `get_object_definition` - Get object definitions (views, procedures)
13. `get_table_size_statistics` - Get table size and space usage
14. `get_row_count_for_table` - Get row counts (approximate or exact)

#### Locking Tools (2)

15. `get_index_usage_statistics` - Analyze index usage patterns
16. `get_missing_index_suggestions` - Get missing index recommendations

#### Agent Tools (3)

17. `get_current_blocking_snapshot` - Identify blocking sessions
18. `get_recent_deadlocks` - Retrieve recent deadlock information
19. `get_sql_agent_jobs` - List SQL Agent jobs

#### Security Tools (4)

20. `get_sql_agent_job_history` - Get job execution history
21. `get_recent_failed_jobs` - Find recently failed jobs
22. `get_principals_and_roles` - List database principals and roles
23. `get_permissions_for_principal` - Get permissions for a principal

#### Utility Tools (2)

24. `get_recent_security_changes` - Track security-related changes
25. `get_recent_schema_changes` - Track schema changes (DDL)
26. `sample_query_results` - Execute query and return sample results
27. `search_objects` - Search for database objects by name pattern

## Configuration

### How Configuration Works

The project uses a **simple, environment-based configuration** system:

1. **Default Values** - Defined in [`config/settings.py`](config/settings.py)
2. **Local Development** - Override defaults in [`.env.local`](.env.local) (tracked in git)
3. **Production** - Override via environment variables (Docker, Fargate, etc.)

**Priority (highest to lowest):**

```
Environment Variables > .env.local > Default Values in Settings
```

### Local Development Setup

1. **Review `.env.local`**:

   ```bash
   cat .env.local
   ```

   This file contains safe defaults for local development. Customize as needed.

2. **Start Docker SQL Server**:

   ```bash
   docker-compose up -d
   ```

3. **Install dependencies**:

   ```bash
   python -m venv .venv
   source .venv/bin/activate  # On Windows: .venv\Scripts\activate
   pip install -r requirements.txt
   ```

4. **Run the server**:
   ```bash
   python server.py
   ```

### Configuration Files

| File                  | Purpose                       | Tracked in Git?             |
| --------------------- | ----------------------------- | --------------------------- |
| `config/settings.py`  | Default values and validation | ✅ Yes                      |
| `.env.local`          | Local development overrides   | ✅ Yes (safe defaults only) |
| Environment variables | Production overrides          | ❌ No (set in deployment)   |

**Important:** Never commit real passwords or secrets! The `.env.local` file in git contains only safe defaults for local Docker.

## Claude Desktop Integration

### Step 1: Configure Claude Desktop

Edit Claude Desktop config file:

- **macOS**: `~/Library/Application Support/Claude/claude_desktop_config.json`
- **Windows**: `%APPDATA%\Claude\claude_desktop_config.json`

#### Single Database Configuration

```json
{
  "mcpServers": {
    "sql-server": {
      "command": "/Users/shahar.wiener/repos/mcp-sql-server/.venv/bin/python",
      "args": ["/Users/shahar.wiener/repos/mcp-sql-server/server.py"],
      "env": {
        "DB_CONN_LOCALDB": "Driver={ODBC Driver 18 for SQL Server};Server=127.0.0.1,1433;Database=LocalDB;UID=sa;PWD=YourStrong@Passw0rd;TrustServerCertificate=yes;Encrypt=yes;LoginTimeout=30"
      }
    }
  }
}
```

#### Multiple Databases Configuration

```json
{
  "mcpServers": {
    "sql-server": {
      "command": "/Users/shahar.wiener/repos/mcp-sql-server/.venv/bin/python",
      "args": ["/Users/shahar.wiener/repos/mcp-sql-server/server.py"],
      "env": {
        "DB_CONN_MOBYDOM5": "Driver={ODBC Driver 18 for SQL Server};Server=127.0.0.1;Database=mobydom5;Uid=sa;Pwd=YourStrong!Passw0rd;TrustServerCertificate=yes;",
        "DB_CONN_MCPAY": "Driver={ODBC Driver 18 for SQL Server};Server=127.0.0.1;Database=mcpay;Uid=sa;Pwd=YourStrong!Passw0rd;TrustServerCertificate=yes;",
        "DB_CONN_BILLING": "Driver={ODBC Driver 18 for SQL Server};Server=127.0.0.1;Database=billing;Uid=sa;Pwd=YourStrong!Passw0rd;TrustServerCertificate=yes;"
      }
    }
  }
}
```

**Key Points**:

- ✅ Use **absolute paths** for `command` and `args`
- ✅ For multiple databases, use `DB_CONN_<NAME>` pattern
- ✅ Database name is derived from env var name (e.g., `DB_CONN_MOBYDOM5` → `mobydom5`)

### Step 2: Restart Claude Desktop

1. Quit Claude Desktop completely
2. Relaunch Claude Desktop
3. Look for the 🔌 (plug) icon in the bottom-right corner
4. Click to enable "sql-server"

### Step 3: Test the Integration

In Claude Desktop, try:

- _"Show me all tables in the mobydom5 database"_
- _"Query the Users table and show me 5 records"_
- _"What's the structure of the accounts table?"_

## Cursor Integration

To use this MCP server with Cursor:

1. **Open Cursor Settings** → **Features** → **MCP**
2. **Add New MCP Server**:

   - **Name**: `sql-server`
   - **Type**: `SSE` (if running in Docker/Cloud) or `Command` (if local)

   **Option A: Local (Command)**

   - Command: `python /path/to/mcp-sql-server/server.py`

   **Option B: Remote / Docker (SSE)**

   - URL: `http://localhost:9303/sse` (or your cloud URL)

## AWS Fargate Deployment

### Step 1: Build and Push Docker Image

```bash
# Build image
docker build -t mcp-sql-server .

# Tag for ECR
docker tag mcp-sql-server:latest ACCOUNT_ID.dkr.ecr.REGION.amazonaws.com/mcp-sql-server:latest

# Push to ECR
aws ecr get-login-password --region REGION | docker login --username AWS --password-stdin ACCOUNT_ID.dkr.ecr.REGION.amazonaws.com
docker push ACCOUNT_ID.dkr.ecr.REGION.amazonaws.com/mcp-sql-server:latest
```

### Step 2: Create ECS Task Definition

Use the provided `task-definition.json` as a template. Key configurations:

- **Task Role**: Ensure it has permissions for SSM Parameter Store (if using)
- **Environment Variables**: Set `PANGO_ENV=Prd`, `MCP_TRANSPORT=sse`, `MCP_HOST=0.0.0.0`
- **Secrets**: Store `DB_CONNECTION_STRING` in AWS Secrets Manager or SSM Parameter Store
- **Networking**: Deploy in a private subnet with access to your SQL Server

### Step 3: Deploy Service

```bash
aws ecs create-service \
  --cluster my-cluster \
  --service-name mcp-sql-server \
  --task-definition mcp-sql-server-task \
  --desired-count 1 \
  --launch-type FARGATE \
  --network-configuration "awsvpcConfiguration={subnets=[subnet-xxx],securityGroups=[sg-xxx]}"
```

### Step 4: Configure Load Balancer (Optional)

If exposing via ALB:

- Target port: `9303`
- Health check path: `/sse`
- Protocol: HTTP

## Environment Variables Reference

| Variable                     | Description                      | Default         | Required |
| ---------------------------- | -------------------------------- | --------------- | -------- |
| `PANGO_ENV`                  | Environment name (Int, Stg, Prd) | `Int`           | No       |
| `LOG_LEVEL`                  | Logging level                    | `INFO`          | No       |
| `MCP_TRANSPORT`              | `stdio` or `sse`                 | `stdio`         | No       |
| `MCP_HOST`                   | Host to bind to                  | `127.0.0.1`     | No       |
| `MCP_PORT`                   | Port for SSE server              | `9303`          | No       |
| `DB_CONN_<NAME>`             | Database connection strings      | -               | Yes\*    |
| `DB_NAME`                    | (Legacy) Default database name   | -               | No       |
| `DB_CONNECTION_STRING`       | (Legacy) ODBC Connection String  | -               | No       |
| `MAX_QUERY_LENGTH`           | Max query length                 | `10000`         | No       |
| `MAX_RESULT_ROWS`            | Max rows to return               | `10000`         | No       |
| `DEFAULT_CONNECTION_TIMEOUT` | Connection timeout (s)           | `30`            | No       |
| `DEFAULT_COMMAND_TIMEOUT`    | Query timeout (s)                | `300`           | No       |
| `AUDIT_LOG_ENABLED`          | Enable audit logging             | `True`          | No       |
| `AUDIT_LOG_PATH`             | Audit log directory              | `./logs/audit/` | No       |
| `AWS_REGION`                 | AWS Region for SSM               | `us-east-1`     | No       |
| `SSM_PARAMETER_NAME`         | SSM parameter for config         | -               | No       |

\* At least one `DB_CONN_<NAME>` is required. Legacy `DB_NAME` + `DB_CONNECTION_STRING` is also supported for backward compatibility.

## Configuration Notes

### Host Binding (`MCP_HOST`)

- **Local Development**: Use `127.0.0.1` (localhost only)
- **Docker/Cloud**: Use `0.0.0.0` (all interfaces)
- **Production**: Set via environment variable, never hardcode

### Port Configuration (`MCP_PORT`)

- **Default**: `9303`
- **Customizable**: Set `MCP_PORT` environment variable
- **Docker**: Map container port to host port (e.g., `-p 8080:9303`)

## Security

### Read-Only Enforcement

The server enforces **strict read-only access** through multiple layers:

1. **Query Validation**: All SQL queries are validated to ensure they only contain SELECT statements
2. **Disabled Write Tools**: The following tools are **disabled by default** because they could execute write operations:
   - `execute_stored_procedure` - Stored procedures can contain UPDATE/DELETE/INSERT
   - `execute_sql_script_transactional` - Scripts could contain destructive operations
3. **SQL Injection Protection**: Parameterized queries and input validation prevent injection attacks
4. **Dangerous Pattern Blocking**: Blocks patterns like `xp_cmdshell`, `OPENROWSET`, etc.
5. **Safe Parsing**: `parse_sql_script` uses `SET PARSEONLY ON` to validate syntax without execution

### Re-enabling Write Operations

If you need to enable write operations (e.g., for administrative tools):

1. **Uncomment the tools** in `server.py` and `services/infrastructure/db_connection_service.py`
2. **Add validation** to inspect stored procedure definitions before execution
3. **Implement authentication** to restrict access to trusted users only
4. **Enable comprehensive audit logging** to track all write operations
5. **Use dedicated write-only database users** with minimal permissions

### Other Security Measures

- **Audit Logs**: All queries are logged to `logs/audit/` (if enabled) for compliance
- **Secrets**: Never commit `.env` files. Use AWS Secrets Manager or SSM Parameter Store in production
- **SQL Injection Protection**: Parameterized queries and input validation
- **Connection Security**: Enforce encrypted connections in production

## Troubleshooting

| Issue                                      | Solution                                                                                      |
| ------------------------------------------ | --------------------------------------------------------------------------------------------- |
| **Server not appearing in Claude Desktop** | • Verify absolute paths in config<br>• Restart Claude Desktop<br>• Check Claude Desktop logs  |
| **Connection failures**                    | • Verify database connectivity<br>• Check connection string format<br>• Test with SQL client  |
| **Wrong host/port**                        | • Check `MCP_HOST` and `MCP_PORT` env vars<br>• Use `0.0.0.0` for Docker/Cloud                |
| **Multiple databases not working**         | • Use `DB_CONN_<NAME>` pattern<br>• Check env var names<br>• Verify connection strings        |
| **Audit logs not appearing**               | • Check `AUDIT_LOG_ENABLED=true`<br>• Verify log path is writable<br>• Check file permissions |

**Debug Logs**:

- **macOS**: `~/Library/Logs/Claude/mcp*.log`
- **Windows**: `%APPDATA%\Claude\logs\mcp*.log`

## Usage Examples

Once configured, ask Claude:

**Query Examples**:

- _"Show me the last 10 records from the Users table in mobydom5"_
- _"Count how many active accounts are in the database"_
- _"Find all records where FirstName = 'John'"_

**Schema Examples**:

- _"What tables are available in the mcpay database?"_
- _"Show me the structure of the accounts table"_
- _"List all columns in the Users table with their data types"_

**Performance Examples**:

- _"Show me the slowest queries in the last hour"_
- _"What indexes are missing on the Users table?"_
- _"Check the fragmentation level of all indexes"_

**Multi-Database Examples**:

- _"Compare the Users table structure between mobydom5 and mcpay"_
- _"Show me tables from all configured databases"_

---

**Version 2.0** • Refactored Architecture • Cloud Ready • November 2024
