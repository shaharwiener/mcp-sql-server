# Docker Setup Guide for SQL Server MCP

This guide provides step-by-step instructions for setting up and testing the SQL Server MCP using Docker.

## Prerequisites

- Docker and Docker Compose installed
- At least 4GB of free RAM (SQL Server requires significant memory)
- Ports 1433 (SQL Server) available on your host

## Quick Start

### Option 1: Automated Setup (Recommended)

```bash
# Run the setup script
./setup-and-test.sh
```

This will:
1. Build Docker images
2. Start SQL Server and MCP server containers
3. Initialize the test database
4. Verify the setup

### Option 2: Manual Setup

```bash
# 1. Build and start containers
docker-compose up -d --build

# 2. Wait for SQL Server to be ready (30-60 seconds)
# Check logs: docker logs sql-server-int

# 3. Initialize the database
docker exec sql-server-int /opt/mssql-tools/bin/sqlcmd \
  -C -S localhost -U sa -P "McpSql2025!Secure" \
  -i /init-db.sql

# 4. Verify setup
./test-mcp.sh
```

## Container Architecture

```
┌─────────────────────┐
│   SQL Server        │
│   (sql-server-int)  │
│   Port: 1433        │
│   Database: MyAppDB │
└──────────┬──────────┘
           │
           │ (Docker Network)
           │
┌──────────▼──────────┐
│   MCP Server        │
│   (mcp-sql-server)  │
│   Python 3.11       │
└─────────────────────┘
```

## Testing

### Run All Tests

```bash
./test-mcp.sh
```

### Manual Testing

**Test SQL Server:**
```bash
docker exec sql-server-int /opt/mssql-tools/bin/sqlcmd \
  -C -S localhost -U sa -P "McpSql2025!Secure" \
  -Q "SELECT COUNT(*) FROM MyAppDB.dbo.Users"
```

**Test MCP Server Configuration:**
```bash
docker exec mcp-sql-server python -c "
from config.configuration import get_config
config = get_config()
print(f'Environment: {config.environment}')
"
```

**Test Database Connection:**
```bash
docker exec mcp-sql-server python -c "
from services.infrastructure.db_connection_service import DbConnectionService
service = DbConnectionService()
conn = service.get_connection(env='Int')
print('Connection successful!')
conn.close()
"
```

**Test Query Execution:**
```bash
docker exec mcp-sql-server python -c "
from services.core.execution_service import ExecutionService
service = ExecutionService()
result = service.execute_readonly('SELECT TOP 5 * FROM dbo.Users', env='Int')
print(f'Rows returned: {result[\"row_count\"]}')
"
```

## Connecting from Host

### SQL Clients (Azure Data Studio, SSMS, etc.)

- **Server:** `localhost,1433`
- **Username:** `sa`
- **Password:** `McpSql2025!Secure`
- **Database:** `MyAppDB`

### Connection String

```
Driver={ODBC Driver 18 for SQL Server};Server=localhost,1433;Database=MyAppDB;Uid=sa;Pwd=McpSql2025!Secure;Encrypt=yes;TrustServerCertificate=yes;
```

## Troubleshooting

### SQL Server Won't Start

**Check logs:**
```bash
docker logs sql-server-int
```

**Common issues:**
- Port 1433 already in use: Change port mapping in `docker-compose.yml`
- Insufficient memory: SQL Server needs at least 2GB RAM
- EULA not accepted: Ensure `ACCEPT_EULA=Y` is set

### MCP Server Can't Connect to SQL Server

**Check network connectivity:**
```bash
docker exec mcp-sql-server ping sql-server-int
```

**Check environment variables:**
```bash
docker exec mcp-sql-server env | grep DB_
```

**Verify connection string:**
```bash
docker exec mcp-sql-server python -c "
from config.configuration import get_config
config = get_config()
print(config.database.connection_components.get('Int'))
"
```

### Database Not Initialized

**Check if database exists:**
```bash
docker exec sql-server-int /opt/mssql-tools/bin/sqlcmd \
  -C -S localhost -U sa -P "McpSql2025!Secure" \
  -Q "SELECT name FROM sys.databases WHERE name = 'MyAppDB'"
```

**Re-run initialization:**
```bash
docker exec sql-server-int /opt/mssql-tools/bin/sqlcmd \
  -C -S localhost -U sa -P "McpSql2025!Secure" \
  -i /init-db.sql
```

### Container Name Mismatch

If Docker Compose creates containers with different names:

```bash
# Find the actual container name
docker ps --format "{{.Names}}" | grep mcp

# Use the actual name in Cursor configuration
```

## Maintenance

### View Logs

```bash
# SQL Server logs
docker logs sql-server-int

# MCP Server logs
docker logs mcp-sql-server

# Follow logs in real-time
docker logs -f mcp-sql-server
```

### Stop Containers

```bash
# Stop (keeps data)
docker-compose stop

# Stop and remove containers (keeps volumes)
docker-compose down

# Stop and remove everything (deletes data)
docker-compose down -v
```

### Reset Everything

```bash
# Stop and remove everything
docker-compose down -v

# Remove images (optional)
docker rmi mcp-sql-server_mcp-sql-server

# Start fresh
./setup-and-test.sh
```

### Update Code

```bash
# Rebuild after code changes
docker-compose build mcp-sql-server
docker-compose up -d mcp-sql-server
```

## Performance Tuning

### SQL Server Memory

By default, SQL Server uses up to 80% of available memory. To limit it, add to `docker-compose.yml`:

```yaml
environment:
  - MSSQL_MEMORY_LIMIT_MB=2048
```

### Connection Pool

Adjust connection pool size in `config/config.yaml`:

```yaml
database:
  connection_pool_size: 10  # Adjust as needed
```

## Security Notes

⚠️ **This setup is for LOCAL DEVELOPMENT ONLY**

- Default SA password is weak (`McpSql2025!Secure`)
- SQL Server accepts connections from any host
- No encryption required for local testing
- Do NOT use these credentials in production

For production:
- Use strong passwords
- Restrict network access
- Enable encryption
- Use least-privilege database users

