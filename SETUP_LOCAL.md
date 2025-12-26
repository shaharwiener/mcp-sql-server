# Local Environment Setup for MCP Tools

This guide helps you set up a local environment to run MCP tools.

## Quick Start (Recommended: Using Docker)

The easiest way to test MCP tools locally is using Docker with the code mounted as a volume:

```bash
# 1. Ensure Docker containers are running
docker-compose up -d

# 2. Use the wrapper script
./call_mcp.sh "SELECT username FROM dbo.Users" Int

# 3. Or call directly via docker exec
docker exec -i mcp-sql-server python -c "
import sys
sys.path.insert(0, '/app')
from services.core.execution_service import ExecutionService
execution_service = ExecutionService()
result = execution_service.execute_readonly(
    query='SELECT username FROM dbo.Users',
    env='Int'
)
print(result)
"
```

The `docker-compose.yml` has been configured to mount your local code as a volume, so any changes you make locally are immediately available in the container.

## Full Local Setup (Without Docker)

If you want to run MCP tools completely locally (without Docker), you'll need:

### Prerequisites

1. **Python 3.10+** (MCP requires Python 3.10 or higher)
2. **SQL Server ODBC Driver** (for macOS)
3. **Access to Docker SQL Server** (running on localhost:1433)

### Setup Steps

#### 1. Install SQL Server ODBC Driver (macOS)

```bash
# Install using Homebrew
brew tap microsoft/mssql-release https://github.com/Microsoft/homebrew-mssql-release
brew update
HOMEBREW_NO_ENV_FILTERING=1 ACCEPT_EULA=Y brew install msodbcsql18 mssql-tools18
```

#### 2. Create Python 3.10+ Virtual Environment

```bash
# Check available Python versions
python3.10 --version  # or python3.11, python3.12

# Create new venv with Python 3.10+
python3.10 -m venv venv310
source venv310/bin/activate

# Install dependencies
pip install --upgrade pip
pip install "mcp[cli]>=1.6.0" "structlog" "pydantic" "pyodbc>=5.1.0" "sqlglot>=19.0.0" "pyyaml" "python-dotenv"
```

#### 3. Set Environment Variables

```bash
export DB_SERVER_INT=localhost
export DB_DATABASE_INT=MyAppDB
export DB_USERNAME_INT=sa
export DB_PASSWORD_INT=McpSql2025!Secure
export MCP_CONFIG_PATH=config/config.yaml
```

#### 4. Test the Setup

```bash
# Activate the new venv
source venv310/bin/activate

# Run the test script
./call_mcp.sh "SELECT username FROM dbo.Users" Int
```

## Available Scripts

- **`call_mcp.sh`**: Simple wrapper to call MCP tools via Docker
- **`test-mcp.sh`**: Comprehensive test script to verify MCP server functionality

## How It Works

The MCP tools (`query_readonly`, `review_sql_script`, etc.) are defined in `server.py` using the `@mcp.tool()` decorator. These tools call the underlying services:

- `ExecutionService.execute_readonly()` - Executes read-only queries with all security layers
- `ReviewService.review()` - Performs comprehensive SQL analysis

When you call these functions directly (via Docker or locally), you're using the same code path that gets invoked via the MCP protocol. The only difference is:
- **Via MCP Protocol**: JSON-RPC over stdio/SSE (used by Cursor)
- **Direct Call**: Python function call (for testing/development)

## Troubleshooting

### pyodbc Import Error on macOS

If you see `symbol not found in flat namespace '_SQLAllocHandle'`, the ODBC driver isn't properly linked. Try:

```bash
# Reinstall pyodbc
pip uninstall pyodbc
pip install pyodbc --no-binary pyodbc

# Or use Docker (recommended)
```

### Python Version Issues

MCP requires Python 3.10+. If your system only has Python 3.9:

- Use Docker (recommended)
- Or install Python 3.10+ via Homebrew: `brew install python@3.11`

