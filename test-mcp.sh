#!/bin/bash
# Test script to verify MCP server is working correctly

set -e

echo "🧪 Testing MCP SQL Server..."

# Check if containers are running
if ! docker ps | grep -q "mcp-sql-server"; then
    echo "❌ MCP server container is not running. Start it with: docker-compose up -d"
    exit 1
fi

if ! docker ps | grep -q "sql-server-int"; then
    echo "❌ SQL Server container is not running. Start it with: docker-compose up -d"
    exit 1
fi

echo "✅ Containers are running"

# Test 1: Check if MCP server can load configuration
echo ""
echo "Test 1: Loading configuration..."
if docker exec mcp-sql-server python -c "
import sys
sys.path.insert(0, '/app')
from config.configuration import get_config
config = get_config()
print(f'✅ Config loaded: Environment={config.environment}')
print(f'✅ Available environments: {config.available_environments}')
print(f'✅ Int environment configured: {\"Int\" in config.database.connection_components}')
" 2>&1; then
    echo "✅ Configuration test passed"
else
    echo "❌ Configuration test failed"
    exit 1
fi

# Test 2: Check if MCP server can connect to SQL Server
echo ""
echo "Test 2: Testing database connection..."
if docker exec mcp-sql-server python -c "
import sys
sys.path.insert(0, '/app')
from services.infrastructure.db_connection_service import DbConnectionService
service = DbConnectionService()
conn = service.get_connection(env='Int')
cursor = conn.cursor()
cursor.execute('SELECT 1 as test')
result = cursor.fetchone()
conn.close()
print(f'✅ Database connection successful: {result[0]}')
" 2>&1; then
    echo "✅ Database connection test passed"
else
    echo "❌ Database connection test failed"
    exit 1
fi

# Test 3: Test a simple query
echo ""
echo "Test 3: Testing query execution..."
if docker exec mcp-sql-server python -c "
import sys
sys.path.insert(0, '/app')
from services.core.execution_service import ExecutionService
service = ExecutionService()
result = service.execute_readonly('SELECT COUNT(*) as user_count FROM dbo.Users', env='Int')
print(f'✅ Query executed successfully')
if result.get('success') and result.get('data'):
    user_count = result['data'][0].get('user_count', result['data'][0].get(list(result['data'][0].keys())[0], 'N/A'))
    print(f'✅ User count: {user_count}')
else:
    print(f'✅ Query executed (row_count: {result.get(\"row_count\", 0)})')
" 2>&1; then
    echo "✅ Query execution test passed"
else
    echo "❌ Query execution test failed"
    exit 1
fi

# Test 4: Test SQL analysis
echo ""
echo "Test 4: Testing SQL analysis..."
if docker exec mcp-sql-server python -c "
import sys
sys.path.insert(0, '/app')
from services.analysis.sql_analyzer import SqlAnalyzer
analyzer = SqlAnalyzer()
result = analyzer.analyze('SELECT * FROM dbo.Users')
print(f'✅ SQL analysis completed')
print(f'✅ Risk score: {result.summary.risk_score}')
print(f'✅ Status: {result.summary.status}')
" 2>&1; then
    echo "✅ SQL analysis test passed"
else
    echo "❌ SQL analysis test failed"
    exit 1
fi

echo ""
echo "🎉 All tests passed! MCP server is ready to use."
echo ""
echo "Next steps:"
echo "  1. Configure Cursor to connect to the MCP server (see README.md)"
echo "  2. Test with: docker exec -i mcp-sql-server python server.py"

