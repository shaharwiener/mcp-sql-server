#!/bin/bash
# Setup and test script for SQL Server MCP with Docker

set -e

echo "🚀 Setting up SQL Server MCP with Docker..."

# Make init script executable
chmod +x init-db.sh

# Build and start containers
echo "📦 Building and starting Docker containers..."
docker-compose down -v 2>/dev/null || true
docker-compose build
docker-compose up -d

# Wait for SQL Server to be ready
echo "⏳ Waiting for SQL Server to be ready..."
max_attempts=30
attempt=0
while [ $attempt -lt $max_attempts ]; do
    if docker exec sql-server-int /opt/mssql-tools/bin/sqlcmd -C -S localhost -U sa -P "McpSql2025!Secure" -Q "SELECT 1" &>/dev/null; then
        echo "✅ SQL Server is ready!"
        break
    fi
    attempt=$((attempt + 1))
    echo "   Attempt $attempt/$max_attempts..."
    sleep 2
done

if [ $attempt -eq $max_attempts ]; then
    echo "❌ SQL Server failed to start in time"
    exit 1
fi

# Initialize database
echo "🗄️  Initializing database..."
docker exec sql-server-int /opt/mssql-tools/bin/sqlcmd -C -S localhost -U sa -P "McpSql2025!Secure" -i /init-db.sql || {
    echo "⚠️  Running initialization from host..."
    docker exec -i sql-server-int /opt/mssql-tools/bin/sqlcmd -C -S localhost -U sa -P "McpSql2025!Secure" < init-db.sql
}

# Wait for MCP server container to be ready
echo "⏳ Waiting for MCP server container..."
sleep 5

# Test MCP server connection
echo "🧪 Testing MCP server..."
if docker exec mcp-sql-server python -c "import sys; sys.path.insert(0, '/app'); from config.configuration import get_config; config = get_config(); print('✅ Config loaded successfully')" 2>/dev/null; then
    echo "✅ MCP server container is ready!"
else
    echo "⚠️  MCP server container may not be fully ready yet"
fi

# Display connection information
echo ""
echo "✅ Setup complete!"
echo ""
echo "📋 Connection Information:"
echo "   SQL Server:"
echo "     Host: localhost"
echo "     Port: 1433"
echo "     Username: sa"
echo "     Password: McpSql2025!Secure"
echo "     Database: MyAppDB"
echo ""
echo "   MCP Server Container:"
echo "     Container: mcp-sql-server"
echo "     Command: docker exec -i mcp-sql-server python server.py"
echo ""
echo "📝 Next Steps:"
echo "   1. Run tests: ./test-mcp.sh"
echo "   2. Configure Cursor to use the MCP server (see README.md)"
echo "   3. Test the connection using: docker exec -i mcp-sql-server python server.py"
echo ""
echo "🧹 To stop containers: docker-compose down"
echo "🧹 To stop and remove volumes: docker-compose down -v"

