#!/bin/bash
# Quick start script for running MCP SQL Server in Docker

set -e

echo "🚀 Starting MCP SQL Server in Docker..."
echo ""

# Step 1: Check if SQL Server is running
echo "📊 Step 1: Checking SQL Server database..."
if ! docker ps | grep -q mcp-sql-server-db; then
    echo "   Starting SQL Server container..."
    docker-compose up -d
    echo "   Waiting for SQL Server to be ready..."
    sleep 10
else
    echo "   ✅ SQL Server is already running"
fi
echo ""

# Step 2: Build the MCP server image
echo "🔨 Step 2: Building MCP server Docker image..."
docker build -t mcp-sql-server:latest .
echo "   ✅ Image built successfully"
echo ""

# Step 3: Stop and remove existing container if it exists
if docker ps -a | grep -q mcp-sql-server; then
    echo "🧹 Step 3: Removing existing container..."
    docker rm -f mcp-sql-server
    echo "   ✅ Old container removed"
else
    echo "🧹 Step 3: No existing container to remove"
fi
echo ""

# Step 4: Run the MCP server container
echo "🐳 Step 4: Starting MCP server container..."
docker run -d \
  --name mcp-sql-server \
  -p 9303:9303 \
  --env-file .env.local \
  -e MCP_TRANSPORT=sse \
  -e MCP_HOST=0.0.0.0 \
  -e DB_CONN_MASTER='Driver={ODBC Driver 18 for SQL Server};Server=host.docker.internal;Database=master;Uid=sa;Pwd=YourStrong!Passw0rd;TrustServerCertificate=yes;LoginTimeout=30;' \
  -e DB_CONN_LOCALDB='Driver={ODBC Driver 18 for SQL Server};Server=host.docker.internal;Database=LocalDB;Uid=sa;Pwd=YourStrong!Passw0rd;TrustServerCertificate=yes;LoginTimeout=30;' \
  mcp-sql-server:latest

echo "   ✅ Container started"
echo ""

# Step 5: Wait for server to be ready
echo "⏳ Step 5: Waiting for MCP server to be ready..."
sleep 3

# Step 6: Check if server is responding
echo "🔍 Step 6: Verifying server health..."
if curl -s http://localhost:9303/health > /dev/null 2>&1; then
    echo "   ✅ Server is healthy and responding"
else
    echo "   ⚠️  Server might not be ready yet. Check logs with: docker logs mcp-sql-server"
fi
echo ""

# Step 7: Show logs
echo "📋 Step 7: Server logs (last 20 lines):"
echo "----------------------------------------"
docker logs --tail 20 mcp-sql-server
echo "----------------------------------------"
echo ""

# Final instructions
echo "✅ MCP SQL Server is running!"
echo ""
echo "📝 Next steps:"
echo "   1. Configure Cursor to connect to: http://localhost:9303/sse"
echo "   2. Add this to Cursor's MCP settings:"
echo ""
echo '   {
     "mcpServers": {
       "sql-server": {
         "url": "http://localhost:9303/sse",
         "transport": {
           "type": "sse"
         }
       }
     }
   }'
echo ""
echo "🔧 Useful commands:"
echo "   View logs:    docker logs -f mcp-sql-server"
echo "   Stop server:  docker stop mcp-sql-server"
echo "   Start server: docker start mcp-sql-server"
echo "   Restart:      docker restart mcp-sql-server"
echo ""
