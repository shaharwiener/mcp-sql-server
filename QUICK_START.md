# Quick Start Guide

Get the SQL Server MCP running with Docker and connected to Cursor in 5 minutes!

## 🚀 Step 1: Start Everything

```bash
./setup-and-test.sh
```

This will:
- ✅ Build Docker images
- ✅ Start SQL Server
- ✅ Start MCP server
- ✅ Initialize test database

## 🧪 Step 2: Verify Setup

```bash
./test-mcp.sh
```

All tests should pass ✅

## 🎯 Step 3: Connect Cursor

### In Cursor Settings:

1. Go to **Settings** → **Features** → **MCP**
2. Click **+ Add New MCP Server**
3. Configure:
   - **Name:** `SQL Server MCP`
   - **Type:** `Command`
   - **Command:** `docker`
   - **Args:** `exec -i mcp-sql-server python server.py`

4. Look for the green indicator ✅

## 🎉 Step 4: Test It!

Ask Cursor:
- "Use SQL Server MCP to query the Users table"
- "Review this SQL: SELECT * FROM dbo.Users"
- "Get the schema for MyAppDB"

## 📚 More Information

- **Detailed Docker Setup:** See [DOCKER_SETUP.md](DOCKER_SETUP.md)
- **Full Documentation:** See [README.md](README.md)
- **Cursor Config Example:** See [cursor-mcp-config.json](cursor-mcp-config.json)

## 🛑 Stop Everything

```bash
docker-compose down
```

## 🔄 Restart

```bash
docker-compose up -d
```

## ❓ Troubleshooting

**Container not found?**
```bash
docker ps  # Check container names
```

**SQL Server not ready?**
```bash
docker logs sql-server-int
```

**MCP server issues?**
```bash
docker logs mcp-sql-server
```

For more help, see [DOCKER_SETUP.md](DOCKER_SETUP.md#troubleshooting)

