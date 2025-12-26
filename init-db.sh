#!/bin/bash
# Initialize SQL Server database with test data
# This script waits for SQL Server to be ready and then runs the initialization SQL

set -e

echo "Waiting for SQL Server to be ready..."
until /opt/mssql-tools/bin/sqlcmd -C -S localhost -U sa -P "$MSSQL_SA_PASSWORD" -Q "SELECT 1" &>/dev/null; do
    echo "SQL Server is not ready yet. Waiting..."
    sleep 5
done

echo "SQL Server is ready. Initializing database..."
/opt/mssql-tools/bin/sqlcmd -C -S localhost -U sa -P "$MSSQL_SA_PASSWORD" -i /docker-entrypoint-initdb.d/init-db.sql

echo "Database initialization complete!"

