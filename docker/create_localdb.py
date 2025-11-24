#!/usr/bin/env python
"""Create LocalDB database in Docker SQL Server"""
import pyodbc
import time

# Wait for SQL Server to be ready
print("Waiting for SQL Server to be ready...")
time.sleep(5)

try:
    conn = pyodbc.connect(
        'Driver={ODBC Driver 18 for SQL Server};'
        'Server=127.0.0.1;'
        'Database=master;'
        'Uid=sa;'
        'Pwd=YourStrong!Passw0rd;'
        'TrustServerCertificate=yes;',
        timeout=30,
        autocommit=True  # Required for CREATE DATABASE
    )
    cursor = conn.cursor()
    
    # Create LocalDB if it doesn't exist
    cursor.execute("IF NOT EXISTS (SELECT * FROM sys.databases WHERE name = 'LocalDB') CREATE DATABASE LocalDB")
    print('✅ LocalDB database created successfully')
    
    # Verify it exists
    cursor.execute('SELECT name FROM sys.databases ORDER BY name')
    dbs = [row[0] for row in cursor.fetchall()]
    print(f'📊 Available databases: {", ".join(dbs)}')
    
    conn.close()
    print('✅ Database setup complete')
except Exception as e:
    print(f'❌ Error: {e}')
    exit(1)
