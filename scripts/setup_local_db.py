#!/usr/bin/env python
"""Setup LocalDB database with sample users table and data."""
import pyodbc
import os
from pathlib import Path
from dotenv import load_dotenv

# Load local config from project root
project_root = Path(__file__).parent.parent
load_dotenv(project_root / '.env.local')

def setup_local_db():
    """Create users table and insert sample data in LocalDB."""
    print("--- Setting up LocalDB Database ---")
    
    # Use the new DB_CONN_LOCALDB configuration
    conn_str = os.getenv('DB_CONN_LOCALDB')
    if not conn_str:
        print("❌ DB_CONN_LOCALDB not found in .env.local")
        print("   Make sure .env.local has: DB_CONN_LOCALDB=Driver={...};Database=LocalDB;...")
        return

    try:
        conn = pyodbc.connect(conn_str, autocommit=True)
        cursor = conn.cursor()
        
        print("✅ Connected to LocalDB database")

        # Create users table
        print("Creating 'users' table...")
        cursor.execute("""
            IF NOT EXISTS (SELECT * FROM sysobjects WHERE name='users' AND xtype='U')
            CREATE TABLE users (
                id INT PRIMARY KEY IDENTITY(1,1),
                name NVARCHAR(100),
                email NVARCHAR(100),
                created_at DATETIME DEFAULT GETDATE()
            )
        """)
        
        # Insert sample data
        print("Inserting sample data...")
        cursor.execute("SELECT COUNT(*) FROM users")
        count = cursor.fetchone()[0]
        
        if count == 0:
            cursor.execute("""
                INSERT INTO users (name, email) VALUES 
                ('Alice', 'alice@example.com'),
                ('Bob', 'bob@example.com'),
                ('Charlie', 'charlie@example.com')
            """)
            print("✅ Inserted 3 sample users")
        else:
            print(f"ℹ️  Table already has {count} rows. Skipping insertion.")

        # Verify
        cursor.execute("SELECT name, email FROM users")
        users = cursor.fetchall()
        print(f"\n📊 Current users in database:")
        for user in users:
            print(f"   - {user[0]} ({user[1]})")

        conn.close()
        print("\n✅ Setup complete!")
        
    except Exception as e:
        print(f"❌ Setup failed: {e}")
        return 1
    
    return 0

if __name__ == "__main__":
    exit(setup_local_db())
