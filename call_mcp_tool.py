#!/usr/bin/env python3
"""
Call MCP tool functionality locally without requiring MCP package.
This directly calls the same code that the MCP tool invokes.
"""
import sys
import os

# Set environment variables for local connection to Docker SQL Server
os.environ['DB_SERVER_INT'] = 'localhost'
os.environ['DB_DATABASE_INT'] = 'MyAppDB'
os.environ['DB_USERNAME_INT'] = 'sa'
os.environ['DB_PASSWORD_INT'] = 'McpSql2025!Secure'
os.environ['MCP_CONFIG_PATH'] = 'config/config.yaml'

# Add project to path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

# Import the execution service directly (what the MCP tool calls)
from services.core.execution_service import ExecutionService

def main():
    """Call the query_readonly functionality (same as MCP tool)."""
    print("Calling query_readonly (MCP tool functionality)...")
    print("=" * 60)
    
    # Create execution service (same as what the MCP tool uses)
    execution_service = ExecutionService()
    
    # Call execute_readonly (this is what query_readonly MCP tool calls)
    result = execution_service.execute_readonly(
        query='SELECT username, first_name, last_name FROM dbo.Users ORDER BY username',
        env='Int'
    )
    
    # Display results
    if result.get('success'):
        print('\nUser Names:')
        print('=' * 60)
        for row in result.get('data', []):
            username = row.get('username', 'N/A')
            first_name = row.get('first_name', '')
            last_name = row.get('last_name', '')
            full_name = f'{first_name} {last_name}'.strip() if first_name or last_name else 'N/A'
            print(f'  Username: {username:<20} Name: {full_name}')
        print(f'\nTotal: {result.get("row_count", 0)} users')
        print(f'Execution time: {result.get("execution_time_ms", 0):.2f}ms')
        
        if result.get('best_practice_warnings'):
            print(f'\nBest Practice Warnings: {len(result.get("best_practice_warnings", []))}')
            for warning in result.get('best_practice_warnings', [])[:3]:
                print(f'  - {warning.get("code")}: {warning.get("description")}')
        
        if result.get('review_summary'):
            review = result.get('review_summary', {})
            print(f'\nReview Status: {review.get("status")}')
            print(f'Risk Score: {review.get("risk_score")}')
    else:
        print(f'\nError: {result.get("error")}')
        if result.get('blocking_violations'):
            print(f'Blocking Violations: {len(result.get("blocking_violations", []))}')

if __name__ == "__main__":
    main()

