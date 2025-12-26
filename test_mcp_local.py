#!/usr/bin/env python3
"""
Test MCP tools locally (without docker exec).
This script calls the MCP tool functions directly.
"""
import sys
import os

# Load environment variables
from dotenv import load_dotenv
load_dotenv('.env.local')

# Add project to path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from server import query_readonly

def main():
    """Test query_readonly MCP tool locally."""
    print("Calling query_readonly MCP tool...")
    print("=" * 60)
    
    # Call the MCP tool function
    result = query_readonly(
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
    else:
        print(f'\nError: {result.get("error")}')
        if result.get('blocking_violations'):
            print(f'Blocking Violations: {len(result.get("blocking_violations", []))}')

if __name__ == "__main__":
    main()

