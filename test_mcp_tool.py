#!/usr/bin/env python3
"""
Test MCP tool functionality.
This script can be run:
1. Locally (if Python 3.10+ and ODBC driver are installed)
2. Inside Docker: docker exec -i mcp-sql-server python test_mcp_tool.py
"""
import sys
import os

# Set environment variables for connection
os.environ.setdefault('DB_SERVER_INT', 'localhost' if os.path.exists('/.dockerenv') else 'sql-server-int')
os.environ.setdefault('DB_DATABASE_INT', 'MyAppDB')
os.environ.setdefault('DB_USERNAME_INT', 'sa')
os.environ.setdefault('DB_PASSWORD_INT', 'McpSql2025!Secure')
os.environ.setdefault('MCP_CONFIG_PATH', 'config/config.yaml')

# Add project to path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

try:
    from services.core.execution_service import ExecutionService
except ImportError as e:
    print(f"Error importing ExecutionService: {e}")
    print("\nThis might be due to:")
    print("1. Missing dependencies - run: pip install -r requirements.txt")
    print("2. ODBC driver issues - see SETUP_LOCAL.md")
    print("3. Python version - MCP requires Python 3.10+")
    sys.exit(1)

def test_query_readonly():
    """Test the query_readonly functionality."""
    print("Testing query_readonly (MCP tool functionality)...")
    print("=" * 60)
    
    try:
        execution_service = ExecutionService()
        
        result = execution_service.execute_readonly(
            query='SELECT username, first_name, last_name FROM dbo.Users ORDER BY username',
            env='Int'
        )
        
        if result.get('success'):
            print('\n✓ Query executed successfully')
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
            
            return True
        else:
            print(f'\n✗ Query failed: {result.get("error")}')
            if result.get('blocking_violations'):
                print(f'\nBlocking Violations:')
                for violation in result.get('blocking_violations', []):
                    print(f'  - {violation.get("code")}: {violation.get("title")}')
                    print(f'    {violation.get("description")}')
            return False
            
    except Exception as e:
        print(f'\n✗ Error: {e}')
        import traceback
        traceback.print_exc()
        return False

if __name__ == "__main__":
    success = test_query_readonly()
    sys.exit(0 if success else 1)

