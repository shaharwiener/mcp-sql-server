#!/bin/bash
# Run MCP tool locally using Docker (code is mounted, not copied)
# This allows you to edit code locally and test it immediately

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

# Check if Docker container is running
if ! docker ps | grep -q mcp-sql-server; then
    echo "Starting Docker containers..."
    docker-compose up -d
    echo "Waiting for SQL Server to be ready..."
    sleep 5
fi

# Run the MCP tool call inside Docker with code mounted
echo "Running MCP tool call..."
docker exec -i mcp-sql-server python -c "
import sys
sys.path.insert(0, '/app')

from services.core.execution_service import ExecutionService

execution_service = ExecutionService()
result = execution_service.execute_readonly(
    query='SELECT username, first_name, last_name FROM dbo.Users ORDER BY username',
    env='Int'
)

if result.get('success'):
    print('\nUser Names:')
    print('=' * 60)
    for row in result.get('data', []):
        username = row.get('username', 'N/A')
        first_name = row.get('first_name', '')
        last_name = row.get('last_name', '')
        full_name = f'{first_name} {last_name}'.strip() if first_name or last_name else 'N/A'
        print(f'  Username: {username:<20} Name: {full_name}')
    print(f'\nTotal: {result.get(\"row_count\", 0)} users')
    print(f'Execution time: {result.get(\"execution_time_ms\", 0):.2f}ms')
    
    if result.get('best_practice_warnings'):
        print(f'\nBest Practice Warnings: {len(result.get(\"best_practice_warnings\", []))}')
        for warning in result.get('best_practice_warnings', [])[:3]:
            print(f'  - {warning.get(\"code\")}: {warning.get(\"description\")}')
    
    if result.get('review_summary'):
        review = result.get('review_summary', {})
        print(f'\nReview Status: {review.get(\"status\")}')
        print(f'Risk Score: {review.get(\"risk_score\")}')
else:
    print(f'\nError: {result.get(\"error\")}')
    if result.get('blocking_violations'):
        print(f'Blocking Violations: {len(result.get(\"blocking_violations\", []))}')
"

