#!/bin/bash
# Simple wrapper to call MCP tool functions via Docker
# Usage: ./call_mcp.sh "SELECT * FROM Users" [env] [database]

set -e

QUERY="${1:-SELECT username, first_name, last_name FROM dbo.Users ORDER BY username}"
ENV="${2:-Int}"
DATABASE="${3:-}"

echo "Calling MCP tool: query_readonly"
echo "Query: $QUERY"
echo "Environment: $ENV"
echo "============================================================"

docker exec -i mcp-sql-server python -c "
import sys
sys.path.insert(0, '/app')
from services.core.execution_service import ExecutionService

execution_service = ExecutionService()
result = execution_service.execute_readonly(
    query='$QUERY',
    env='$ENV'${DATABASE:+, database='$DATABASE'}
)

if result.get('success'):
    print('\n✓ Success')
    print(f'Rows: {result.get(\"row_count\", 0)}')
    print(f'Execution time: {result.get(\"execution_time_ms\", 0):.2f}ms')
    print('\nData:')
    print('=' * 60)
    import json
    print(json.dumps(result.get('data', []), indent=2, default=str))
    
    if result.get('best_practice_warnings'):
        print(f'\n⚠ Best Practice Warnings: {len(result.get(\"best_practice_warnings\", []))}')
        for warning in result.get('best_practice_warnings', [])[:5]:
            print(f'  - {warning.get(\"code\")}: {warning.get(\"title\")}')
    
    if result.get('review_summary'):
        review = result.get('review_summary', {})
        print(f'\n📊 Review: {review.get(\"status\")} (Risk: {review.get(\"risk_score\")})')
else:
    print(f'\n✗ Error: {result.get(\"error\")}')
    if result.get('blocking_violations'):
        print('\nBlocking Violations:')
        for violation in result.get('blocking_violations', []):
            print(f'  - {violation.get(\"code\")}: {violation.get(\"title\")}')
    exit(1)
"

