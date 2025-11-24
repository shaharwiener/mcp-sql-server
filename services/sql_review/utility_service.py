# services/sql_review/utility_service.py
"""Utility tools for the SQL Review Agent.

Provides methods for sampling query results and searching database objects.
"""

from typing import Dict, Any, List, Optional
from .base_service import BaseSQLReviewService
from .constants import DEFAULT_QUERY_TIMEOUT, DEFAULT_SAMPLE_SIZE, VALID_OBJECT_TYPES, OBJECT_TYPE_MAP


class UtilityService(BaseSQLReviewService):
    """Service exposing utility helper methods."""

    def __init__(self, connection_strings: Dict[str, Any] = None):
        super().__init__(connection_strings)

    def sample_query_results(self, database: str, query: str, max_rows: int = DEFAULT_SAMPLE_SIZE, timeout_seconds: int = DEFAULT_QUERY_TIMEOUT) -> Dict[str, Any]:
        """Execute a query and return a sample of results.
        
        Args:
            database: Database name.
            query: SQL query to execute.
            max_rows: Maximum rows to return.
            timeout_seconds: Query timeout.
        """
        # Validate database name
        is_valid, error = self._validate_database(database)
        if not is_valid:
            return {"success": False, "rows": [], "columns": [], "error": error}
            
        # Validate query length
        is_valid, error = self._validate_query_length(query)
        if not is_valid:
            return {"success": False, "rows": [], "columns": [], "error": error}

        # Wrap query with TOP clause if it doesn't already have one
        # This is a simple approach - for production, more sophisticated parsing might be needed
        query_upper = query.strip().upper()
        if not query_upper.startswith("SELECT TOP"):
            # Insert TOP clause after SELECT
            if query_upper.startswith("SELECT"):
                query = f"SELECT TOP {max_rows} " + query[6:].strip()
            else:
                # If not a SELECT, just execute as-is (might be a stored proc call, etc.)
                pass
        
        try:
            conn = self.db_service.get_connection(database)
            conn.timeout = timeout_seconds
            cursor = conn.cursor()
            cursor.execute(query)
            
            rows = []
            columns = []
            
            if cursor.description:
                # Build column metadata
                for col in cursor.description:
                    columns.append({
                        "name": col[0],
                        "type": str(col[1].__name__) if col[1] else "unknown"
                    })
                
                # Fetch rows (limited by TOP clause or max_rows)
                for row in cursor.fetchmany(max_rows):
                    row_dict = {}
                    for i, col in enumerate(cursor.description):
                        value = row[i]
                        # Convert non-serializable types to strings
                        if value is not None and not isinstance(value, (str, int, float, bool)):
                            value = str(value)
                        row_dict[col[0]] = value
                    rows.append(row_dict)

            self.audit_service.log_schema_access(
                database=database,
                scope="sample_query",
                success=True,
                user_id=None,
                error=None,
            )
            return {"success": True, "rows": rows, "columns": columns, "error": None}
            
        except Exception as e:
            self.audit_service.log_schema_access(
                database=database,
                scope="sample_query",
                success=False,
                user_id=None,
                error=str(e),
            )
            return {"success": False, "rows": [], "columns": [], "error": f"Database error: {str(e)}"}

    def search_objects(self, database: str, pattern: str, object_type: Optional[str] = None, timeout_seconds: int = DEFAULT_QUERY_TIMEOUT) -> Dict[str, Any]:
        """Search for database objects by name pattern.
        
        Args:
            database: Database name.
            pattern: Search pattern (supports SQL LIKE wildcards).
            object_type: Optional filter by object type (table, view, procedure, function, trigger).
            timeout_seconds: Query timeout.
        """
        # Validate database name
        is_valid, error = self._validate_database(database)
        if not is_valid:
            return {"success": False, "objects": [], "error": error}

        # Validate object type if provided
        if object_type and object_type.lower() not in VALID_OBJECT_TYPES:
            return {"success": False, "objects": [], "error": f"Invalid object_type. Must be one of: {', '.join(VALID_OBJECT_TYPES)}"}
        
        # Build type filter
        type_filter = ""
        if object_type:
            # Map friendly name to SQL Server type code
            type_code = OBJECT_TYPE_MAP.get(object_type.lower())
            if type_code:
                # For functions, we need to include all function-related types
                if object_type.lower() == "function":
                    type_filter = "AND o.type IN ('FN', 'IF', 'TF')"
                else:
                    type_filter = f"AND o.type = '{type_code}'"
        
        # Sanitize pattern for LIKE - basic validation
        # Pattern should only contain alphanumeric, underscore, percent, and brackets
        if not all(c.isalnum() or c in ['_', '%', '[', ']', ' '] for c in pattern):
            return {"success": False, "objects": [], "error": "Invalid pattern. Only alphanumeric, %, _, and [] allowed."}

        query = f"""
        SELECT 
            s.name AS schema_name,
            o.name AS object_name,
            CASE o.type
                WHEN 'U' THEN 'table'
                WHEN 'V' THEN 'view'
                WHEN 'P' THEN 'procedure'
                WHEN 'FN' THEN 'function'
                WHEN 'IF' THEN 'function'
                WHEN 'TF' THEN 'function'
                WHEN 'TR' THEN 'trigger'
                ELSE o.type
            END AS object_type
        FROM sys.objects o
        JOIN sys.schemas s ON o.schema_id = s.schema_id
        WHERE o.name LIKE ?
        {type_filter}
        AND o.is_ms_shipped = 0
        AND o.type IN ('U', 'V', 'P', 'FN', 'IF', 'TF', 'TR')
        ORDER BY s.name, o.name;
        """
        
        try:
            conn = self.db_service.get_connection(database)
            conn.timeout = timeout_seconds
            cursor = conn.cursor()
            cursor.execute(query, (pattern,))
            
            objects = []
            if cursor.description:
                for row in cursor.fetchall():
                    objects.append({
                        "schema_name": row.schema_name,
                        "object_name": row.object_name,
                        "object_type": row.object_type
                    })

            self.audit_service.log_schema_access(
                database=database,
                scope="search_objects",
                success=True,
                user_id=None,
                error=None,
            )
            return {"success": True, "objects": objects, "error": None}
            
        except Exception as e:
            self.audit_service.log_schema_access(
                database=database,
                scope="search_objects",
                success=False,
                user_id=None,
                error=str(e),
            )
            return {"success": False, "objects": [], "error": f"Database error: {str(e)}"}
