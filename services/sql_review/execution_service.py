"""
SQL Review - Execution Service

Provides tools for executing and validating SQL queries and scripts.
"""
import time
import pyodbc
import structlog
from typing import Dict, Any, List, Optional
from services.infrastructure.db_connection_service import DbConnectionService
from services.infrastructure.validation_service import ValidationService
from services.common.exceptions import DatabaseError, ValidationError
from config.settings import settings

logger = structlog.get_logger()


class ExecutionService:
    """Service for executing and validating SQL queries and scripts."""
    
    def __init__(self):
        """Initialize execution service."""
        self.db_service = DbConnectionService()
        self.validation_service = ValidationService()

    def _error_response(self, error_message: str) -> Dict[str, Any]:
        """Helper to return standardized error response."""
        return {
            "success": False,
            "rows": [],
            "columns": [],
            "row_count": 0,
            "execution_ms": None,
            "error": error_message
        }

    
    def execute_sql_query(
        self,
        database: str,
        query: str,
        max_rows: Optional[int] = None,
        timeout_seconds: int = 60
    ) -> Dict[str, Any]:
        """Execute a read‑only SELECT query and return results.

        Args:
            database: Target database name.
            query: SQL query string (must be SELECT‑only).
            max_rows: Optional limit on rows returned. If ``None`` the value is taken from
                :py:meth:`settings.MAX_RESULT_ROWS` (default 5000).
            timeout_seconds: Query timeout in seconds.
        """
        # Validate query length and structure
        is_valid, error = self.validation_service.validate_query_length(query)
        if not is_valid:
            return self._error_response(error)
        is_valid, error = self.validation_service.validate_query_comprehensive(query)
        if not is_valid:
            return self._error_response(error)
        if not self._is_select_only(query):
            return self._error_response("Only SELECT queries are allowed.")

        # Resolve row limit – use config if not supplied
        if max_rows is None:
            max_rows = settings.MAX_RESULT_ROWS
        if max_rows <= 0:
            return self._error_response("max_rows must be a positive integer.")

        # Ensure a TOP clause is present to enforce the limit at the DB level
        query_upper = query.strip().upper()
        if not query_upper.startswith("SELECT TOP"):
            if query_upper.startswith("SELECT"):
                query = f"SELECT TOP {max_rows} " + query[6:].strip()

        start_time = time.time()
        try:
            conn = self.db_service.get_connection(database)
            conn.timeout = timeout_seconds
            cursor = conn.cursor()
            cursor.execute(query)
            rows = []
            columns = []
            if cursor.description:
                columns = [desc[0] for desc in cursor.description]
                for row in cursor.fetchmany(max_rows):
                    rows.append({col: val for col, val in zip(columns, row)})
            execution_ms = (time.time() - start_time) * 1000
            return {
                "success": True,
                "rows": rows,
                "columns": columns,
                "row_count": len(rows),
                "execution_ms": round(execution_ms, 2),
                "error": None,
            }
        except pyodbc.Error as e:
            execution_ms = (time.time() - start_time) * 1000
            return {
                "success": False,
                "rows": [],
                "columns": [],
                "row_count": 0,
                "execution_ms": round(execution_ms, 2),
                "error": f"Database error: {str(e)}",
            }
        except Exception as e:
            execution_ms = (time.time() - start_time) * 1000
            return {
                "success": False,
                "rows": [],
                "columns": [],
                "row_count": 0,
                "execution_ms": round(execution_ms, 2),
                "error": f"Unexpected error: {str(e)}",
            }
        """
        Execute a read-only T-SQL query (SELECT, system views, DMVs) and return rows.
        
        Args:
            database: Database name to query
            query: SQL query to execute (must be read-only)
            max_rows: Maximum number of rows to return (default 1000)
            timeout_seconds: Query timeout in seconds (default 60)
            
        Returns:
            Dictionary containing:
            - success: bool
            - rows: List of row dictionaries (columnName: value)
            - columns: List of column metadata {name, type}
            - row_count: Number of rows returned
            - execution_ms: Execution time in milliseconds
            - error: Error message if failed
        """
        start_time = time.time()
        
        try:
            # Validate query length
            is_valid, error = self.validation_service.validate_query_length(query)
            if not is_valid:
                return {
                    "success": False,
                    "rows": [],
                    "columns": [],
                    "row_count": 0,
                    "execution_ms": None,
                    "error": error
                }
            
            # Comprehensive query validation (structure, dangerous patterns)
            is_valid, error = self.validation_service.validate_query_comprehensive(query)
            if not is_valid:
                return {
                    "success": False,
                    "rows": [],
                    "columns": [],
                    "row_count": 0,
                    "execution_ms": None,
                    "error": error
                }
            
            # Validate query is SELECT-only (read-only)
            if not self._is_select_only(query):
                return {
                    "success": False,
                    "rows": [],
                    "columns": [],
                    "row_count": 0,
                    "execution_ms": None,
                    "error": "Only SELECT queries are allowed. Write operations (UPDATE/INSERT/DELETE/DROP/CREATE/ALTER) are blocked."
                }
            
            # Validate database name
            is_valid, error = self.validation_service.validate_database_name(database)
            if not is_valid:
                return {
                    "success": False,
                    "rows": [],
                    "columns": [],
                    "row_count": 0,
                    "execution_ms": None,
                    "error": error
                }

            
            # Get database connection
            conn = self.db_service.get_connection(database)
            
            # Set query timeout on connection
            conn.timeout = timeout_seconds
            
            cursor = conn.cursor()
            
            try:
                # Execute query
                query_start = time.time()
                cursor.execute(query)
                
                # Get column metadata
                columns = []
                if cursor.description:
                    for col in cursor.description:
                        columns.append({
                            "name": col[0],
                            "type": self._get_type_name(col[1])
                        })
                
                # Fetch rows (up to max_rows)
                rows = []
                row_count = 0
                for row in cursor.fetchall():
                    if row_count >= max_rows:
                        break
                    row_dict = {}
                    for idx, col in enumerate(columns):
                        value = row[idx]
                        # Convert non-JSON-serializable types
                        if value is not None:
                            row_dict[col["name"]] = self._serialize_value(value)
                        else:
                            row_dict[col["name"]] = None
                    rows.append(row_dict)
                    row_count += 1
                
                query_end = time.time()
                execution_ms = (query_end - query_start) * 1000
                
                return {
                    "success": True,
                    "rows": rows,
                    "columns": columns,
                    "row_count": row_count,
                    "execution_ms": round(execution_ms, 2),
                    "error": None
                }
                
            finally:
                cursor.close()
                conn.close()
                
        except pyodbc.Error as e:
            execution_ms = (time.time() - start_time) * 1000
            return {
                "success": False,
                "rows": [],
                "columns": [],
                "row_count": 0,
                "execution_ms": round(execution_ms, 2),
                "error": f"Database error: {str(e)}"
            }
        except Exception as e:
            execution_ms = (time.time() - start_time) * 1000
            return {
                "success": False,
                "rows": [],
                "columns": [],
                "row_count": 0,
                "execution_ms": round(execution_ms, 2),
                "error": f"Unexpected error: {str(e)}"
            }
    
    def execute_sql_script_transactional(
        self,
        database: str,
        script: str,
        timeout_seconds: int = 120
    ) -> Dict[str, Any]:
        """
        Execute an arbitrary script (DDL/DML) inside an explicit transaction and ALWAYS roll back.
        Used for runtime validation and estimating impact without persisting changes.
        
        Args:
            database: Database name to execute against
            script: SQL script to execute (can include DDL/DML)
            timeout_seconds: Script timeout in seconds (default 120)
            
        Returns:
            Dictionary containing:
            - success: bool (True if script executed without errors)
            - messages: List of PRINT messages and info messages
            - statement_results: List of {statement_text, rows_affected}
            - error: Error message if failed
        """
        start_time = time.time()
        
        try:
            # Validate script length
            is_valid, error = self.validation_service.validate_query_length(script)
            if not is_valid:
                return {
                    "success": False,
                    "messages": [],
                    "statement_results": [],
                    "error": error
                }
            
            # Validate database name
            is_valid, error = self.validation_service.validate_database_name(database)
            if not is_valid:
                return {
                    "success": False,
                    "messages": [],
                    "statement_results": [],
                    "error": error
                }
            
            # Get database connection
            conn = self.db_service.get_connection(database)
            conn.timeout = timeout_seconds
            conn.autocommit = False  # Ensure manual transaction control
            
            cursor = conn.cursor()
            messages = []
            statement_results = []
            
            try:
                # Begin explicit transaction
                cursor.execute("BEGIN TRANSACTION")
                
                # Split script into statements (simple split by GO or semicolon)
                statements = self._split_sql_script(script)
                
                for stmt in statements:
                    if not stmt.strip():
                        continue
                    
                    try:
                        # Execute statement
                        cursor.execute(stmt)
                        
                        # Capture rows affected
                        rows_affected = cursor.rowcount if cursor.rowcount >= 0 else None
                        
                        statement_results.append({
                            "statement_text": stmt[:100] + "..." if len(stmt) > 100 else stmt,
                            "rows_affected": rows_affected
                        })
                        
                        # Capture any messages (this is tricky with pyodbc, may not capture all)
                        # We'll rely on the statement results primarily
                        
                    except pyodbc.Error as stmt_error:
                        # Statement failed - record error and stop
                        return {
                            "success": False,
                            "messages": messages,
                            "statement_results": statement_results,
                            "error": f"Statement failed: {str(stmt_error)}"
                        }
                
                # Always rollback
                cursor.execute("ROLLBACK TRANSACTION")
                
                execution_ms = (time.time() - start_time) * 1000
                
                return {
                    "success": True,
                    "messages": messages,
                    "statement_results": statement_results,
                    "execution_ms": round(execution_ms, 2),
                    "error": None
                }
                
            finally:
                try:
                    # Ensure rollback even if something went wrong
                    cursor.execute("IF @@TRANCOUNT > 0 ROLLBACK TRANSACTION")
                except:
                    pass
                cursor.close()
                conn.close()
                
        except pyodbc.Error as e:
            execution_ms = (time.time() - start_time) * 1000
            return {
                "success": False,
                "messages": [],
                "statement_results": [],
                "execution_ms": round(execution_ms, 2),
                "error": f"Database error: {str(e)}"
            }
        except Exception as e:
            execution_ms = (time.time() - start_time) * 1000
            return {
                "success": False,
                "messages": [],
                "statement_results": [],
                "execution_ms": round(execution_ms, 2),
                "error": f"Unexpected error: {str(e)}"
            }
    
    def parse_sql_script(
        self,
        database: str,
        script: str
    ) -> Dict[str, Any]:
        """
        Validate syntax only, without executing.
        Uses SET PARSEONLY ON to detect syntax errors.
        
        Args:
            database: Database name (for context)
            script: SQL script to validate
            
        Returns:
            Dictionary containing:
            - valid: bool
            - errors: List of error messages with line/position info if possible
        """
        try:
            # Validate script length
            is_valid, error = self.validation_service.validate_query_length(script)
            if not is_valid:
                return {
                    "valid": False,
                    "errors": [error]
                }
            
            # Validate database name
            is_valid, error = self.validation_service.validate_database_name(database)
            if not is_valid:
                return {
                    "valid": False,
                    "errors": [error]
                }
            
            # Get database connection
            conn = self.db_service.get_connection(database)
            cursor = conn.cursor()
            
            try:
                # Enable parse-only mode
                cursor.execute("SET PARSEONLY ON")
                
                # Try to parse the script
                try:
                    cursor.execute(script)
                    # If we get here, syntax is valid
                    cursor.execute("SET PARSEONLY OFF")
                    
                    return {
                        "valid": True,
                        "errors": []
                    }
                    
                except pyodbc.Error as parse_error:
                    # Syntax error detected
                    cursor.execute("SET PARSEONLY OFF")
                    
                    error_msg = str(parse_error)
                    # Try to extract line number if available
                    return {
                        "valid": False,
                        "errors": [error_msg]
                    }
                    
            finally:
                try:
                    cursor.execute("SET PARSEONLY OFF")
                except:
                    pass
                cursor.close()
                conn.close()
                
        except Exception as e:
            return {
                "valid": False,
                "errors": [f"Unexpected error: {str(e)}"]
            }
    
    def _split_sql_script(self, script: str) -> List[str]:
        """Split SQL script into individual statements."""
        # Simple split by GO or semicolon
        # This is a basic implementation - production code might need more sophisticated parsing
        import re
        
        # Remove comments
        script = re.sub(r'--.*$', '', script, flags=re.MULTILINE)
        script = re.sub(r'/\*.*?\*/', '', script, flags=re.DOTALL)
        
        # Split by GO (case-insensitive, must be on its own line)
        statements = re.split(r'^\s*GO\s*$', script, flags=re.MULTILINE | re.IGNORECASE)
        
        # Further split by semicolons if no GO statements
        if len(statements) == 1:
            statements = [s.strip() for s in script.split(';') if s.strip()]
        
        return [s.strip() for s in statements if s.strip()]
    
    
    def _is_select_only(self, query: str) -> bool:
        """Check if query is SELECT-only (read-only)."""
        import re
        # Remove comments and whitespace
        cleaned = re.sub(r'--.*$', '', query, flags=re.MULTILINE)
        cleaned = re.sub(r'/\*.*?\*/', '', cleaned, flags=re.DOTALL)
        cleaned = cleaned.strip()
        
        # Check if starts with SELECT
        if not re.match(r'^\s*SELECT\b', cleaned, re.IGNORECASE):
            return False
        
        # Check for write operations
        unsafe_patterns = r'\b(UPDATE|INSERT|DELETE|DROP|CREATE|ALTER|TRUNCATE|EXEC|EXECUTE)\b'
        if re.search(unsafe_patterns, cleaned, re.IGNORECASE):
            return False
        
        return True
    
    def _get_type_name(self, type_code) -> str:
        """Convert pyodbc type code to readable type name."""
        type_map = {
            str: "string",
            int: "integer",
            float: "float",
            bool: "boolean",
            bytes: "binary"
        }
        return type_map.get(type_code, "unknown")
    
    def _serialize_value(self, value: Any) -> Any:
        """Convert value to JSON-serializable format."""
        import datetime
        import decimal
        
        if isinstance(value, (datetime.date, datetime.datetime)):
            return value.isoformat()
        elif isinstance(value, decimal.Decimal):
            return float(value)
        elif isinstance(value, bytes):
            return value.decode('utf-8', errors='replace')
        else:
            return value
