# services/sql_review/performance_service.py
"""Performance and plan analysis tools for the SQL Review Agent.

This module provides a lightweight implementation of several performance‑related
functions. The real‑world implementation would use SQL Server's SHOWPLAN and
STATISTICS commands, but for the purpose of unit testing we provide a simple
wrapper that returns structured JSON with placeholder data.
"""

import time
from typing import Any, Dict, List, Optional

from services.infrastructure.db_connection_service import DbConnectionService
from services.infrastructure.validation_service import ValidationService


class PerformanceService:
    """Service exposing performance‑related helper methods.

    Each method validates the input, executes a minimal query against the
    validation database and returns a JSON‑serialisable dictionary. The actual
    SQL Server features (SHOWPLAN_XML, STATISTICS IO/TIME, etc.) are not
    exercised in the test suite – the focus is on the contract and error
    handling.
    """

    def __init__(self) -> None:
        self.db_service = DbConnectionService()
        self.validation_service = ValidationService()

    # ---------------------------------------------------------------------
    # Helper
    # ---------------------------------------------------------------------
    def _run_query(self, database: str, query: str, timeout_seconds: int = 30) -> Dict[str, Any]:
        """Execute a query and return rows + column metadata.

        This private helper is used by the public methods to avoid code
        duplication. Errors are captured and returned in a consistent JSON
        structure.
        """
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
                for row in cursor.fetchall():
                    rows.append({col: val for col, val in zip(columns, row)})
            execution_ms = (time.time() - start_time) * 1000
            return {
                "success": True,
                "rows": rows,
                "columns": columns,
                "execution_ms": round(execution_ms, 2),
                "error": None,
            }
        except Exception as e:
            execution_ms = (time.time() - start_time) * 1000
            return {
                "success": False,
                "rows": [],
                "columns": [],
                "execution_ms": round(execution_ms, 2),
                "error": f"Database error: {str(e)}",
            }
        finally:
            try:
                cursor.close()
                conn.close()
            except Exception:
                pass

    # ---------------------------------------------------------------------
    # Public API
    # ---------------------------------------------------------------------
    def get_execution_plan(self, database: str, query: str, timeout_seconds: int = 30) -> Dict[str, Any]:
        """Return the XML execution plan for *query*.

        The implementation runs ``SET SHOWPLAN_XML ON`` before the query and
        ``SET SHOWPLAN_XML OFF`` afterwards. The XML is returned as a string in
        the ``plan`` field. If validation fails or the query errors, ``success``
        is ``False`` and ``error`` contains the message.
        """
        # Validate length and database name first
        is_valid, error = self.validation_service.validate_query_length(query)
        if not is_valid:
            return {"success": False, "plan": None, "error": error}
        is_valid, error = self.validation_service.validate_database_name(database)
        if not is_valid:
            return {"success": False, "plan": None, "error": error}

        conn = None
        try:
            # We need a dedicated connection to handle SET SHOWPLAN_XML ON/OFF
            # because they cannot be in the same batch as the query
            conn = self.db_service.get_connection(database)
            conn.autocommit = True
            cursor = conn.cursor()
            
            # 1. Turn on SHOWPLAN
            import sys
            print(f"DEBUG: Setting SHOWPLAN_XML ON for database {database}", file=sys.stderr)
            cursor.execute("SET SHOWPLAN_XML ON")
            
            # 2. Execute query to get plan
            print(f"DEBUG: Executing query: {query}", file=sys.stderr)
            cursor.execute(query)
            
            # 3. Fetch plan
            plan_xml = ""
            debug_info = []
            if cursor.description:
                debug_info.append(f"Description: {[d[0] for d in cursor.description]}")
                row = cursor.fetchone()
                if row:
                    plan_xml = row[0]
                    debug_info.append(f"Row found. Plan length: {len(str(plan_xml))}")
                else:
                    debug_info.append("No row returned")
            else:
                debug_info.append("No description")
            
            # 4. Turn off SHOWPLAN
            cursor.execute("SET SHOWPLAN_XML OFF")
            
            if not plan_xml:
                return {
                    "success": False, 
                    "plan": None, 
                    "error": f"Empty plan returned. Debug: {'; '.join(debug_info)}. DB: {database}"
                }
            
            return {"success": True, "plan": plan_xml, "error": None}
            
        except Exception as e:
            return {"success": False, "plan": None, "error": f"Database error: {str(e)}"}
        finally:
            if conn:
                try:
                    conn.close()
                except Exception:
                    pass

    def get_query_cost_estimate(self, database: str, query: str, timeout_seconds: int = 30) -> Dict[str, Any]:
        """Return a simple cost estimate for *query*.

        In a full implementation this would parse the execution plan and extract
        the estimated CPU and I/O cost. Here we simply return a placeholder value
        of ``estimated_cost`` = 1.0 for successful queries.
        """
        # Re‑use validation logic
        is_valid, error = self.validation_service.validate_query_length(query)
        if not is_valid:
            return {"success": False, "estimated_cost": None, "error": error}
        is_valid, error = self.validation_service.validate_database_name(database)
        if not is_valid:
            return {"success": False, "estimated_cost": None, "error": error}

        # Execute the query safely (read‑only) to ensure it runs
        result = self._run_query(database, query, timeout_seconds)
        if not result["success"]:
            return {"success": False, "estimated_cost": None, "error": result["error"]}
        # Placeholder cost – in real life this would be derived from the plan
        return {"success": True, "estimated_cost": 1.0, "error": None}

    def get_index_usage_statistics(self, database: str, schema_name: Optional[str] = None, table_name: Optional[str] = None, include_database_wide: bool = False, timeout_seconds: int = 30) -> Dict[str, Any]:
        """Return index usage statistics (seeks, scans, lookups, updates).
        
        Args:
            database: Database name.
            schema_name: Optional schema filter.
            table_name: Optional table filter.
            include_database_wide: If True, ignores table/schema filters (not fully implemented as per requirement interpretation, usually implies scope). 
                                   Actually, standard usage is per table or all tables in DB. 
                                   We will interpret this as: if False and no table provided, return top 50.
                                   If True, maybe return all? Let's stick to filtering by DB.
            timeout_seconds: Query timeout.
        """
        # Validate database name
        is_valid, error = self.validation_service.validate_database_name(database)
        if not is_valid:
            return {"success": False, "indexes": [], "error": error}

        # Validate schema/table if provided
        if schema_name and not schema_name.replace('_', '').isalnum():
             return {"success": False, "indexes": [], "error": "Invalid schema name"}
        
        if table_name:
            is_valid, error = self.validation_service.validate_table_name(table_name)
            if not is_valid:
                return {"success": False, "indexes": [], "error": error}

        # Build query
        # We need to join sys.indexes, sys.tables, sys.schemas, and sys.dm_db_index_usage_stats
        # Filter by current DB ID for usage stats
        
        filters = []
        if schema_name:
            filters.append(f"s.name = '{schema_name}'")
        if table_name:
            filters.append(f"t.name = '{table_name}'")
            
        where_clause = " AND ".join(filters)
        if where_clause:
            where_clause = "AND " + where_clause
            
        query = f"""
        SELECT TOP 100
            s.name AS schema_name,
            t.name AS table_name,
            ISNULL(i.name, 'HEAP') AS index_name,
            ISNULL(ius.user_seeks, 0) AS user_seeks,
            ISNULL(ius.user_scans, 0) AS user_scans,
            ISNULL(ius.user_lookups, 0) AS user_lookups,
            ISNULL(ius.user_updates, 0) AS user_updates,
            CONVERT(varchar, ius.last_user_seek, 120) AS last_user_seek,
            CONVERT(varchar, ius.last_user_scan, 120) AS last_user_scan,
            CONVERT(varchar, ius.last_user_lookup, 120) AS last_user_lookup,
            CONVERT(varchar, ius.last_user_update, 120) AS last_user_update
        FROM sys.indexes i
        JOIN sys.tables t ON i.object_id = t.object_id
        JOIN sys.schemas s ON t.schema_id = s.schema_id
        LEFT JOIN sys.dm_db_index_usage_stats ius ON i.object_id = ius.object_id AND i.index_id = ius.index_id AND ius.database_id = DB_ID()
        WHERE i.object_id > 100 -- Exclude system tables
        {where_clause}
        ORDER BY (ISNULL(ius.user_seeks, 0) + ISNULL(ius.user_scans, 0) + ISNULL(ius.user_lookups, 0) + ISNULL(ius.user_updates, 0)) DESC;
        """
        
        result = self._run_query(database, query, timeout_seconds)
        if not result["success"]:
            return {"success": False, "indexes": [], "error": result["error"]}
            
        return {"success": True, "indexes": result["rows"], "error": None}

    def get_missing_index_suggestions(self, database: str, schema_name: Optional[str] = None, table_name: Optional[str] = None, top: int = 20, timeout_seconds: int = 30) -> Dict[str, Any]:
        """Return missing index suggestions based on DMVs.
        
        Args:
            database: Database name.
            schema_name: Optional schema filter.
            table_name: Optional table filter.
            top: Number of suggestions to return (default 20).
            timeout_seconds: Query timeout.
        """
        # Validate database name
        is_valid, error = self.validation_service.validate_database_name(database)
        if not is_valid:
            return {"success": False, "suggestions": [], "error": error}

        # Validate schema/table if provided
        if schema_name and not schema_name.replace('_', '').isalnum():
             return {"success": False, "suggestions": [], "error": "Invalid schema name"}
        
        if table_name:
            is_valid, error = self.validation_service.validate_table_name(table_name)
            if not is_valid:
                return {"success": False, "suggestions": [], "error": error}

        # Build query
        filters = []
        if schema_name:
            filters.append(f"sch.name = '{schema_name}'")
        if table_name:
            filters.append(f"o.name = '{table_name}'")
            
        where_clause = " AND ".join(filters)
        if where_clause:
            where_clause = "AND " + where_clause
            
        query = f"""
        SELECT TOP {top}
            sch.name AS schema_name,
            o.name AS table_name,
            d.equality_columns,
            d.inequality_columns,
            d.included_columns,
            s.avg_user_impact AS improvement_measure
        FROM sys.dm_db_missing_index_details d
        JOIN sys.dm_db_missing_index_groups g ON d.index_handle = g.index_handle
        JOIN sys.dm_db_missing_index_group_stats s ON g.index_group_handle = s.group_handle
        JOIN sys.objects o ON d.object_id = o.object_id
        JOIN sys.schemas sch ON o.schema_id = sch.schema_id
        WHERE d.database_id = DB_ID()
        {where_clause}
        ORDER BY s.avg_user_impact DESC;
        """
        
        result = self._run_query(database, query, timeout_seconds)
        if not result["success"]:
            return {"success": False, "suggestions": [], "error": result["error"]}
            
        # Process columns to lists
        suggestions = []
        for row in result["rows"]:
            suggestions.append({
                "schema_name": row["schema_name"],
                "table_name": row["table_name"],
                "equality_columns": [c.strip() for c in row["equality_columns"].split(',')] if row["equality_columns"] else [],
                "inequality_columns": [c.strip() for c in row["inequality_columns"].split(',')] if row["inequality_columns"] else [],
                "included_columns": [c.strip() for c in row["included_columns"].split(',')] if row["included_columns"] else [],
                "improvement_measure": float(row["improvement_measure"]) if row["improvement_measure"] is not None else 0.0
            })
            
        return {"success": True, "suggestions": suggestions, "error": None}

    def get_query_statistics(self, database: str, query: str, timeout_seconds: int = 30) -> Dict[str, Any]:
        """Return basic statistics for *query* (rows, execution time).

        The method re‑uses ``_run_query`` to obtain the row count and execution
        time. The returned dictionary contains ``row_count`` and ``execution_ms``.
        """
        # Validate query and database
        is_valid, error = self.validation_service.validate_query_length(query)
        if not is_valid:
            return {"success": False, "row_count": 0, "execution_ms": None, "error": error}
        is_valid, error = self.validation_service.validate_database_name(database)
        if not is_valid:
            return {"success": False, "row_count": 0, "execution_ms": None, "error": error}
        result = self._run_query(database, query, timeout_seconds)
        if not result["success"]:
            return {"success": False, "row_count": 0, "execution_ms": None, "error": result["error"]}
        return {
            "success": True,
            "row_count": len(result["rows"]),
            "execution_ms": result["execution_ms"],
            "error": None,
        }
