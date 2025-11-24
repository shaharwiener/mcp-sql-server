# services/sql_review/security_service.py
"""Security and schema change tracking tools for the SQL Review Agent.

Provides methods to inspect principals, permissions, and track security/schema changes.
"""

from typing import Dict, Any, List, Optional
from .base_service import BaseSQLReviewService
from .constants import DEFAULT_QUERY_TIMEOUT, DEFAULT_TIME_WINDOW_MINUTES, SECURITY_EVENT_CLASSES, SCHEMA_CHANGE_EVENT_CLASSES


class SecurityService(BaseSQLReviewService):
    """Service exposing security-related helper methods."""

    def __init__(self, connection_strings: Dict[str, Any] = None):
        super().__init__(connection_strings)

    def get_principals_and_roles(self, database: str, timeout_seconds: int = DEFAULT_QUERY_TIMEOUT) -> Dict[str, Any]:
        """List database principals and roles.
        
        Args:
            database: Database name.
            timeout_seconds: Query timeout.
        """
        # Validate database name
        is_valid, error = self._validate_database(database)
        if not is_valid:
            return {"success": False, "principals": [], "error": error}

        # Query sys.database_principals
        # We want name and type_desc.
        query = """
        SELECT name, type_desc
        FROM sys.database_principals
        ORDER BY type_desc, name;
        """
        
        try:
            conn = self.db_service.get_connection(database)
            conn.timeout = timeout_seconds
            cursor = conn.cursor()
            cursor.execute(query)
            
            principals = []
            if cursor.description:
                for row in cursor.fetchall():
                    principals.append({
                        "name": row.name,
                        "type_desc": row.type_desc
                    })

            self.audit_service.log_schema_access(
                database=database,
                scope="principals",
                success=True,
                user_id=None,
                error=None,
            )
            return {"success": True, "principals": principals, "error": None}
            
        except Exception as e:
            self.audit_service.log_schema_access(
                database=database,
                scope="principals",
                success=False,
                user_id=None,
                error=str(e),
            )
            return {"success": False, "principals": [], "error": f"Database error: {str(e)}"}

    def get_permissions_for_principal(self, database: str, principal_name: str, timeout_seconds: int = DEFAULT_QUERY_TIMEOUT) -> Dict[str, Any]:
        """List permissions for a specific principal.
        
        Args:
            database: Database name.
            principal_name: Name of the principal/user/role.
            timeout_seconds: Query timeout.
        """
        # Validate database name
        is_valid, error = self._validate_database(database)
        if not is_valid:
            return {"success": False, "permissions": [], "error": error}
            
        # Validate principal name (basic check)
        # We can use validate_table_name as it checks for valid identifiers
        is_valid_princ, error_princ = self._validate_object_name(principal_name)
        if not is_valid_princ:
             return {"success": False, "permissions": [], "error": error_princ}

        query = """
        SELECT 
            p.permission_name,
            p.state_desc,
            p.class_desc,
            CASE 
                WHEN p.class_desc = 'OBJECT_OR_COLUMN' THEN OBJECT_NAME(p.major_id)
                WHEN p.class_desc = 'SCHEMA' THEN SCHEMA_NAME(p.major_id)
                ELSE NULL 
            END as object_name
        FROM sys.database_permissions p
        JOIN sys.database_principals pr ON p.grantee_principal_id = pr.principal_id
        WHERE pr.name = ?
        ORDER BY p.class_desc, p.permission_name;
        """
        
        try:
            conn = self.db_service.get_connection(database)
            conn.timeout = timeout_seconds
            cursor = conn.cursor()
            cursor.execute(query, (principal_name,))
            
            permissions = []
            if cursor.description:
                for row in cursor.fetchall():
                    permissions.append({
                        "permission_name": row.permission_name,
                        "state_desc": row.state_desc,
                        "class_desc": row.class_desc,
                        "object_name": row.object_name
                    })

            self.audit_service.log_schema_access(
                database=database,
                scope="permissions",
                success=True,
                user_id=None,
                error=None,
            )
            return {"success": True, "permissions": permissions, "error": None}
            
        except Exception as e:
            self.audit_service.log_schema_access(
                database=database,
                scope="permissions",
                success=False,
                user_id=None,
                error=str(e),
            )
            return {"success": False, "permissions": [], "error": f"Database error: {str(e)}"}

    def get_recent_security_changes(self, time_window_minutes: int = DEFAULT_TIME_WINDOW_MINUTES, timeout_seconds: int = DEFAULT_QUERY_TIMEOUT) -> Dict[str, Any]:
        """Track recent security-related changes from default trace.
        
        Args:
            time_window_minutes: Lookback window in minutes.
            timeout_seconds: Query timeout.
        """
        # Security changes are instance-wide, connect to master or first allowed DB
        try:
            conn_db = self._get_system_db_connection()
        except ValueError as e:
            return {"success": False, "changes": [], "error": str(e)}
        
        query = f"""
        DECLARE @path NVARCHAR(260);
        SELECT @path = path FROM sys.traces WHERE is_default = 1;
        
        IF @path IS NOT NULL
        BEGIN
            SELECT 
                TE.name AS action,
                CONVERT(varchar, T.StartTime, 120) as event_time,
                T.LoginName as principal_name,
                T.ObjectName as object_name,
                T.TextData as details,
                T.TargetLoginName,
                T.TargetUserName
            FROM fn_trace_gettable(@path, DEFAULT) T
            JOIN sys.trace_events TE ON T.EventClass = TE.trace_event_id
            WHERE T.StartTime > DATEADD(minute, -{time_window_minutes}, GETDATE())
              AND T.EventClass IN ({','.join(map(str, SECURITY_EVENT_CLASSES))})
            ORDER BY T.StartTime DESC;
        END
        """
        
        try:
            conn = self.db_service.get_connection(conn_db)
            conn.timeout = timeout_seconds
            cursor = conn.cursor()
            cursor.execute(query)
            
            changes = []
            if cursor.description:
                for row in cursor.fetchall():
                    # Construct details
                    # TextData often contains the SQL or details.
                    # TargetLoginName/TargetUserName might be relevant.
                    
                    details = row.details or ""
                    if row.TargetLoginName:
                        details += f" (Target Login: {row.TargetLoginName})"
                    if row.TargetUserName:
                        details += f" (Target User: {row.TargetUserName})"
                        
                    changes.append({
                        "event_time": row.event_time,
                        "action": row.action,
                        "principal_name": row.principal_name,
                        "object_name": row.object_name,
                        "details": details.strip()
                    })
            
            # If no cursor description, it might be because @path was NULL (no result set).
            # In that case changes is empty, which is fine, or we can return an error/warning.
            # But the query as written returns nothing if @path is NULL.
            # Let's check if we want to be explicit.
            # If I change the query to return a row if null, I can detect it.
            # But for now, empty list is acceptable if trace is missing.

            self.audit_service.log_schema_access(
                database=conn_db,
                scope="security_changes",
                success=True,
                user_id=None,
                error=None,
            )
            return {"success": True, "changes": changes, "error": None}
            
        except Exception as e:
            self.audit_service.log_schema_access(
                database=conn_db if 'conn_db' in locals() else "unknown",
                scope="security_changes",
                success=False,
                user_id=None,
                error=str(e),
            )
            return {"success": False, "changes": [], "error": f"Database error: {str(e)}"}

    def get_recent_schema_changes(self, database: str, object_name: Optional[str] = None, time_window_minutes: int = DEFAULT_TIME_WINDOW_MINUTES, timeout_seconds: int = DEFAULT_QUERY_TIMEOUT) -> Dict[str, Any]:
        """Track recent schema changes (DDL) from default trace.
        
        Args:
            database: Database name.
            object_name: Optional object name filter.
            time_window_minutes: Lookback window in minutes.
            timeout_seconds: Query timeout.
        """
        # Validate database name
        is_valid, error = self._validate_database(database)
        if not is_valid:
            return {"success": False, "changes": [], "error": error}
            
        # Validate object name if provided
        if object_name:
            is_valid_obj, error_obj = self._validate_object_name(object_name)
            if not is_valid_obj:
                return {"success": False, "changes": [], "error": error_obj}

        # Build parameter list and object filter
        params = []
        object_filter = ""
        if object_name:
            object_filter = "AND T.ObjectName = ?"
            params.append(object_name)

        # Add database parameter
        params.append(database)

        query = f"""
        DECLARE @path NVARCHAR(260);
        SELECT @path = path FROM sys.traces WHERE is_default = 1;
        
        IF @path IS NOT NULL
        BEGIN
            SELECT 
                TE.name AS action,
                CONVERT(varchar, T.StartTime, 120) as event_time,
                T.ObjectName as object_name,
                T.ObjectType as object_type,
                T.TextData as details,
                T.DatabaseName
            FROM fn_trace_gettable(@path, DEFAULT) T
            JOIN sys.trace_events TE ON T.EventClass = TE.trace_event_id
            WHERE T.StartTime > DATEADD(minute, -{time_window_minutes}, GETDATE())
              AND T.DatabaseName = ?
              AND T.EventClass IN ({','.join(map(str, SCHEMA_CHANGE_EVENT_CLASSES))})
              {object_filter}
            ORDER BY T.StartTime DESC;
        END
        """
        
        try:
            conn = self.db_service.get_connection(database)
            conn.timeout = timeout_seconds
            cursor = conn.cursor()
            cursor.execute(query, params)
            
            changes = []
            if cursor.description:
                for row in cursor.fetchall():
                    changes.append({
                        "event_time": row.event_time,
                        "action": row.action,
                        "object_name": row.object_name,
                        "object_type": str(row.object_type) if row.object_type else None,
                        "details": row.details
                    })

            self.audit_service.log_schema_access(
                database=database,
                scope="schema_changes",
                success=True,
                user_id=None,
                error=None,
            )
            return {"success": True, "changes": changes, "error": None}
            
        except Exception as e:
            self.audit_service.log_schema_access(
                database=database,
                scope="schema_changes",
                success=False,
                user_id=None,
                error=str(e),
            )
            return {"success": False, "changes": [], "error": f"Database error: {str(e)}"}
