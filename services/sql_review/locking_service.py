# services/sql_review/locking_service.py
"""Locking and blocking analysis tools for the SQL Review Agent.

Provides a method to capture the current blocking snapshot in the database.
"""

from typing import Dict, Any, List
from .base_service import BaseSQLReviewService
from .constants import DEFAULT_QUERY_TIMEOUT, DEFAULT_TIME_WINDOW_MINUTES


class LockingService(BaseSQLReviewService):
    """Service exposing locking‑related helper methods.

    Currently implements:
    - get_current_blocking_snapshot: returns a list of active blocking sessions.
    """

    def __init__(self, connection_strings: Dict[str, Any] = None):
        super().__init__(connection_strings)

    def get_current_blocking_snapshot(self, database: str, include_query_text: bool = True, timeout_seconds: int = DEFAULT_QUERY_TIMEOUT) -> Dict[str, Any]:
        """Return a snapshot of currently blocked sessions.

        The query selects rows from ``sys.dm_exec_requests`` where a session is
        blocked by another session. The result includes ``blocking_session_id``,
        ``session_id``, ``wait_type``, ``wait_time_ms`` and ``statement_text``.
        """
        # Validate database name
        is_valid, error = self._validate_database(database)
        if not is_valid:
            return {"success": False, "sessions": [], "error": error}

        if include_query_text:
            query = (
                "SELECT "
                "   r.session_id, "
                "   r.blocking_session_id, "
                "   r.wait_type, "
                "   r.wait_time AS wait_time_ms, "
                "   t.text AS statement_text "
                "FROM sys.dm_exec_requests r "
                "CROSS APPLY sys.dm_exec_sql_text(r.sql_handle) t "
                "WHERE r.blocking_session_id <> 0;"
            )
        else:
            query = (
                "SELECT "
                "   session_id, "
                "   blocking_session_id, "
                "   wait_type, "
                "   wait_time AS wait_time_ms "
                "FROM sys.dm_exec_requests "
                "WHERE blocking_session_id <> 0;"
            )

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
            self.audit_service.log_schema_access(
                database=database,
                scope="blocking_snapshot",
                success=True,
                user_id=None,
                error=None,
            )
            return {"success": True, "sessions": rows, "error": None}
        except Exception as e:
            self.audit_service.log_schema_access(
                database=database,
                scope="blocking_snapshot",
                success=False,
                user_id=None,
                error=str(e),
            )
            return {"success": False, "sessions": [], "error": f"Database error: {str(e)}"}
    def get_recent_deadlocks(self, database: str, time_window_minutes: int = DEFAULT_TIME_WINDOW_MINUTES, timeout_seconds: int = DEFAULT_QUERY_TIMEOUT) -> Dict[str, Any]:
        """Return recent deadlock events from Extended Events / system_health.
        
        Args:
            database: Database name.
            time_window_minutes: Lookback window in minutes (default 1440 = 24h).
            timeout_seconds: Query timeout.
        """
        # Validate database name
        is_valid, error = self._validate_database(database)
        if not is_valid:
            return {"success": False, "deadlocks": [], "error": error}

        # Query system_health ring buffer for xml_deadlock_report
        query = f"""
        SELECT 
            xed.value('(@timestamp)[1]', 'varchar(50)') as event_time, 
            xed.query('data/value/deadlock').value('.', 'varchar(max)') as deadlock_xml
        FROM 
        (
            SELECT CAST([target_data] AS XML) AS TargetData
            FROM sys.dm_xe_session_targets st
            JOIN sys.dm_xe_sessions s ON s.address = st.event_session_address
            WHERE s.name = 'system_health' AND st.target_name = 'ring_buffer'
        ) AS Data
        CROSS APPLY TargetData.nodes('RingBufferTarget/event[@name="xml_deadlock_report"]') AS XEventData(xed)
        WHERE xed.value('(@timestamp)[1]', 'datetime') > DATEADD(minute, -{time_window_minutes}, GETDATE())
        ORDER BY xed.value('(@timestamp)[1]', 'datetime') DESC;
        """
        
        try:
            conn = self.db_service.get_connection(database)
            conn.timeout = timeout_seconds
            cursor = conn.cursor()
            cursor.execute(query)
            
            deadlocks = []
            import xml.etree.ElementTree as ET
            
            if cursor.description:
                for row in cursor.fetchall():
                    event_time = row[0]
                    xml_str = row[1]
                    
                    victim_spid = None
                    process_info = None
                    
                    try:
                        if xml_str:
                            # The XML returned is the content of <deadlock>...</deadlock>
                            # But sometimes it might be wrapped or just the inner part.
                            # Let's assume it's valid XML.
                            # We might need to wrap it in a root if it's a fragment, but here it should be <deadlock> root.
                            root = ET.fromstring(xml_str)
                            
                            # Find victim
                            victim_list = root.find("victim-list")
                            if victim_list is not None:
                                victim_process = victim_list.find("victimProcess")
                                if victim_process is not None:
                                    victim_spid = victim_process.get("id")
                            
                            # Find process info for victim
                            if victim_spid:
                                # Search in process-list
                                process_list = root.find("process-list")
                                if process_list is not None:
                                    for process in process_list.findall("process"):
                                        if process.get("id") == victim_spid:
                                            # Try to get inputbuf
                                            inputbuf = process.find("inputbuf")
                                            if inputbuf is not None:
                                                process_info = inputbuf.text
                                            break
                    except Exception as parse_err:
                        process_info = f"Error parsing XML: {str(parse_err)}"

                    deadlocks.append({
                        "event_time": event_time,
                        "victim_spid": int(victim_spid) if victim_spid and victim_spid.isdigit() else None,
                        "process_info": process_info
                    })

            self.audit_service.log_schema_access(
                database=database,
                scope="deadlocks",
                success=True,
                user_id=None,
                error=None,
            )
            return {"success": True, "deadlocks": deadlocks, "error": None}
            
        except Exception as e:
            self.audit_service.log_schema_access(
                database=database,
                scope="deadlocks",
                success=False,
                user_id=None,
                error=str(e),
            )
            # Return success=True with empty list on error to be graceful, or False?
            # Requirement says "error?: string".
            return {"success": False, "deadlocks": [], "error": f"Database error: {str(e)}"}
