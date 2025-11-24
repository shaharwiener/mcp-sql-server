# services/sql_review/agent_service.py
"""SQL Server Agent analysis tools for the SQL Review Agent.

Provides methods to inspect SQL Agent jobs, history, and logs.
"""

from typing import Dict, Any, List, Optional
from .base_service import BaseSQLReviewService
from .constants import DEFAULT_QUERY_TIMEOUT, DEFAULT_MAX_ROWS, DEFAULT_TIME_WINDOW_MINUTES, JOB_STATUS_MAP


class AgentService(BaseSQLReviewService):
    """Service exposing SQL Agent-related helper methods."""

    def __init__(self, connection_strings: Dict[str, Any] = None):
        super().__init__(connection_strings)

    def get_sql_agent_jobs(self, include_disabled: bool = False, timeout_seconds: int = DEFAULT_QUERY_TIMEOUT) -> Dict[str, Any]:
        """List jobs and their schedule/last status.
        
        Args:
            include_disabled: If True, include disabled jobs.
            timeout_seconds: Query timeout.
        """
        # Agent jobs are instance-wide (stored in msdb)
        # Connect to master or first allowed database
        try:
            db_name = self._get_system_db_connection()
        except ValueError as e:
            return {"success": False, "jobs": [], "error": str(e)}
        
        # Filter for enabled/disabled
        enabled_filter = "" if include_disabled else "AND j.enabled = 1"
        
        query = f"""
        SELECT 
            j.name,
            j.enabled,
            CASE h.run_status
                WHEN 0 THEN 'Failed'
                WHEN 1 THEN 'Succeeded'
                WHEN 2 THEN 'Retry'
                WHEN 3 THEN 'Canceled'
                ELSE 'Unknown'
            END AS last_run_outcome,
            NULLIF(STR(h.run_date, 8, 0), '0') AS last_run_date_str,
            NULLIF(STR(h.run_time, 6, 0), '0') AS last_run_time_str,
            NULLIF(STR(s.next_run_date, 8, 0), '0') AS next_run_date_str,
            NULLIF(STR(s.next_run_time, 6, 0), '0') AS next_run_time_str
        FROM msdb.dbo.sysjobs j
        LEFT JOIN (
            SELECT job_id, run_status, run_date, run_time,
                   ROW_NUMBER() OVER (PARTITION BY job_id ORDER BY run_date DESC, run_time DESC) as rn
            FROM msdb.dbo.sysjobhistory
            WHERE step_id = 0 -- Job outcome
        ) h ON j.job_id = h.job_id AND h.rn = 1
        LEFT JOIN msdb.dbo.sysjobschedules js ON j.job_id = js.job_id
        LEFT JOIN msdb.dbo.sysschedules s ON js.schedule_id = s.schedule_id
        WHERE 1=1 {enabled_filter}
        ORDER BY j.name;
        """
        
        try:
            # We need to find a valid database to connect to. 
            # If 'master' is not in allowed list, we might fail validation if we enforce it strictly.
            # But DbConnectionService.get_connection takes a db name.
            # Let's try to use the first allowed DB from SecurityConfig if 'master' is not explicitly allowed?
            # Or just rely on the fact that we need *some* connection.
            # For now, I'll try 'master'. If it fails validation, I'll need a fallback.
            # But wait, I can't import SecurityConfig here easily without circular imports maybe?
            # Actually I can.
            
            from config.settings import settings
            allowed_dbs = settings.allowed_databases
            if not allowed_dbs:
                 return {"success": False, "jobs": [], "error": "No allowed databases found to establish connection."}
            
            # Prefer master if allowed, else first one
            conn_db = "master" if "master" in allowed_dbs else allowed_dbs[0]
            
            conn = self.db_service.get_connection(conn_db)
            conn.timeout = timeout_seconds
            cursor = conn.cursor()
            cursor.execute(query)
            
            jobs = []
            if cursor.description:
                for row in cursor.fetchall():
                    # Format dates/times
                    # run_date is YYYYMMDD (int or str), run_time is HHMMSS (int or str)
                    # We need to format them nicely or just return as string.
                    # Output format: "last_run_date?: string".
                    
                    def fmt_datetime(d_str, t_str):
                        if not d_str or d_str == '0': return None
                        # Pad time
                        t_str = t_str.zfill(6) if t_str else "000000"
                        d_str = d_str.strip()
                        if len(d_str) != 8: return d_str # Fallback
                        return f"{d_str[:4]}-{d_str[4:6]}-{d_str[6:]} {t_str[:2]}:{t_str[2:4]}:{t_str[4:]}"

                    last_run = fmt_datetime(str(row.last_run_date_str) if row.last_run_date_str else None, 
                                          str(row.last_run_time_str) if row.last_run_time_str else None)
                    
                    next_run = fmt_datetime(str(row.next_run_date_str) if row.next_run_date_str else None,
                                          str(row.next_run_time_str) if row.next_run_time_str else None)

                    jobs.append({
                        "name": row.name,
                        "enabled": bool(row.enabled),
                        "last_run_outcome": row.last_run_outcome,
                        "last_run_date": last_run,
                        "next_run_date": next_run
                    })

            self.audit_service.log_schema_access(
                database=conn_db,
                scope="agent_jobs",
                success=True,
                user_id=None,
                error=None,
            )
            return {"success": True, "jobs": jobs, "error": None}
            
        except Exception as e:
            # If we failed to connect or query
            return {"success": False, "jobs": [], "error": f"Database error: {str(e)}"}

    def _get_job_history(self, job_name: Optional[str] = None, time_window_minutes: int = DEFAULT_TIME_WINDOW_MINUTES, max_rows: int = DEFAULT_MAX_ROWS, run_status: Optional[int] = None, timeout_seconds: int = DEFAULT_QUERY_TIMEOUT) -> Dict[str, Any]:
        """Internal helper to fetch job history with optional filters."""
        # Validate job name if provided
        if job_name:
            is_valid, error = self._validate_object_name(job_name)
            if not is_valid:
                return {"success": False, "runs": [], "error": error}

        # Build WHERE clause conditions and parameters
        params = []
        conditions = ["h.step_id = 0"]  # Job outcome only
        
        if job_name:
            conditions.append("j.name = ?")
            params.append(job_name)
        
        if run_status is not None:
            conditions.append("h.run_status = ?")
            params.append(run_status)
            
        where_clause = " AND ".join(conditions)
            
        query = f"""
        SELECT TOP {max_rows}
            j.name AS job_name,
            h.run_date,
            h.run_time,
            h.run_duration,
            CASE h.run_status
                WHEN 0 THEN 'Failed'
                WHEN 1 THEN 'Succeeded'
                WHEN 2 THEN 'Retry'
                WHEN 3 THEN 'Canceled'
                ELSE 'Unknown'
            END AS outcome,
            h.message
        FROM msdb.dbo.sysjobhistory h
        JOIN msdb.dbo.sysjobs j ON h.job_id = j.job_id
        WHERE msdb.dbo.agent_datetime(h.run_date, h.run_time) > DATEADD(minute, -{time_window_minutes}, GETDATE())
        AND {where_clause}
        ORDER BY h.run_date DESC, h.run_time DESC;
        """
        
        try:
            conn_db = self._get_system_db_connection()
            
            conn = self.db_service.get_connection(conn_db)
            conn.timeout = timeout_seconds
            cursor = conn.cursor()
            cursor.execute(query, params)
            
            runs = []
            if cursor.description:
                for row in cursor.fetchall():
                    # Format date
                    d_str = str(row.run_date)
                    t_str = str(row.run_time).zfill(6)
                    run_date_str = f"{d_str[:4]}-{d_str[4:6]}-{d_str[6:]} {t_str[:2]}:{t_str[2:4]}:{t_str[4:]}"
                    
                    # Calculate duration in ms
                    # run_duration is HHMMSS int
                    dur = row.run_duration
                    hours = dur // 10000
                    minutes = (dur % 10000) // 100
                    seconds = dur % 100
                    duration_ms = ((hours * 3600) + (minutes * 60) + seconds) * 1000
                    
                    runs.append({
                        "job_name": row.job_name,
                        "run_date": run_date_str,
                        "run_duration_ms": duration_ms,
                        "outcome": row.outcome,
                        "message": row.message
                    })

            self.audit_service.log_schema_access(
                database=conn_db,
                scope="agent_history",
                success=True,
                user_id=None,
                error=None,
            )
            return {"success": True, "runs": runs, "error": None}
            
        except Exception as e:
            return {"success": False, "runs": [], "error": f"Database error: {str(e)}"}

    def get_sql_agent_job_history(self, job_name: Optional[str] = None, time_window_minutes: int = DEFAULT_TIME_WINDOW_MINUTES, max_rows: int = DEFAULT_MAX_ROWS, timeout_seconds: int = DEFAULT_QUERY_TIMEOUT) -> Dict[str, Any]:
        """Return run history for one job or all jobs."""
        return self._get_job_history(job_name, time_window_minutes, max_rows, None, timeout_seconds)

    def get_recent_failed_jobs(self, time_window_minutes: int = DEFAULT_TIME_WINDOW_MINUTES, max_rows: int = DEFAULT_MAX_ROWS, timeout_seconds: int = DEFAULT_QUERY_TIMEOUT) -> Dict[str, Any]:
        """Return only failed job runs in the time window."""
        result = self._get_job_history(None, time_window_minutes, max_rows, 0, timeout_seconds)
        if result["success"]:
            return {"success": True, "failed_runs": result["runs"], "error": None}
        return {"success": False, "failed_runs": [], "error": result["error"]}
