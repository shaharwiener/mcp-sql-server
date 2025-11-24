"""
Audit Service for MCP SQL Server
Provides query audit logging with user tracking
"""
import os
import json
import logging
from datetime import datetime
from typing import Dict, Any, Optional
from pathlib import Path
from config.settings import settings
from .validation_service import ValidationService


class AuditService:
    """Service for auditing database queries and operations."""
    
    def __init__(self):
        """Initialize audit service with logging configuration."""
        self.config = settings
        self.validation_service = ValidationService()
        self.enabled = self.config.get_audit_log_enabled()
        
        if self.enabled:
            self._setup_audit_logging()
    
    def _setup_audit_logging(self):
        """Setup audit logging to file."""
        try:
            # Create audit log directory if it doesn't exist
            log_path = Path(self.config.get_audit_log_path())
            log_path.mkdir(parents=True, exist_ok=True)
        except (OSError, PermissionError) as e:
            # If we can't create the log directory (e.g., read-only filesystem in Claude Desktop),
            # disable audit logging gracefully
            print(f"Warning: Could not create audit log directory: {e}. Audit logging disabled.")
            self.enabled = False
            return
        
        # Setup dedicated audit logger
        self.audit_logger = logging.getLogger('mcp_sql_audit')
        self.audit_logger.setLevel(logging.INFO)
        
        # Remove existing handlers to avoid duplicates
        self.audit_logger.handlers = []
        
        # Create file handler with daily rotation naming
        log_file = log_path / f"audit_{datetime.now().strftime('%Y%m%d')}.log"
        file_handler = logging.FileHandler(log_file, encoding='utf-8')
        file_handler.setLevel(logging.INFO)
        
        # Create formatter for structured logging
        formatter = logging.Formatter(
            '%(asctime)s | %(levelname)s | %(message)s',
            datefmt='%Y-%m-%d %H:%M:%S'
        )
        file_handler.setFormatter(formatter)
        
        self.audit_logger.addHandler(file_handler)
        
        # Don't propagate to root logger
        self.audit_logger.propagate = False
    
    def log_query(self, 
                  database: str,
                  query: str,
                  success: bool,
                  row_count: int = 0,
                  user_id: Optional[str] = None,
                  error: Optional[str] = None,
                  execution_time_ms: Optional[float] = None) -> None:
        """
        Log a query execution event.
        
        Args:
            database: Database that was queried
            query: SQL query that was executed (will be sanitized)
            success: Whether query succeeded
            row_count: Number of rows returned
            user_id: User who executed the query
            error: Error message if query failed
            execution_time_ms: Query execution time in milliseconds
        """
        if not self.enabled:
            return
        
        try:
            # Sanitize query for logging
            sanitized_query = self.validation_service.sanitize_for_logging(query)
            
            # Build audit record
            audit_record = {
                "timestamp": datetime.now().isoformat(),
                "user_id": user_id or "anonymous",
                "database": database,
                "query": sanitized_query,
                "success": success,
                "row_count": row_count,
                "execution_time_ms": execution_time_ms
            }
            
            if error:
                # Sanitize error message
                audit_record["error"] = self._sanitize_error_message(error)
            
            # Log as JSON for easy parsing
            self.audit_logger.info(json.dumps(audit_record))
            
        except Exception as e:
            # Don't fail the operation if audit logging fails
            print(f"Audit logging failed: {str(e)}")
    
    def log_schema_access(self,
                         database: str,
                         scope: str,
                         success: bool,
                         user_id: Optional[str] = None,
                         error: Optional[str] = None) -> None:
        """
        Log a schema access event.
        
        Args:
            database: Database that was accessed
            scope: Schema scope that was requested
            success: Whether access succeeded
            user_id: User who requested schema
            error: Error message if access failed
        """
        if not self.enabled:
            return
        
        try:
            audit_record = {
                "timestamp": datetime.now().isoformat(),
                "user_id": user_id or "anonymous",
                "operation": "schema_access",
                "database": database,
                "scope": scope,
                "success": success
            }
            
            if error:
                audit_record["error"] = self._sanitize_error_message(error)
            
            self.audit_logger.info(json.dumps(audit_record))
            
        except Exception as e:
            print(f"Audit logging failed: {str(e)}")
    
    def log_authentication_attempt(self,
                                   user_id: Optional[str],
                                   success: bool,
                                   reason: Optional[str] = None) -> None:
        """
        Log an authentication attempt.
        
        Args:
            user_id: User attempting authentication
            success: Whether authentication succeeded
            reason: Reason for failure if unsuccessful
        """
        if not self.enabled:
            return
        
        try:
            audit_record = {
                "timestamp": datetime.now().isoformat(),
                "operation": "authentication",
                "user_id": user_id or "unknown",
                "success": success
            }
            
            if reason:
                audit_record["reason"] = reason
            
            self.audit_logger.info(json.dumps(audit_record))
            
        except Exception as e:
            print(f"Audit logging failed: {str(e)}")
    
    def _sanitize_error_message(self, error: str) -> str:
        """
        Sanitize error message to remove sensitive information.
        
        Args:
            error: Error message to sanitize
            
        Returns:
            Sanitized error message
        """
        # Remove potential connection strings
        sanitized = self.validation_service.mask_connection_string(error)
        
        # Remove file paths
        sanitized = re.sub(r'[A-Z]:\\[^\s]+', '***PATH***', sanitized)
        sanitized = re.sub(r'/[^\s]+/[^\s]+', '***PATH***', sanitized)
        
        return sanitized
    
    def get_audit_summary(self, days: int = 1) -> Dict[str, Any]:
        """
        Get audit summary for the last N days.
        
        Args:
            days: Number of days to summarize
            
        Returns:
            Dictionary with audit statistics
        """
        if not self.enabled:
            return {"enabled": False, "message": "Audit logging is disabled"}
        
        # This is a basic implementation
        # For production, you might want to use a database or log aggregation service
        return {
            "enabled": True,
            "log_path": self.config.get_audit_log_path(),
            "message": f"Check audit logs in {self.config.get_audit_log_path()}"
        }


# Add missing import
import re

