"""Base service class for SQL Review services.

Provides common functionality and initialization patterns for all SQL Review services.
"""

from typing import Dict, Any, Optional

from services.infrastructure.db_connection_service import DbConnectionService
from services.infrastructure.validation_service import ValidationService
from services.infrastructure.audit_service import AuditService
from config.settings import settings
from .constants import DEFAULT_SYSTEM_DB


class BaseSQLReviewService:
    """Base class for SQL Review services with common functionality.
    
    All SQL Review services should inherit from this class to ensure
    consistent initialization and access to common utilities.
    """
    
    def __init__(self, connection_strings: Optional[Dict[str, Any]] = None):
        """Initialize the service with database connections and utilities.
        
        Args:
            connection_strings: Optional connection string overrides
        """
        self.db_service = DbConnectionService(connection_strings)
        self.validation_service = ValidationService()
        self.audit_service = AuditService()
    
    def _get_system_db_connection(self) -> str:
        """Get the name of a system database to connect to.
        
        Returns 'master' if available, otherwise the first allowed database.
        This is useful for queries that need to access system-wide information
        like SQL Agent jobs, security changes, etc.
        
        Returns:
            Database name to connect to
            
        Raises:
            ValueError: If no allowed databases are configured
        """
        allowed_dbs = settings.allowed_databases
        if not allowed_dbs:
            raise ValueError("No allowed databases found to establish connection.")
        
        return DEFAULT_SYSTEM_DB if DEFAULT_SYSTEM_DB in allowed_dbs else allowed_dbs[0]
    
    def _validate_database(self, database: str) -> tuple[bool, Optional[str]]:
        """Validate a database name.
        
        Args:
            database: Database name to validate
            
        Returns:
            Tuple of (is_valid, error_message)
        """
        return self.validation_service.validate_database_name(database)
    
    def _validate_query_length(self, query: str) -> tuple[bool, Optional[str]]:
        """Validate query length.
        
        Args:
            query: SQL query to validate
            
        Returns:
            Tuple of (is_valid, error_message)
        """
        return self.validation_service.validate_query_length(query)
    
    def _validate_object_name(self, name: str) -> tuple[bool, Optional[str]]:
        """Validate an object name (table, view, etc.).
        
        Args:
            name: Object name to validate
            
        Returns:
            Tuple of (is_valid, error_message)
        """
        return self.validation_service.validate_table_name(name)
