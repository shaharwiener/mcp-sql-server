"""
Validation Service for MCP SQL Server
Centralized input validation and sanitization
"""
import re
from typing import Dict, Any, Optional, Tuple
from config.settings import settings


class ValidationService:
    """Service for validating and sanitizing inputs."""
    
    def __init__(self):
        """Initialize validation service with security configuration."""
        self.config = settings
        
        # Compile regex patterns for performance
        self.table_name_pattern = re.compile(self.config.TABLE_NAME_PATTERN)
        self.dangerous_patterns = [
            re.compile(pattern, re.IGNORECASE) 
            for pattern in self.config.get_dangerous_sql_patterns()
        ]
    
    def validate_query_length(self, query: str) -> Tuple[bool, Optional[str]]:
        """
        Validate query length is within limits.
        
        Args:
            query: SQL query string
            
        Returns:
            Tuple of (is_valid, error_message)
        """
        if not query:
            return False, "Query cannot be empty"
        
        if len(query) > self.config.get_max_query_length():
            return False, f"Query exceeds maximum length of {self.config.get_max_query_length()} characters"
        
        return True, None
    
    def validate_database_name(self, database: str) -> Tuple[bool, Optional[str]]:
        """
        Validate database name is in allowed list.
        
        Args:
            database: Database name to validate
            
        Returns:
            Tuple of (is_valid, error_message)
        """
        if not database:
            return False, "Database name cannot be empty"
        
        if not self.config.is_database_allowed(database):
            allowed = self.config.get_allowed_databases()
            return False, f"Database '{database}' is not in allowed list: {allowed}"
        
        return True, None
    
    def validate_table_name(self, table_name: str) -> Tuple[bool, Optional[str]]:
        """
        Validate table name contains only safe characters.
        
        Args:
            table_name: Table name to validate
            
        Returns:
            Tuple of (is_valid, error_message)
        """
        if not table_name:
            return False, "Table name cannot be empty"
        
        if len(table_name) > self.config.get_max_table_name_length():
            return False, f"Table name exceeds maximum length of {self.config.get_max_table_name_length()}"
        
        if not self.table_name_pattern.match(table_name):
            return False, "Table name contains invalid characters. Only alphanumeric and underscore allowed"
        
        return True, None
    
    def sanitize_table_name(self, table_name: str) -> str:
        """
        Sanitize table name using SQL Server bracket notation.
        
        Args:
            table_name: Table name to sanitize
            
        Returns:
            Sanitized table name with brackets
        """
        # Validate first
        is_valid, error = self.validate_table_name(table_name)
        if not is_valid:
            raise ValueError(error)
        
        # Escape any existing brackets and wrap in brackets
        escaped = table_name.replace('[', '[[').replace(']', ']]')
        return f"[{escaped}]"
    
    def check_dangerous_patterns(self, query: str) -> Tuple[bool, Optional[str]]:
        """
        Check query for dangerous SQL patterns.
        
        Args:
            query: SQL query to check
            
        Returns:
            Tuple of (is_safe, error_message)
        """
        for pattern in self.dangerous_patterns:
            if pattern.search(query):
                return False, f"Query contains potentially dangerous pattern: {pattern.pattern}"
        
        return True, None
    
    def validate_query_structure(self, query: str) -> Tuple[bool, Optional[str]]:
        """
        Validate overall query structure for safety.
        
        Args:
            query: SQL query to validate
            
        Returns:
            Tuple of (is_valid, error_message)
        """
        # Check for multiple statements (semicolons in suspicious places)
        stripped_query = query.strip()
        semicolon_count = stripped_query.count(';')
        
        # Allow one semicolon at the end, but be suspicious of more
        if semicolon_count > 1:
            return False, "Query contains multiple statements (multiple semicolons)"
        
        if semicolon_count == 1 and not stripped_query.endswith(';'):
            return False, "Semicolon found in middle of query (potential SQL injection)"
        
        # Check for null bytes (potential encoding attack)
        if '\x00' in query:
            return False, "Query contains null bytes"
        
        # Check for suspicious character sequences
        if '\\x' in query or '\\u' in query:
            return False, "Query contains encoded characters"
        
        return True, None
    
    def validate_query_comprehensive(self, query: str) -> Tuple[bool, Optional[str]]:
        """
        Perform comprehensive query validation.
        
        Args:
            query: SQL query to validate
            
        Returns:
            Tuple of (is_valid, error_message)
        """
        # Length check
        is_valid, error = self.validate_query_length(query)
        if not is_valid:
            return False, error
        
        # Structure check
        is_valid, error = self.validate_query_structure(query)
        if not is_valid:
            return False, error
        
        # Dangerous patterns check
        is_safe, error = self.check_dangerous_patterns(query)
        if not is_safe:
            return False, error
        
        return True, None
    
    def sanitize_for_logging(self, query: str, max_length: int = 500) -> str:
        """
        Sanitize query for safe logging (remove potential PII/sensitive data).
        
        Args:
            query: SQL query to sanitize
            max_length: Maximum length for logged query
            
        Returns:
            Sanitized query string
        """
        # Truncate if too long
        sanitized = query[:max_length]
        if len(query) > max_length:
            sanitized += "... [truncated]"
        
        # Replace potential sensitive values (strings in quotes)
        sanitized = re.sub(r"'[^']*'", "'***'", sanitized)
        
        # Replace phone numbers pattern
        sanitized = re.sub(r'\+?\d{10,}', '***PHONE***', sanitized)
        
        # Replace email patterns
        sanitized = re.sub(r'\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,}\b', '***EMAIL***', sanitized)
        
        return sanitized
    
    def mask_connection_string(self, connection_string: str) -> str:
        """
        Mask sensitive parts of connection string.
        
        Args:
            connection_string: Connection string to mask
            
        Returns:
            Masked connection string
        """
        # Mask password
        masked = re.sub(r'(PWD|Password)=([^;]+)', r'\1=***', connection_string, flags=re.IGNORECASE)
        
        # Mask UID if present
        masked = re.sub(r'(UID|User ID)=([^;]+)', r'\1=***', masked, flags=re.IGNORECASE)
        
        return masked

