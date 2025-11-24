"""
Custom exceptions for the MCP SQL Server application.
"""

class MCPError(Exception):
    """Base exception for all MCP errors."""
    def __init__(self, message: str, details: dict = None):
        super().__init__(message)
        self.details = details or {}

class ConfigurationError(MCPError):
    """Raised when there is a configuration issue."""
    pass

class DatabaseError(MCPError):
    """Raised when a database operation fails."""
    pass

class ValidationError(MCPError):
    """Raised when input validation fails."""
    pass

class SecurityError(MCPError):
    """Raised when a security check fails."""
    pass
