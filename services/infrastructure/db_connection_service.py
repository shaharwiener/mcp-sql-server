import contextlib
from typing import Dict, Any, Optional, Callable
import pyodbc
import structlog
from services.common.exceptions import DatabaseError, ConfigurationError
from config.settings import settings

logger = structlog.get_logger()


class DatabaseType:
    """Enum-like class for database types"""
    SQL_SERVER = 'sqlserver'


class DbConnectionService:
    def __init__(self, connection_strings: Optional[Dict[str, Any]] = None):
        """
        Initialize the database connection service.

        Args:
            connection_strings: Dictionary of connection strings. 
            Defaults to retrieving from ConfigurationService if not provided.
        """
        self.connection_strings = connection_strings or self._get_default_connection_strings()

    def _get_default_connection_strings(self) -> Dict[str, Any]:
        """
        Retrieve default connection strings from Settings.
        
        Returns:
            Dictionary of SQL Server connection strings (lowercase keys).
        """
        return settings.connection_strings


    def _get_connection_string(self, db: str) -> Any:
        """
        Retrieve connection string for a specific database (case-insensitive lookup).

        Args:
            db: Database identifier.

        Returns:
            Connection string or configuration.

        Raises:
            ConfigurationError: If connection string is not found or not properly initialized.
        """
        if not isinstance(self.connection_strings, dict):
            raise ConfigurationError("Connection strings not properly initialized")
        
        # Case-insensitive lookup - convert to lowercase
        db_lower = db.lower()
        conn_str = self.connection_strings.get(db_lower)
        
        if not conn_str:
            raise ConfigurationError(f"No connection string found for database: {db}")
            
        return conn_str

    def get_connection(self, db: str, connection_timeout: int = None) -> pyodbc.Connection:
        """
        Get a raw pyodbc connection object.
        Useful for operations requiring session state (e.g., SHOWPLAN).
        
        Args:
            db: Database identifier.
            connection_timeout: Timeout in seconds for connection.
            
        Returns:
            pyodbc.Connection object.
        """
        connection_string = self._get_connection_string(db)
        timeout = connection_timeout or settings.DEFAULT_CONNECTION_TIMEOUT
        return pyodbc.connect(connection_string, timeout=timeout)

    def execute_query(self,
                      query: str,
                      db: str,
                      fetch_method: Optional[Callable] = None,
                      connection_timeout: int = None,
                      command_timeout: int = None) -> Optional[Any]:
        """
        Execute a SQL Server query with configurable timeouts.

        Args:
            query: SQL query to execute.
            db: Database identifier.
            fetch_method: Optional callable to handle result fetching.
            connection_timeout: Connection timeout in seconds (default: settings.DEFAULT_CONNECTION_TIMEOUT).
            command_timeout: Query execution timeout in seconds (default: settings.DEFAULT_COMMAND_TIMEOUT).

        Returns:
            Query results or None.
        """
        # Validate database exists
        self._get_connection_string(db)
        
        conn_timeout = connection_timeout or settings.DEFAULT_CONNECTION_TIMEOUT
        cmd_timeout = command_timeout or settings.DEFAULT_COMMAND_TIMEOUT
            
        # logger.debug("executing_query", database=db, connection_timeout=conn_timeout, command_timeout=cmd_timeout)
        return self._execute_sqlserver_query(query, db, fetch_method or self._default_fetch, conn_timeout, cmd_timeout)

    @staticmethod
    def _default_fetch(cursor, connection):
        """
        Default fetch method if none is provided.

        Args:
            cursor: Database cursor.
            connection: Database connection.

        Returns:
            Fetched results or None.
        """
        if hasattr(cursor, 'fetchall'):
            results = cursor.fetchall()
            return results if results else []
        return None

    def _execute_sqlserver_query(self, query: str, db: str, fetch_method: Callable, connection_timeout: int, command_timeout: int) -> Optional[Any]:
        """
        Execute a query on SQL Server with timeout controls.
        
        Args:
            query: SQL query to execute
            db: Database identifier  
            fetch_method: Method to fetch results
            connection_timeout: Connection timeout in seconds
            command_timeout: Query execution timeout in seconds
            
        Returns:
            Query results or None
        """
        connection_string = self._get_connection_string(db)
        
        # Add connection timeout to connection string if not already present
        if 'timeout' not in connection_string.lower():
            connection_string += f";timeout={connection_timeout}"
        
        try:
            with contextlib.closing(pyodbc.connect(connection_string)) as connection:
                # Set command timeout for the connection
                connection.timeout = command_timeout
                
                with contextlib.closing(connection.cursor()) as cursor:
                    cursor.execute(query)
                    if cursor.description:
                        result = fetch_method(cursor, connection)
                    else:
                        connection.commit()
                        result = None
                    return result
        except pyodbc.Error as e:
            raise DatabaseError(f"Database error: {str(e)}", details={"query": query, "database": db})
        except Exception as e:
            raise DatabaseError(f"Unexpected error executing query: {str(e)}", details={"query": query, "database": db})

    # SECURITY WARNING: execute_stored_procedure is DISABLED
    # This method is commented out because stored procedures can execute UPDATE, DELETE, INSERT,
    # and other destructive operations, which violates the read-only principle of this MCP server.
    # Only uncomment if you have proper validation and authentication in place.
    
    # def execute_stored_procedure(self, procedure: str, db: Optional[str] = None, connection_timeout: int = None, command_timeout: int = None) -> Optional[Any]:
    #     """
    #     Execute a stored procedure on SQL Server with timeout controls.
    #     
    #     WARNING: DISABLED FOR SECURITY - Stored procedures can perform write operations.
    #     
    #     Args:
    #         procedure: Stored procedure name and parameters
    #         db: Database identifier (required if not provided, uses first available)
    #         connection_timeout: Connection timeout in seconds (default: settings.DEFAULT_CONNECTION_TIMEOUT)
    #         command_timeout: Query execution timeout in seconds (default: settings.DEFAULT_COMMAND_TIMEOUT)
    #         
    #     Returns:
    #         Stored procedure results or None
    #     """
    #     # If db not provided, use first available database
    #     if db is None:
    #         if not self.connection_strings or not isinstance(self.connection_strings, dict):
    #             raise ConfigurationError("No connection strings available to determine default database.")
    #         db = list(self.connection_strings.keys())[0]
    #     
    #     def fetch(cursor, connection):
    #         try:
    #             results = cursor.fetchall()
    #             connection.commit()
    #             return results
    #         except pyodbc.ProgrammingError as e:
    #             # If there are no results, it might raise a ProgrammingError.
    #             # We commit and then re-raise to ensure the transaction is saved
    #             # while still signaling that something unexpected happened.
    #             connection.commit()
    #             logger.warning("stored_procedure_no_result", error=str(e))
    #             raise
    #
    #     conn_timeout = connection_timeout or settings.DEFAULT_CONNECTION_TIMEOUT
    #     cmd_timeout = command_timeout or settings.DEFAULT_COMMAND_TIMEOUT
    #
    #     return self.execute_query(procedure, db, fetch, conn_timeout, cmd_timeout)
