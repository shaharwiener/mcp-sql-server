import contextlib
import pyodbc
import structlog
import time
import threading
from queue import Queue, Empty
from typing import Dict, Any, Optional, Callable
from services.common.exceptions import DatabaseError, ConfigurationError
from config.configuration import get_config

logger = structlog.get_logger()

# Global connection pool storage
# We use a global variable to persist the pool across service instantiations
# (ExecutionService creates a NEW DbConnectionService every time)
_CONNECTION_POOLS: Dict[str, 'SimpleConnectionPool'] = {}
_POOL_LOCK = threading.Lock()

class SimpleConnectionPool:
    """
    A simple thread-safe connection pool for pyodbc connections.
    """
    def __init__(self, key: str, max_size: int = 10, timeout: int = 30):
        self.key = key
        self.max_size = max_size
        self.timeout = timeout
        self.pool = Queue(maxsize=max_size)
        self.current_count = 0
        self.lock = threading.Lock()

    def get_connection(self, connection_factory: Callable[[], pyodbc.Connection]) -> pyodbc.Connection:
        """Get a connection from the pool or create a new one."""
        try:
            # Try to get an idle connection immediately
            conn = self.pool.get_nowait()
            
            # Verify connection is alive
            if self._validate_connection(conn):
                return conn
            else:
                # Connection dead, discard and decrement count to allow replacement
                self._discard_connection(conn)
                
        except Empty:
            pass
            
        # No idle connection, check if we can create new
        can_create = False
        with self.lock:
            if self.current_count < self.max_size:
                self.current_count += 1
                can_create = True
        
        if can_create:
            try:
                return connection_factory()
            except Exception:
                with self.lock:
                    self.current_count -= 1
                raise

        # Pool exhausted, wait for one
        try:
            conn = self.pool.get(timeout=self.timeout)
            if self._validate_connection(conn):
                return conn
            else:
                # If we got a bad connection from pool, we must eventually create a new one
                # but for simplicity, we discard and recurse (or fail)
                self._discard_connection(conn)
                # To avoid infinite loop, just try create one more time even if full provided we decremented
                with self.lock:
                    self.current_count += 1
                return connection_factory()
        except Empty:
            raise DatabaseError(
                "Connection pool exhausted. Please increase pool size or try again later.", 
                details={"timeout": self.timeout, "pool_size": self.max_size}
            )

    def return_connection(self, conn: pyodbc.Connection):
        """Return a connection to the pool."""
        if not self._validate_connection(conn):
            self._discard_connection(conn)
            return

        try:
            # Rollback any uncommitted transaction
            conn.rollback()
            self.pool.put_nowait(conn)
        except:
            # Pool full or other error
            self._discard_connection(conn)

    def _validate_connection(self, conn: pyodbc.Connection) -> bool:
        """Check if connection is healthy."""
        try:
            with contextlib.closing(conn.cursor()) as cursor:
                cursor.execute("SELECT 1")
            return True
        except:
            return False

    def _discard_connection(self, conn: pyodbc.Connection):
        """Close and discard a connection."""
        try:
            conn.close()
        except:
            pass
        with self.lock:
            if self.current_count > 0:
                self.current_count -= 1

def get_pool(key: str, max_size: int, timeout: int) -> SimpleConnectionPool:
    """Get or create a singleton pool for the given key."""
    with _POOL_LOCK:
        if key not in _CONNECTION_POOLS:
            _CONNECTION_POOLS[key] = SimpleConnectionPool(key, max_size, timeout)
        return _CONNECTION_POOLS[key]

# Circuit Breaker State
_CIRCUIT_STATE = {
    "failures": 0,
    "last_failure_time": 0,
    "is_open": False
}
_CIRCUIT_LOCK = threading.Lock()
MAX_FAILURES = 5
RESET_TIMEOUT = 30 # seconds

class DbConnectionService:
    def __init__(self):
        """
        Initialize the database connection service.
        """
        self.config = get_config()

    def _check_circuit_breaker(self):
        with _CIRCUIT_LOCK:
            if _CIRCUIT_STATE["is_open"]:
                if time.time() - _CIRCUIT_STATE["last_failure_time"] > RESET_TIMEOUT:
                    # Half-open: try one request
                    _CIRCUIT_STATE["is_open"] = False
                    _CIRCUIT_STATE["failures"] = 0
                    logger.info("circuit_breaker_reset")
                else:
                    raise DatabaseError("Database is temporarily unavailable (Circuit Breaker Open). Please try again later.")

    def _record_failure(self):
        with _CIRCUIT_LOCK:
            _CIRCUIT_STATE["failures"] += 1
            _CIRCUIT_STATE["last_failure_time"] = time.time()
            if _CIRCUIT_STATE["failures"] >= MAX_FAILURES:
                _CIRCUIT_STATE["is_open"] = True
                logger.error("circuit_breaker_opened", failures=_CIRCUIT_STATE["failures"])

    def _record_success(self):
        with _CIRCUIT_LOCK:
            if _CIRCUIT_STATE["failures"] > 0:
                _CIRCUIT_STATE["failures"] = 0

    def _get_connection_string(self, env: Optional[str] = None, db: Optional[str] = None) -> str:
        """
        Construct connection string from configuration for a specific environment.
        """
        target_env = env or self.config.environment
        
        # Validate environment
        if target_env not in self.config.available_environments:
            raise ConfigurationError(f"Environment '{target_env}' is not configured.")
        
        # Priority 1: Use connection components with builder
        if target_env in self.config.database.connection_components:
            from services.infrastructure.connection_string_builder import ConnectionStringBuilder
            components = self.config.database.connection_components[target_env]
            # For local Docker/development, trust server certificate
            # Check if server is localhost or a Docker service name (no dots or localhost)
            is_local = (components.server in ['localhost', '127.0.0.1', 'sql-server-int'] or 
                       '.' not in components.server or 
                       components.server.startswith('sql-server'))
            builder = ConnectionStringBuilder(
                server=components.server,
                database=components.database,
                username=components.username,
                password=components.password,
                connection_timeout=self.config.database.connection_timeout_seconds,
                application_name=self.config.database.app_name,
                trust_server_certificate=is_local  # Trust cert for local Docker connections
            )
            return builder.build(override_database=db)
        
        # Priority 2: Use legacy connection string
        if target_env in self.config.database.connection_strings:
            conn_str_secret = self.config.database.connection_strings[target_env]
            conn_str = conn_str_secret.get_secret_value()
            if db:
                conn_str += f";Database={db}"
            return conn_str
        
        raise ConfigurationError(f"No connection configuration for environment '{target_env}'.")

    def get_connection(self, env: Optional[str] = None, db: Optional[str] = None) -> pyodbc.Connection:
        """
        Get a connection from the pool (or create new via pool).
        """
        self._check_circuit_breaker()
        
        try:
            conn_str = self._get_connection_string(env, db)
            
            # Get pool for this connection string
            pool = get_pool(
                key=conn_str, 
                max_size=self.config.database.connection_pool_size,
                timeout=self.config.database.connection_timeout_seconds
            )
            
            timeout = self.config.database.connection_timeout_seconds

            def connection_factory():
                # Factory to create NEW connection if pool needs one
                try:
                    conn = pyodbc.connect(conn_str, timeout=timeout)
                    
                    # Apply Mandatory Safety Settings
                    with contextlib.closing(conn.cursor()) as cursor:
                        cursor.execute("SET NOCOUNT ON")
                        cursor.execute("SET XACT_ABORT ON")
                        cursor.execute(f"SET LOCK_TIMEOUT {timeout * 1000}") 
                        cursor.execute("SET DEADLOCK_PRIORITY LOW")
                        cursor.execute("SET TRANSACTION ISOLATION LEVEL READ COMMITTED")
                        cursor.execute("SET ARITHABORT ON")
                    return conn
                except Exception as e:
                    self._record_failure()
                    raise e

            conn = pool.get_connection(connection_factory)
            self._record_success() # Reset failures if we got a good connection
            return conn
            
        except pyodbc.Error as e:
            # Note: We record failure inside factory for creation, but if pool retrieval fails generically?
            # Actually simpler: if get_connection_string fails, that's config error, not DB down.
            # If connect() fails, that's recorded.
            raise DatabaseError(f"Failed to connect to database: {str(e)}")

    def execute_query(self,
                      query: str,
                      env: Optional[str] = None,
                      db: Optional[str] = None,
                      fetch_method: Optional[Callable] = None,
                      command_timeout: Optional[int] = None) -> Optional[Any]:
        """
        Execute a SQL Server query with strict safety and timeout controls.
        """
        # Load config limits
        cmd_timeout = command_timeout or self.config.database.command_timeout_seconds
        max_timeout = self.config.database.max_command_timeout_seconds
        
        if cmd_timeout > max_timeout:
            logger.warning(f"Requested timeout {cmd_timeout}s exceeds max {max_timeout}s. Capping.", query_snippet=query[:50])
            cmd_timeout = max_timeout
            
        conn = None
        conn_str = "" # Need key to return to pool
        pool = None
        
        try:
            # We need the connection string/key to identify the pool
            # get_connection handles finding the pool, but we need to know WHICH pool to return to.
            # So we re-resolve the string to find the pool instance.
            conn_str = self._get_connection_string(env, db)
            pool = get_pool(conn_str, self.config.database.connection_pool_size, self.config.database.connection_timeout_seconds)
            
            # Get connection
            conn = self.get_connection(env, db) # This calls pool.get_connection internally
            
            # Set Command Timeout
            conn.timeout = cmd_timeout
            
            with contextlib.closing(conn.cursor()) as cursor:
                if hasattr(cursor, 'timeout'):
                    cursor.timeout = cmd_timeout
                
                # Dynamic LOCK_TIMEOUT for this specific query if needed
                # cursor.execute(f"SET LOCK_TIMEOUT {cmd_timeout * 1000}")
                
                start_time = time.time()
                cursor.execute(query)
                
                if cursor.description:
                    result = (fetch_method or self._default_fetch)(cursor, conn)
                else:
                    conn.commit()
                    result = None
                    
                duration = time.time() - start_time
                if duration > 1.0:
                    logger.info("slow_query", duration=duration, query_snippet=query[:100])
                    
                return result

        except pyodbc.Error as e:
            raise DatabaseError(f"Database error: {str(e)}", details={"query": query[:200], "database": db})
        except Exception as e:
            raise DatabaseError(f"Unexpected error: {str(e)}", details={"query": query[:200]})
        finally:
            if conn and pool:
                pool.return_connection(conn)

    @staticmethod
    def _default_fetch(cursor, connection):
        if hasattr(cursor, 'fetchall'):
            return cursor.fetchall()
        return None
