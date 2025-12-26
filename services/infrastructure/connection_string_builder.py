"""
Connection String Builder for SQL Server.
Builds secure ODBC connection strings from individual components.
"""
from typing import Optional
from pydantic import SecretStr


class ConnectionStringBuilder:
    """Builds SQL Server ODBC connection strings with security defaults."""
    
    def __init__(
        self,
        server: str,
        database: str,
        username: str,
        password: SecretStr,
        driver: str = "ODBC Driver 18 for SQL Server",
        encrypt: bool = True,
        trust_server_certificate: bool = False,
        connection_timeout: int = 30,
        application_name: str = "MCP-SQLServer"
    ):
        """
        Initialize connection string builder.
        
        Args:
            server: SQL Server hostname or IP
            database: Database name
            username: SQL Server username
            password: SQL Server password (SecretStr)
            driver: ODBC driver name
            encrypt: Enable encryption (default: True)
            trust_server_certificate: Trust server certificate (default: False)
            connection_timeout: Connection timeout in seconds (default: 30)
            application_name: Application name for monitoring (default: MCP-SQLServer)
        """
        self.server = server
        self.database = database
        self.username = username
        self.password = password
        self.driver = driver
        self.encrypt = encrypt
        self.trust_server_certificate = trust_server_certificate
        self.connection_timeout = connection_timeout
        self.application_name = application_name
    
    def build(self, override_database: Optional[str] = None) -> str:
        """
        Build ODBC connection string with security defaults.
        
        Args:
            override_database: Optional database to override default
            
        Returns:
            Complete ODBC connection string
        """
        db = override_database or self.database
        
        # Build connection string with security settings
        parts = [
            f"Driver={{{self.driver}}}",
            f"Server={self.server}",
            f"Database={db}",
            f"Uid={self.username}",
            f"Pwd={self.password.get_secret_value()}",
            f"Encrypt={'yes' if self.encrypt else 'no'}",
            f"TrustServerCertificate={'yes' if self.trust_server_certificate else 'no'}",
            f"Connection Timeout={self.connection_timeout}",
            f"APP={self.application_name}",
            # Additional security settings
            "ApplicationIntent=ReadOnly",  # Read-only intent
            "MultiSubnetFailover=Yes",     # Better failover support
        ]
        
        return ";".join(parts) + ";"
    
    @classmethod
    def from_env_vars(
        cls,
        server_var: str,
        database_var: str,
        username_var: str,
        password_var: str
    ) -> "ConnectionStringBuilder":
        """
        Create builder from environment variable values.
        
        Args:
            server_var: Server hostname/IP from env var
            database_var: Database name from env var
            username_var: Username from env var
            password_var: Password from env var
            
        Returns:
            ConnectionStringBuilder instance
        """
        return cls(
            server=server_var,
            database=database_var,
            username=username_var,
            password=SecretStr(password_var)
        )
