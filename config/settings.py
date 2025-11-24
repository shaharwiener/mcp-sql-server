"""
Centralized configuration management using pydantic-settings.
Replaces ConfigLoader, SecurityConfig, and ConfigurationService.
"""
import os
import json
from typing import List, Optional, Dict, Any, Set
from pydantic import Field, SecretStr, computed_field
from pydantic_settings import BaseSettings, SettingsConfigDict

class Settings(BaseSettings):
    """
    Application settings loaded from environment variables and .env files.
    """
    model_config = SettingsConfigDict(
        env_file=('.env', '.env.local'),  # Load both .env and .env.local
        env_file_encoding='utf-8',
        extra='ignore',
        case_sensitive=False
    )

    # --- Environment ---w
    PANGO_ENV: str = Field("Int", description="Deployment environment (Int, Stg, Prd)")
    LOG_LEVEL: str = Field("INFO", description="Logging level")
    
    # --- MCP Server ---
    MCP_TRANSPORT: str = Field("stdio", description="MCP transport mode (stdio, sse)")
    MCP_HOST: str = Field("127.0.0.1", description="Host to bind to")
    MCP_PORT: int = Field(9303, description="Port to bind to")
    
    # --- Database Configuration ---
    # Primary method: DB_NAME and DB_CONNECTION_STRING
    DB_NAME: Optional[str] = Field(None, description="Default database name")
    DB_CONNECTION_STRING: Optional[SecretStr] = Field(None, description="Default database connection string")
    
    # Legacy/Multi-DB support: Dictionary of connection strings
    # Loaded dynamically from env vars starting with DB_CONN_
    
    # --- Security & Limits ---
    MAX_QUERY_LENGTH: int = Field(10000, description="Maximum allowed SQL query length")
    MAX_RESULT_ROWS: int = Field(10000, description="Maximum rows to return")
    MAX_TABLE_NAME_LENGTH: int = Field(128, description="Maximum table name length")
    
    # --- Timeouts ---
    DEFAULT_CONNECTION_TIMEOUT: int = Field(30, description="Default connection timeout in seconds")
    DEFAULT_COMMAND_TIMEOUT: int = Field(300, description="Default query execution timeout in seconds")
    MAX_COMMAND_TIMEOUT: int = Field(600, description="Maximum allowed command timeout")
    
    # --- Audit ---
    AUDIT_LOG_ENABLED: bool = Field(True, description="Enable audit logging")
    AUDIT_LOG_PATH: str = Field("./logs/audit/", description="Path to store audit logs")
    
    # --- AWS / SSM (Optional) ---
    AWS_REGION: str = Field("us-east-1", description="AWS Region")
    SSM_PARAMETER_NAME: Optional[str] = Field(None, description="SSM Parameter name to load config from")
    
    # --- Validation Patterns ---
    TABLE_NAME_PATTERN: str = Field(r'^[a-zA-Z0-9_]+$', description="Regex pattern for valid table names")
    
    DANGEROUS_SQL_PATTERNS: List[str] = Field([
        r'\bxp_cmdshell\b',
        r'\bsp_executesql\b',
        r'\bOPENROWSET\b',
        r'\bOPENDATASOURCE\b',
        r'\bOPENQUERY\b',
        r'\bINTO\s+OUTFILE\b',
        r'\bLOAD_FILE\b',
        r'--\s*$',
        r'/\*.*\*/',
    ], description="Regex patterns for dangerous SQL")
    
    ALLOWED_SQL_KEYWORDS: Set[str] = Field({
        'SELECT', 'FROM', 'WHERE', 'JOIN', 'INNER', 'LEFT', 'RIGHT', 
        'OUTER', 'ON', 'AS', 'AND', 'OR', 'NOT', 'IN', 'LIKE', 'BETWEEN',
        'ORDER', 'BY', 'GROUP', 'HAVING', 'COUNT', 'SUM', 'AVG', 'MAX', 
        'MIN', 'DISTINCT', 'TOP', 'LIMIT', 'OFFSET', 'ASC', 'DESC',
        'CAST', 'CONVERT', 'CASE', 'WHEN', 'THEN', 'ELSE', 'END',
        'IS', 'NULL', 'EXISTS', 'ANY', 'ALL', 'UNION'
    }, description="Allowed SQL keywords for validation")
    
    
    # --- Compatibility Methods (for legacy code expecting method calls) ---
    def get_audit_log_enabled(self) -> bool:
        """Get audit log enabled status (for compatibility)."""
        return self.AUDIT_LOG_ENABLED
    
    def get_audit_log_path(self) -> str:
        """Get audit log path (for compatibility)."""
        return self.AUDIT_LOG_PATH
    
    def get_dangerous_sql_patterns(self) -> List[str]:
        """Get dangerous SQL patterns (for compatibility)."""
        return self.DANGEROUS_SQL_PATTERNS
    
    def get_allowed_sql_keywords(self) -> Set[str]:
        """Get allowed SQL keywords (for compatibility)."""
        return self.ALLOWED_SQL_KEYWORDS
    
    def get_allowed_databases(self) -> List[str]:
        """Get list of allowed databases (for compatibility)."""
        return self.allowed_databases
    
    def get_max_query_length(self) -> int:
        """Get maximum query length (for compatibility)."""
        return self.MAX_QUERY_LENGTH
    
    def get_max_table_name_length(self) -> int:
        """Get maximum table name length (for compatibility)."""
        return self.MAX_TABLE_NAME_LENGTH
    
    def is_database_allowed(self, database: str) -> bool:
        """
        Check if a database is in the allowed list (for compatibility).
        
        If allowed_databases is empty, allow all databases (for testing/development).
        """
        if not self.allowed_databases:
            return True  # Empty list means allow all databases
        return database.lower() in [db.lower() for db in self.allowed_databases]


    @computed_field
    @property
    def connection_strings(self) -> Dict[str, str]:
        """
        Aggregates connection strings from various sources:
        1. DB_NAME + DB_CONNECTION_STRING
        2. DB_CONN_* environment variables
        3. SSM Parameter (if configured)
        """
        conns = {}
        
        # 1. Primary Env Var
        if self.DB_NAME and self.DB_CONNECTION_STRING:
            conns[self.DB_NAME.lower()] = self.DB_CONNECTION_STRING.get_secret_value()
            
        # 2. Legacy DB_CONN_* Env Vars
        for key, value in os.environ.items():
            if key.startswith('DB_CONN_'):
                db_name = key.replace('DB_CONN_', '').lower()
                if db_name not in conns:
                    conns[db_name] = value
                    
        # 3. SSM (if configured)
        if self.SSM_PARAMETER_NAME:
            try:
                import boto3
                ssm = boto3.client('ssm', region_name=self.AWS_REGION)
                response = ssm.get_parameter(
                    Name=self.SSM_PARAMETER_NAME,
                    WithDecryption=True
                )
                ssm_values = json.loads(response['Parameter']['Value'])
                for db, conn in ssm_values.items():
                    if db.lower() not in conns:
                        conns[db.lower()] = conn
            except Exception as e:
                # Log warning but don't crash if SSM fails (unless it's the only source)
                print(f"Warning: Failed to load SSM config: {e}")
                
        return conns

    @computed_field
    @property
    def allowed_databases(self) -> List[str]:
        """Returns list of available database names."""
        return list(self.connection_strings.keys())

# Global settings instance
settings = Settings()
