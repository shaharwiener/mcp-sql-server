"""
Configuration Management Module.
Loads configuration from config.yaml and allows overrides via environment variables.
"""
import os
import yaml
from typing import List, Dict, Any, Optional
from pathlib import Path
from pydantic import BaseModel, Field, SecretStr
import structlog

logger = structlog.get_logger()

# --- Configuration Models ---

class ServerConfig(BaseModel):
    host: str = "0.0.0.0"
    port: int = 9303
    transport: str = "stdio"
    log_level: str = "INFO"

class DatabaseConnectionComponents(BaseModel):
    """Individual connection components for a database."""
    server: str
    database: str
    username: str
    password: SecretStr

class DatabaseConfig(BaseModel):
    connection_pool_size: int = 10
    connection_timeout_seconds: int = 30
    command_timeout_seconds: int = 60
    max_command_timeout_seconds: int = 300
    app_name: str = "MCP-SQLServer"
    
    # Map of EnvName -> Connection Components
    # e.g. {"Int": DatabaseConnectionComponents(...)}
    connection_components: Dict[str, DatabaseConnectionComponents] = Field(default_factory=dict)
    
    # Legacy: Map of EnvName -> Full Connection String (for backward compatibility)
    connection_strings: Dict[str, SecretStr] = Field(default_factory=dict)
    
    # Legacy/Default fallback (optional)
    connection_string: Optional[SecretStr] = None


class RiskWeights(BaseModel):
    no_where_clause: int = 100
    cross_join: int = 80
    wildcard_select: int = 20
    dynamic_sql: int = 90
    ddl_statement: int = 100
    table_scan: int = 60
    missing_index: int = 40

class EnvironmentSafetyOverride(BaseModel):
    """Environment-specific safety overrides."""
    max_rows: Optional[int] = None
    max_execution_time_seconds: Optional[int] = None
    query_cost_threshold: Optional[float] = None
    enable_nolock_hint: bool = False
    require_top_clause: bool = False
    enable_resource_hints: bool = False
    maxdop: Optional[int] = None
    max_grant_percent: Optional[int] = None

class SafetyConfig(BaseModel):
    max_rows: int = 1000
    max_payload_size_mb: int = 1
    max_execution_time_seconds: int = 60
    allow_linked_servers: bool = False
    allowed_databases: List[str] = []
    
    # Environment-specific overrides
    environment_overrides: Dict[str, EnvironmentSafetyOverride] = Field(default_factory=dict)
    
    # Concurrency limits
    max_concurrent_queries: int = 5
    max_concurrent_queries_per_user: int = 2
    
    # Query cost protection
    enable_cost_check: bool = True
    max_query_cost: float = 50.0
    
    # Resource control hints (CPU and memory)
    enable_resource_hints: bool = True
    maxdop: int = 1  # Default: single-threaded
    max_grant_percent: int = 10  # Default: 10% of available memory
    
    risk_weights: RiskWeights = Field(default_factory=RiskWeights)
    
    def get_env_setting(self, env: str, setting: str, default: Any = None) -> Any:
        """Get environment-specific setting with fallback to global default."""
        if env in self.environment_overrides:
            override_value = getattr(self.environment_overrides[env], setting, None)
            if override_value is not None:
                return override_value
        return getattr(self, setting, default)

class BestPracticesConfig(BaseModel):
    enforce_schema_prefix: bool = True
    enforce_no_select_star: bool = True
    enforce_parameterization: bool = True

class LoggingConfig(BaseModel):
    metrics_enabled: bool = True

class McpConfig(BaseModel):
    environment: str = "Int"
    available_environments: List[str] = ["Int", "Stg", "Prd"]
    server: ServerConfig = Field(default_factory=ServerConfig)
    database: DatabaseConfig = Field(default_factory=DatabaseConfig)
    safety: SafetyConfig = Field(default_factory=SafetyConfig)
    best_practices: BestPracticesConfig = Field(default_factory=BestPracticesConfig)
    logging: LoggingConfig = Field(default_factory=LoggingConfig)

# --- Loader Logic ---

class ConfigLoader:
    _instance: Optional[McpConfig] = None

    @classmethod
    def load(cls, config_path: Optional[str] = None) -> McpConfig:
        """
        Load configuration from YAML and override with Environment Variables.
        Singleton pattern to avoid reloading.
        """
        if cls._instance:
            return cls._instance

        # 1. Determine Config Path
        if not config_path:
            config_path = os.getenv("MCP_CONFIG_PATH", "config/config.yaml")

        path = Path(config_path)
        if not path.is_absolute():
            # Assume relative to current working directory or script location
            # Simple fallback: use cwd
            path = Path.cwd() / config_path

        # 2. Load YAML
        config_data = {}
        if path.exists():
            try:
                with open(path, "r") as f:
                    config_data = yaml.safe_load(f) or {}
            except Exception as e:
                logger.error("config_load_error", error=str(e), path=str(path))
                raise RuntimeError(f"Failed to load config file at {path}: {e}")
        else:
            logger.warning("config_file_not_found", path=str(path))

        # 3. Environment Variable Overrides
        # We allow specific env vars to seamlessly inject secrets
        
        # We construct the model instance first to validate types
        try:
            config = McpConfig(**config_data)
            
            # Override transport from environment variable if provided
            mcp_transport = os.getenv("MCP_TRANSPORT")
            if mcp_transport:
                config.server.transport = mcp_transport
                logger.info(f"Transport overridden from environment: {mcp_transport}")
            
            # Inject database connection components
            # Priority 1: Load individual components (new format)
            # DB_SERVER_INT, DB_DATABASE_INT, DB_USERNAME_INT, DB_PASSWORD_INT
            for env in config.available_environments:
                env_upper = env.upper()
                server = os.getenv(f"DB_SERVER_{env_upper}")
                database = os.getenv(f"DB_DATABASE_{env_upper}")
                username = os.getenv(f"DB_USERNAME_{env_upper}")
                password = os.getenv(f"DB_PASSWORD_{env_upper}")
                
                if all([server, database, username, password]):
                    # All components present - use new format
                    config.database.connection_components[env] = DatabaseConnectionComponents(
                        server=server,
                        database=database,
                        username=username,
                        password=SecretStr(password)
                    )
                    logger.info(f"Loaded connection components for {env} environment")
            
            # Priority 2: Load full connection strings (legacy format - backward compatibility)
            # DB_CONNECTION_STRING_INT, DB_CONNECTION_STRING_STG, etc.
            for env in config.available_environments:
                if env in config.database.connection_components:
                    # Skip if we already have components
                    continue
                    
                env_var_name = f"DB_CONNECTION_STRING_{env.upper()}"
                conn_val = os.getenv(env_var_name)
                if conn_val:
                    config.database.connection_strings[env] = SecretStr(conn_val)
                    logger.info(f"Loaded legacy connection string for {env} environment")

            # Priority 3: Legacy/Simple Fallback: DB_CONNECTION_STRING -> maps to config.environment
            default_conn = os.getenv("DB_CONNECTION_STRING")
            if default_conn:
                if (config.environment not in config.database.connection_components and 
                    config.environment not in config.database.connection_strings):
                    config.database.connection_strings[config.environment] = SecretStr(default_conn)
                    logger.info(f"Loaded default connection string for {config.environment} environment")
            
            if not config.database.connection_components and not config.database.connection_strings:
                logger.warning("No database connection configuration found. Database features will fail.")
                
            cls._instance = config
            return config
            
        except Exception as e:
            logger.error("config_validation_error", error=str(e))
            raise ValueError(f"Invalid Configuration: {e}")


def get_config() -> McpConfig:
    return ConfigLoader.load()
