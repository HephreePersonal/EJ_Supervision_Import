from __future__ import annotations

import json
import logging
import os
from pathlib import Path
from typing import Dict, Any, Optional, Union
from functools import lru_cache
from dotenv import load_dotenv
from pydantic import field_validator, ConfigDict, Field, SecretStr, DirectoryPath
from pydantic_settings import BaseSettings

logger = logging.getLogger(__name__)

# Load environment variables from a .env file if present.
load_dotenv()

def parse_database_name(conn_str: str | None) -> Optional[str]:
    """Extract the database name from an ODBC connection string."""
    if not conn_str:
        return None
    for part in conn_str.split(";"):
        if part.lower().startswith("database="):
            return part.split("=", 1)[1]
    return None

class ETLConstants:
    """Default values used across the ETL pipeline."""
    DEFAULT_SQL_TIMEOUT = 300
    DEFAULT_BULK_INSERT_BATCH_SIZE = 100
    MAX_RETRY_ATTEMPTS = 3
    CONNECTION_TIMEOUT = 30
    DEFAULT_CSV_CHUNK_SIZE = 50000

class Settings(BaseSettings):
    """Application configuration."""

    model_config = ConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=False,
        extra="allow"
    )
    
    # Database connection fields
    driver: Optional[str] = None
    server: Optional[str] = None
    database: Optional[str] = None
    user: Optional[str] = None
    password: Optional[str] = None
    
    # File paths
    ej_csv_dir: Optional[str] = None
    ej_log_dir: Optional[str] = None
    
    # Feature flags
    include_empty_tables: bool = False
    skip_pk_creation: bool = False
    always_include_tables: list[str] = Field(default_factory=list)
    
    # Performance settings
    sql_timeout: int = Field(default=ETLConstants.DEFAULT_SQL_TIMEOUT)
    csv_chunk_size: int = Field(default=ETLConstants.DEFAULT_CSV_CHUNK_SIZE)
    max_retry_attempts: int = Field(default=ETLConstants.MAX_RETRY_ATTEMPTS)
    connection_timeout: int = Field(default=ETLConstants.CONNECTION_TIMEOUT)
    
    @property
    def mssql_target_conn_str(self) -> Optional[str]:
        """Build connection string from components."""
        if not all([self.driver, self.server, self.database, self.user]):
            return None
        
        conn_str = f"DRIVER={self.driver};SERVER={self.server};DATABASE={self.database};UID={self.user}"
        if self.password:
            conn_str += f";PWD={self.password}"
        return conn_str
    
    @property
    def mssql_target_db_name(self) -> Optional[str]:
        """Get the database name."""
        return self.database

def load_config_from_file(config_path: str = "config/secure_config.json") -> Dict[str, Any]:
    """Load configuration from JSON file."""
    path = Path(config_path)
    if not path.exists():
        return {}
    
    try:
        with open(path, 'r') as f:
            return json.load(f)
    except Exception as e:
        logger.error(f"Error loading config file {path}: {e}")
        return {}

def save_config_to_file(config: Dict[str, Any], config_path: str = "config/secure_config.json") -> None:
    """Save configuration to JSON file."""
    path = Path(config_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    
    try:
        with open(path, 'w') as f:
            json.dump(config, f, indent=2)
        logger.info(f"Configuration saved to {path}")
    except Exception as e:
        logger.error(f"Error saving config file {path}: {e}")

def get_settings() -> Settings:
    """Get settings loaded from config file."""
    config_data = load_config_from_file()
    return Settings(**config_data)

def save_settings(settings_obj: Settings) -> None:
    """Save settings to config file."""
    # Convert settings to dict, excluding None values
    config_data = {}
    for field_name, field_value in settings_obj.model_dump().items():
        if field_value is not None:
            config_data[field_name] = field_value
    
    save_config_to_file(config_data)

# For backward compatibility
def get_secure_settings() -> Settings:
    """Alias for get_settings."""
    return get_settings()

# Legacy aliases
SecretManager = None
ConfigurationManager = None

def migrate_existing_configuration():
    """No-op for backward compatibility."""
    pass

# Create settings instance
settings = get_settings()