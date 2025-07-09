"""Secure configuration management with encrypted secrets."""

from __future__ import annotations

import os
import json
import logging
from pathlib import Path
from typing import Optional, Dict, Any, List

from pydantic import BaseSettings, SecretStr, validator, Field
from pydantic_settings import BaseSettings
import keyring
from cryptography.fernet import Fernet

logger = logging.getLogger(__name__)


class SecretManager:
    """Manages encrypted secrets using system keyring."""
    
    def __init__(self, service_name: str = "etl_supervision_importer"):
        self.service_name = service_name
    
    def store_secret(self, key: str, value: str) -> None:
        """Store encrypted secret in system keyring."""
        try:
            keyring.set_password(self.service_name, key, value)
            logger.info(f"Secret {key} stored successfully")
        except Exception as e:
            raise RuntimeError(f"Failed to store secret {key}: {e}")
    
    def get_secret(self, key: str) -> Optional[str]:
        """Retrieve secret from system keyring."""
        try:
            secret = keyring.get_password(self.service_name, key)
            if secret:
                logger.debug(f"Secret {key} retrieved successfully")
            return secret
        except Exception as e:
            logger.warning(f"Failed to retrieve secret {key}: {e}")
            return None
    
    def delete_secret(self, key: str) -> None:
        """Remove secret from keyring."""
        try:
            keyring.delete_password(self.service_name, key)
            logger.info(f"Secret {key} deleted successfully")
        except Exception as e:
            logger.warning(f"Failed to delete secret {key}: {e}")


class SecureETLSettings(BaseSettings):
    """Enhanced settings with security and validation."""
    
    # Database settings
    mssql_target_conn_str: Optional[SecretStr] = Field(default=None, env="MSSQL_TARGET_CONN_STR")
    mssql_target_db_name: Optional[str] = Field(default=None, env="MSSQL_TARGET_DB_NAME")
    
    # File paths - using proper validation
    ej_csv_dir: Path = Field(..., env="EJ_CSV_DIR")
    ej_log_dir: Path = Field(default_factory=lambda: Path.cwd() / "logs", env="EJ_LOG_DIR")
    
    # Performance settings with reasonable limits
    sql_timeout: int = Field(default=300, ge=30, le=7200, env="SQL_TIMEOUT")
    csv_chunk_size: int = Field(default=50000, ge=1000, le=1000000, env="CSV_CHUNK_SIZE")
    db_pool_size: int = Field(default=5, ge=1, le=20, env="DB_POOL_SIZE")
    db_max_overflow: int = Field(default=10, ge=0, le=50, env="DB_MAX_OVERFLOW")
    db_pool_timeout: int = Field(default=30, ge=5, le=300, env="DB_POOL_TIMEOUT")
    
    # Feature flags
    include_empty_tables: bool = Field(default=False, env="INCLUDE_EMPTY_TABLES")
    skip_pk_creation: bool = Field(default=False, env="SKIP_PK_CREATION")
    
    # Security settings
    max_retry_attempts: int = Field(default=3, ge=1, le=10, env="MAX_RETRY_ATTEMPTS")
    connection_timeout: int = Field(default=30, ge=5, le=120, env="CONNECTION_TIMEOUT")
    
    # Override tables (loaded from secure config)
    always_include_tables: List[str] = Field(default_factory=list)
    
    class Config:
        env_file = ".env"
        env_file_encoding = "utf-8"
        case_sensitive = False
        # Prevent extra fields for security
        extra = "forbid"
        # Validate assignment
        validate_assignment = True
    
    @validator("mssql_target_conn_str")
    def validate_connection_string(cls, v: Optional[SecretStr]) -> Optional[SecretStr]:
        """Validate connection string format."""
        if not v:
            return v
        
        conn_str = v.get_secret_value().upper()
        required_parts = ["DRIVER", "SERVER"]
        
        for part in required_parts:
            if f"{part}=" not in conn_str:
                raise ValueError(f"Connection string missing required part: {part}")
        
        # Check for potentially dangerous elements
        dangerous_keywords = ["OPENROWSET", "OPENQUERY", "xp_", "sp_cmdshell"]
        for keyword in dangerous_keywords:
            if keyword.upper() in conn_str:
                raise ValueError(f"Connection string contains dangerous keyword: {keyword}")
        
        return v
    
    @validator("ej_csv_dir")
    def validate_csv_dir(cls, v: Path) -> Path:
        """Ensure CSV directory exists and is accessible."""
        if not v.exists():
            raise ValueError(f"CSV directory does not exist: {v}")
        if not v.is_dir():
            raise ValueError(f"CSV path is not a directory: {v}")
        if not os.access(v, os.R_OK):
            raise ValueError(f"CSV directory is not readable: {v}")
        return v
    
    @validator("ej_log_dir")
    def ensure_log_dir(cls, v: Path) -> Path:
        """Ensure log directory exists."""
        v.mkdir(parents=True, exist_ok=True)
        if not os.access(v, os.W_OK):
            raise ValueError(f"Log directory is not writable: {v}")
        return v


class ConfigurationManager:
    """Centralized configuration management with security."""
    
    def __init__(self, config_path: Optional[Path] = None):
        self.config_path = config_path or Path("config/secure_config.json")
        self.secret_manager = SecretManager()
        self._settings: Optional[SecureETLSettings] = None
    
    def load_settings(self) -> SecureETLSettings:
        """Load settings with secret injection."""
        if self._settings is None:
            # Load environment overrides
            env_vars = self._load_environment_variables()
            
            # Load non-secret config from file
            file_config = self._load_file_config()
            
            # Merge configurations (env takes precedence)
            merged_config = {**file_config, **env_vars}
            
            # Inject secrets if not provided via environment
            if not merged_config.get("MSSQL_TARGET_CONN_STR"):
                conn_str = self.secret_manager.get_secret("mssql_connection")
                if conn_str:
                    merged_config["MSSQL_TARGET_CONN_STR"] = conn_str
            
            # Create validated settings
            self._settings = SecureETLSettings(**merged_config)
        
        return self._settings
    
    def save_connection_string(self, connection_string: str) -> None:
        """Securely store connection string."""
        self.secret_manager.store_secret("mssql_connection", connection_string)
    
    def save_non_secret_config(self, config: Dict[str, Any]) -> None:
        """Save non-secret configuration to file."""
        # Filter out sensitive data
        safe_config = {
            k: v for k, v in config.items()
            if not self._is_sensitive_key(k)
        }
        
        # Ensure config directory exists
        self.config_path.parent.mkdir(parents=True, exist_ok=True)
        
        with open(self.config_path, 'w') as f:
            json.dump(safe_config, f, indent=2, default=str)
        
        logger.info(f"Configuration saved to {self.config_path}")
    
    def migrate_legacy_config(self, legacy_config_path: Path) -> None:
        """Migrate from legacy plaintext configuration."""
        if not legacy_config_path.exists():
            logger.info("No legacy config found, skipping migration")
            return
        
        with open(legacy_config_path) as f:
            legacy_config = json.load(f)
        
        # Extract and store sensitive data securely
        if all(key in legacy_config for key in ["driver", "server", "database", "user", "password"]):
            conn_str = (
                f"DRIVER={legacy_config['driver']};"
                f"SERVER={legacy_config['server']};"
                f"DATABASE={legacy_config['database']};"
                f"UID={legacy_config['user']};"
                f"PWD={legacy_config['password']}"
            )
            self.save_connection_string(conn_str)
            
            # Store database name separately if needed
            if "database" in legacy_config:
                os.environ["MSSQL_TARGET_DB_NAME"] = legacy_config["database"]
        
        # Save non-sensitive configuration
        safe_config = {
            k: v for k, v in legacy_config.items()
            if k not in ["driver", "server", "database", "user", "password"]
        }
        
        self.save_non_secret_config(safe_config)
        
        logger.info("Legacy configuration migrated successfully")
        logger.warning(f"Please review and remove legacy config file: {legacy_config_path}")
    
    def _load_environment_variables(self) -> Dict[str, Any]:
        """Load configuration from environment variables."""
        return {
            k: v for k, v in os.environ.items()
            if k.startswith(("MSSQL_", "EJ_", "SQL_", "CSV_", "DB_", "INCLUDE_", "FAIL_", "SKIP_", "MAX_", "CONNECTION_"))
        }
    
    def _load_file_config(self) -> Dict[str, Any]:
        """Load non-secret configuration from file."""
        if not self.config_path.exists():
            return {}
        
        try:
            with open(self.config_path) as f:
                return json.load(f)
        except Exception as e:
            logger.error(f"Failed to load config file {self.config_path}: {e}")
            return {}
    
    def _is_sensitive_key(self, key: str) -> bool:
        """Check if a configuration key contains sensitive data."""
        sensitive_patterns = [
            "password", "pwd", "pass", "secret", "key", "token",
            "connection", "conn_str", "user", "uid", "username"
        ]
        key_lower = key.lower()
        return any(pattern in key_lower for pattern in sensitive_patterns)


# Factory function for easy access
def get_secure_settings() -> SecureETLSettings:
    """Get validated settings with secrets injected."""
    config_manager = ConfigurationManager()
    return config_manager.load_settings()


# Migration utility
def migrate_existing_configuration():
    """Utility function to migrate existing configuration."""
    config_manager = ConfigurationManager()
    legacy_config_path = Path("config/values.json")
    
    if legacy_config_path.exists():
        print("Found legacy configuration file. Migrating to secure storage...")
        config_manager.migrate_legacy_config(legacy_config_path)
        print("Migration completed!")
        print(f"Legacy file at {legacy_config_path} can be safely deleted after verification.")
    else:
        print("No legacy configuration found.")


if __name__ == "__main__":
    # Run migration if called directly
    migrate_existing_configuration()