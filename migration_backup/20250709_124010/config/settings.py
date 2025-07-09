from __future__ import annotations

import json
import logging
import os
from pathlib import Path
from typing import Dict, Any, Optional

from dotenv import load_dotenv

from pydantic_settings import BaseSettings
from pydantic import DirectoryPath, Field, SecretStr, validator
import keyring

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

    #: Default timeout for SQL statements in seconds
    DEFAULT_SQL_TIMEOUT = 300

    #: Default number of rows to insert per batch when doing bulk inserts
    DEFAULT_BULK_INSERT_BATCH_SIZE = 100

    #: Maximum number of retry attempts for transient failures
    MAX_RETRY_ATTEMPTS = 3

    #: Default connection timeout when establishing database connections
    CONNECTION_TIMEOUT = 30

    #: Default number of rows per chunk when reading large CSV files
    DEFAULT_CSV_CHUNK_SIZE = 50000


class Settings(BaseSettings):
    """Application configuration loaded from environment variables."""

    mssql_source_conn_str: Optional[SecretStr] = Field(default=None, env="MSSQL_SOURCE_CONN_STR")
    mssql_target_conn_str: SecretStr = Field(..., env="MSSQL_TARGET_CONN_STR")
    mssql_target_db_name: Optional[str] = Field(default=None, env="MSSQL_TARGET_DB_NAME")

    ej_csv_dir: DirectoryPath = Field(..., env="EJ_CSV_DIR")
    ej_log_dir: Path = Field(default_factory=Path.cwd, env="EJ_LOG_DIR")

    sql_timeout: int = Field(ETLConstants.DEFAULT_SQL_TIMEOUT, env="SQL_TIMEOUT")
    include_empty_tables: bool = Field(False, env="INCLUDE_EMPTY_TABLES")
    csv_chunk_size: int = Field(ETLConstants.DEFAULT_CSV_CHUNK_SIZE, env="CSV_CHUNK_SIZE")

    max_retry_attempts: int = Field(ETLConstants.MAX_RETRY_ATTEMPTS, env="MAX_RETRY_ATTEMPTS")
    connection_timeout: int = Field(ETLConstants.CONNECTION_TIMEOUT, env="CONNECTION_TIMEOUT")
    csv_dir: Optional[str] = Field(default=None, env="CSV_DIR")  # Legacy field
    always_include_tables: list[str] = Field(default_factory=list, env="ALWAYS_INCLUDE_TABLES")


    mysql_host: Optional[str] = Field(default=None, env="MYSQL_HOST")
    mysql_user: Optional[str] = Field(default=None, env="MYSQL_USER")
    mysql_password: Optional[SecretStr] = Field(default=None, env="MYSQL_PASSWORD")
    mysql_database: Optional[str] = Field(default=None, env="MYSQL_DATABASE")
    mysql_port: int = Field(3306, env="MYSQL_PORT")

    db_pool_size: int = Field(5, env="DB_POOL_SIZE")
    db_max_overflow: int = Field(10, env="DB_MAX_OVERFLOW")
    db_pool_timeout: int = Field(30, env="DB_POOL_TIMEOUT")

    @validator("mssql_target_conn_str")
    def _require_target_conn_str(cls, v: SecretStr) -> SecretStr:
        if not v or not v.get_secret_value():
            raise ValueError("MSSQL_TARGET_CONN_STR is required")
        return v

    @validator("mssql_target_db_name", always=True)
    def _derive_db_name(cls, v: Optional[str], values: dict) -> Optional[str]:
        conn = values.get("mssql_target_conn_str")
        secret = conn.get_secret_value() if isinstance(conn, SecretStr) else conn
        return v or parse_database_name(secret)

    @validator("sql_timeout")
    def _check_timeout(cls, v: int) -> int:
        if v <= 0:
            raise ValueError("SQL_TIMEOUT must be positive")
        return v

    @validator("csv_chunk_size")
    def _check_chunk_size(cls, v: int) -> int:
        if v <= 0:
            raise ValueError("CSV_CHUNK_SIZE must be positive")
        return v

    @validator("db_pool_size")
    def _check_pool_size(cls, v: int) -> int:
        if v <= 0:
            raise ValueError("DB_POOL_SIZE must be positive")
        return v

    @validator("db_max_overflow")
    def _check_max_overflow(cls, v: int) -> int:
        if v < 0:
            raise ValueError("DB_MAX_OVERFLOW cannot be negative")
        return v

    @validator("db_pool_timeout")
    def _check_pool_timeout(cls, v: int) -> int:
        if v <= 0:
            raise ValueError("DB_POOL_TIMEOUT must be positive")
        return v

    class Config:
        env_file = ".env"
        env_file_encoding = "utf-8"
        case_sensitive = False
        extra = "allow"  # This will allow additional fields that aren't explicitly defined

    @property
    def mysql_conn_dict(self) -> dict[str, Optional[str]]:
        """Return MySQL connection parameters as a dictionary."""
        password = self.mysql_password.get_secret_value() if self.mysql_password else None
        if not all([self.mysql_host, self.mysql_user, password, self.mysql_database]):
            return {}
        return {
            "host": self.mysql_host,
            "user": self.mysql_user,
            "password": password,
            "database": self.mysql_database,
            "port": self.mysql_port,
        }


# Instantiate once at import time
settings = Settings()


class SecretManager:
    """Manage secrets using the system keyring."""

    def __init__(self, service_name: str = "etl_supervision_importer"):
        self.service_name = service_name

    def store_secret(self, key: str, value: str) -> None:
        try:
            keyring.set_password(self.service_name, key, value)
            logger.info(f"Secret {key} stored successfully")
        except Exception as exc:  # pragma: no cover - system keyring may vary
            raise RuntimeError(f"Failed to store secret {key}: {exc}") from exc

    def get_secret(self, key: str) -> Optional[str]:
        try:
            secret = keyring.get_password(self.service_name, key)
            if secret:
                logger.debug(f"Secret {key} retrieved successfully")
            return secret
        except Exception as exc:  # pragma: no cover - system keyring may vary
            logger.warning(f"Failed to retrieve secret {key}: {exc}")
            return None

    def delete_secret(self, key: str) -> None:
        try:
            keyring.delete_password(self.service_name, key)
            logger.info(f"Secret {key} deleted successfully")
        except Exception as exc:  # pragma: no cover - system keyring may vary
            logger.warning(f"Failed to delete secret {key}: {exc}")


class ConfigurationManager:
    """Load and store configuration with optional secure helpers."""

    def __init__(self, config_path: Optional[Path] = None):
        self.config_path = config_path or Path("config/secure_config.json")
        self.secret_manager = SecretManager()
        self._settings: Optional[Settings] = None

    def load_settings(self) -> Settings:
        if self._settings is None:
            env_vars = self._load_environment_variables()
            file_config = self._load_file_config()
            merged_config = {**file_config, **env_vars}

            if not merged_config.get("MSSQL_TARGET_CONN_STR"):
                conn_str = self.secret_manager.get_secret("mssql_connection")
                if conn_str:
                    merged_config["MSSQL_TARGET_CONN_STR"] = conn_str

            self._settings = Settings(**merged_config)

        return self._settings

    def save_connection_string(self, connection_string: str) -> None:
        self.secret_manager.store_secret("mssql_connection", connection_string)

    def save_non_secret_config(self, config: Dict[str, Any]) -> None:
        safe_config = {k: v for k, v in config.items() if not self._is_sensitive_key(k)}
        self.config_path.parent.mkdir(parents=True, exist_ok=True)
        with open(self.config_path, "w") as fh:
            json.dump(safe_config, fh, indent=2, default=str)
        logger.info(f"Configuration saved to {self.config_path}")

    def migrate_legacy_config(self, legacy_config_path: Path) -> None:
        if not legacy_config_path.exists():
            logger.info("No legacy config found, skipping migration")
            return

        with open(legacy_config_path) as fh:
            legacy_config = json.load(fh)

        if all(k in legacy_config for k in ["driver", "server", "database", "user", "password"]):
            conn_str = (
                f"DRIVER={legacy_config['driver']};"
                f"SERVER={legacy_config['server']};"
                f"DATABASE={legacy_config['database']};"
                f"UID={legacy_config['user']};"
                f"PWD={legacy_config['password']}"
            )
            self.save_connection_string(conn_str)
            if "database" in legacy_config:
                os.environ["MSSQL_TARGET_DB_NAME"] = legacy_config["database"]

        safe_config = {k: v for k, v in legacy_config.items() if k not in ["driver", "server", "database", "user", "password"]}
        self.save_non_secret_config(safe_config)

        logger.info("Legacy configuration migrated successfully")
        logger.warning(f"Please review and remove legacy config file: {legacy_config_path}")

    def _load_environment_variables(self) -> Dict[str, Any]:
        return {k: v for k, v in os.environ.items() if k.startswith(("MSSQL_", "EJ_", "SQL_", "CSV_", "DB_", "INCLUDE_", "FAIL_", "SKIP_", "MAX_", "CONNECTION_"))}

    def _load_file_config(self) -> Dict[str, Any]:
        if not self.config_path.exists():
            return {}
        try:
            with open(self.config_path) as fh:
                return json.load(fh)
        except Exception as exc:
            logger.error(f"Failed to load config file {self.config_path}: {exc}")
            return {}

    @staticmethod
    def _is_sensitive_key(key: str) -> bool:
        sensitive_patterns = [
            "password",
            "pwd",
            "pass",
            "secret",
            "key",
            "token",
            "connection",
            "conn_str",
            "user",
            "uid",
            "username",
        ]
        key_lower = key.lower()
        return any(pattern in key_lower for pattern in sensitive_patterns)


def get_settings() -> Settings:
    """Return settings loaded with optional secure helpers."""
    manager = ConfigurationManager()
    return manager.load_settings()


# Backwards compatibility
get_secure_settings = get_settings


def migrate_existing_configuration() -> None:
    """Migrate legacy plaintext configuration to the secure format."""
    config_manager = ConfigurationManager()
    legacy_config_path = Path("config/secure_config.json")
    if legacy_config_path.exists():
        print("Found legacy configuration file. Migrating to secure storage...")
        config_manager.migrate_legacy_config(legacy_config_path)
        print("Migration completed!")
        print(
            f"Legacy file at {legacy_config_path} can be safely deleted after verification."
        )
    else:
        print("No legacy configuration found.")
