from __future__ import annotations

import os
from pathlib import Path
from typing import Optional

from dotenv import load_dotenv

from pydantic_settings import BaseSettings
from pydantic import DirectoryPath, Field, SecretStr, validator


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
