from __future__ import annotations

import os
import urllib.parse
from typing import Any

from dotenv import load_dotenv
import sqlalchemy
from sqlalchemy.engine import URL
from config import settings

Engine = Any  # runtime fallback for type hints
Connection = Any

# Ensure environment variables from .env are loaded like previous modules
load_dotenv()

_engines: dict[str, Engine] = {}


def build_mssql_url(conn_str: str) -> URL:
    """Return an SQLAlchemy URL for an ODBC connection string."""
    encoded = urllib.parse.quote_plus(conn_str)
    return URL.create("mssql+pyodbc", query={"odbc_connect": encoded})


def build_mysql_url(
    host: str,
    user: str,
    password: str,
    database: str,
    port: int = 3306,
) -> URL:
    """Return a MySQL connection URL using mysqlconnector."""
    return URL.create(
        "mysql+mysqlconnector",
        username=user,
        password=password,
        host=host,
        port=port,
        database=database,
    )


def get_engine(url: URL | str) -> Engine:
    """Return (and cache) a SQLAlchemy engine for ``url``."""
    key = str(url)
    engine = _engines.get(key)
    if engine is None:
        engine = sqlalchemy.create_engine(
            url,
            pool_size=settings.db_pool_size,
            max_overflow=settings.db_max_overflow,
            pool_timeout=settings.db_pool_timeout,
            pool_pre_ping=True,
        )
        _engines[key] = engine
    return engine


def get_connection(url: URL | str) -> Connection:
    """Return a pooled connection for ``url``."""
    return get_engine(url).connect()


def get_mssql_connection(conn_str: str) -> Connection:
    """Return a connection using an ODBC connection string."""
    return get_connection(build_mssql_url(conn_str))


def get_source_connection() -> Connection:
    """Connect to the configured MSSQL source database."""
    conn = settings.mssql_source_conn_str
    return get_mssql_connection(conn.get_secret_value() if conn else "")


def get_target_connection() -> Connection:
    """Connect to the configured MSSQL target database."""
    conn = settings.mssql_target_conn_str
    return get_mssql_connection(conn.get_secret_value())


def get_mysql_connection(
    host: str | None = None,
    user: str | None = None,
    password: str | None = None,
    database: str | None = None,
    port: int | None = None,
) -> Connection:
    """Return a pooled MySQL connection using provided args or configuration."""
    host = host or os.getenv("MYSQL_HOST") or settings.mysql_host
    user = user or os.getenv("MYSQL_USER") or settings.mysql_user
    env_pass = os.getenv("MYSQL_PASSWORD")
    settings_pass = (
        settings.mysql_password.get_secret_value() if settings.mysql_password else None
    )
    password = password or env_pass or settings_pass
    database = database or os.getenv("MYSQL_DATABASE") or settings.mysql_database
    port_value = port or os.getenv("MYSQL_PORT") or settings.mysql_port or 3306
    port = int(port_value)

    if not all([host, user, password, database]):
        raise ValueError("Missing required MySQL connection parameters.")

    return get_connection(build_mysql_url(host, user, password, database, port))


__all__ = [
    "build_mssql_url",
    "build_mysql_url",
    "get_engine",
    "get_connection",
    "get_mssql_connection",
    "get_source_connection",
    "get_target_connection",
    "get_mysql_connection",
]
