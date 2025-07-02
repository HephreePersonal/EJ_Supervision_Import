"""Convenience wrappers for establishing MSSQL connections."""

from __future__ import annotations

import urllib.parse
import sqlalchemy
from sqlalchemy.pool import NullPool
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from sqlalchemy.engine import Engine, Connection

Engine = Any  # runtime fallback for type hints
Connection = Any

from config import settings

_engines: dict[str, Engine] = {}

def _get_engine(conn_str: str) -> Engine:
    """Return (and cache) a SQLAlchemy engine for the given connection string."""
    conn_str += ";connection timeout=30;command timeout=30"
    engine = _engines.get(conn_str)
    if engine is None:
        params = urllib.parse.quote_plus(conn_str)
        url = f"mssql+pyodbc:///?odbc_connect={params}"
        engine = sqlalchemy.create_engine(
            url,
            echo=False,  # Set to True for debugging
            pool_pre_ping=True,
            pool_size=5,
            max_overflow=10,
            pool_timeout=settings.db_pool_timeout,
        )
        _engines[conn_str] = engine
    return engine


def get_mssql_connection(conn_str: str) -> Connection:
    """Return a pooled connection using the given connection string."""
    return _get_engine(conn_str).connect()

def get_source_connection() -> Connection:
    """Connect to the source MSSQL database configured in settings."""
    conn = settings.mssql_source_conn_str
    return get_mssql_connection(conn.get_secret_value() if conn else "")

def get_target_connection() -> Connection:
    """Connect to the target MSSQL database configured in settings."""
    conn = settings.mssql_target_conn_str
    return get_mssql_connection(conn.get_secret_value())

# Use explicit metadata loading instead of reflection
metadata = sqlalchemy.MetaData()
# Only load tables you need explicitly
# table = Table('your_table', metadata, autoload_with=engine)

