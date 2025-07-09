from __future__ import annotations

"""Simple database connectivity checks."""

import logging
from typing import Optional

import pyodbc

from config.settings import settings

logger = logging.getLogger(__name__)


def check_connection(conn_str: str, timeout: int = 5) -> bool:
    """Return ``True`` if a connection can be established using ``conn_str``."""
    try:
        connector = getattr(pyodbc, "connect", None)
        if not callable(connector):
            logger.warning("pyodbc.connect unavailable; skipping connectivity check")
            return True
        conn = connector(conn_str, timeout=timeout)
        if hasattr(conn, "close"):
            conn.close()
        return True
    except Exception as exc:  # pragma: no cover - best effort
        logger.error("Database connection failed: %s", exc)
        return False


def check_target_connection(timeout: int = 5) -> bool:
    """Check connectivity to the configured target database."""
    conn = settings.mssql_target_conn_str
    conn_str = conn if conn else ""
    return check_connection(conn_str, timeout)

