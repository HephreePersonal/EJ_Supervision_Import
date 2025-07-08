from __future__ import annotations

"""Convenience wrappers for establishing MSSQL connections."""

from typing import Any

from .connections import (
    build_mssql_url,
    get_engine,
    get_connection,
    get_mssql_connection,
    get_source_connection,
    get_target_connection,
)

Engine = Any  # runtime fallback for type hints
Connection = Any

__all__ = [
    "build_mssql_url",
    "get_engine",
    "get_connection",
    "get_mssql_connection",
    "get_source_connection",
    "get_target_connection",
]
