from __future__ import annotations

"""Convenience wrappers for establishing MySQL connections."""

from typing import Any

from .connections import (
    build_mysql_url,
    get_engine,
    get_connection,
    get_mysql_connection,
)

Engine = Any
Connection = Any

__all__ = [
    "build_mysql_url",
    "get_engine",
    "get_connection",
    "get_mysql_connection",
]
