"""Simplified secure importer base class."""

from __future__ import annotations

import logging
from typing import Any

from etl.base_importer import BaseDBImporter
from utils.etl_helpers import load_sql, run_sql_script
from utils.sql_security import validate_sql_statement

logger = logging.getLogger(__name__)


class SecureBaseDBImporter(BaseDBImporter):
    """Base importer that validates SQL before execution."""

    def run_sql_file(self, conn: Any, name: str, filename: str) -> None:
        """Load a SQL file, validate it and execute using :func:`run_sql_script`."""
        sql = load_sql(filename, self.db_name)
        sql = validate_sql_statement(sql, allow_ddl=True)
        run_sql_script(conn, name, sql, timeout=self.config["sql_timeout"])
