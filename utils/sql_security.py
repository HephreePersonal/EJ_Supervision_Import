import logging
import re
from typing import Optional

logger = logging.getLogger(__name__)

_IDENTIFIER_RE = re.compile(r'^[A-Za-z_][A-Za-z0-9_]*$')
_DANGEROUS = {"DROP", "DELETE", "TRUNCATE", "ALTER", "EXEC", "EXECUTE"}


def validate_sql_identifier(identifier: str) -> str:
    """Return ``identifier`` if it matches a basic SQL identifier pattern."""
    if not isinstance(identifier, str) or not _IDENTIFIER_RE.match(identifier):
        raise ValueError(f"Invalid SQL identifier '{identifier}'")
    return identifier


def validate_table_name(schema: str, table: str, database: Optional[str] = None) -> str:
    """Return a validated table name in ``schema.table`` or ``db.schema.table`` format."""
    schema = validate_sql_identifier(schema)
    table = validate_sql_identifier(table)
    if database:
        database = validate_sql_identifier(database)
        return f"{database}.{schema}.{table}"
    return f"{schema}.{table}"


def validate_sql_statement(sql: str, allow_ddl: bool = False) -> str:
    """Perform a few basic checks to guard against obvious SQL injection."""
    if not sql or not sql.strip():
        raise ValueError("SQL statement cannot be empty")
    check_sql = sql.upper()
    for kw in _DANGEROUS:
        if kw in check_sql and not allow_ddl:
            raise ValueError(f"Dangerous keyword detected: {kw}")
    if ';' in sql and sql.strip().count(';') > 1:
        raise ValueError("Multiple statements are not allowed")
    return sql
