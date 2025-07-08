import logging
import re
from dataclasses import dataclass
from typing import Optional, List

logger = logging.getLogger(__name__)

_IDENTIFIER_RE = re.compile(r'^[A-Za-z_][A-Za-z0-9_]*$')
_DANGEROUS = {"DROP", "DELETE", "TRUNCATE", "ALTER", "EXEC", "EXECUTE"}


@dataclass
class ValidationResult:
    """Result of SQL validation."""

    is_valid: bool
    issues: List[str]


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


class SQLSecurityValidator:
    """Wrapper class providing simple SQL validation methods."""

    def validate_sql_statement(self, sql: str, allow_ddl: bool = False) -> ValidationResult:
        """Validate ``sql`` and return a ``ValidationResult``."""
        try:
            validate_sql_statement(sql, allow_ddl=allow_ddl)
            return ValidationResult(True, [])
        except Exception as exc:  # pragma: no cover - validation failures
            logger.warning("SQL validation issue: %s", exc)
            return ValidationResult(False, [str(exc)])

    def validate_table_name(self, schema: str, table: str, database: Optional[str] = None) -> str:
        """Delegate to :func:`validate_table_name`. Raises :class:`ValueError` on failure."""
        return validate_table_name(schema, table, database)

    def validate_sql_identifier(self, identifier: str) -> str:
        """Delegate to :func:`validate_sql_identifier`. Raises :class:`ValueError` on failure."""
        return validate_sql_identifier(identifier)


__all__ = [
    "validate_sql_identifier",
    "validate_table_name",
    "validate_sql_statement",
    "SQLSecurityValidator",
    "ValidationResult",
]
