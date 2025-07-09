"""Core ETL utilities shared across all database import scripts.

These helpers provide environment validation, configuration loading and
SQL execution utilities used by the importer modules.  For an overview of
the entire ETL pipeline and configuration options see the ``Architecture``
and ``Configuration`` sections of ``README.md``.
"""

from __future__ import annotations

import logging
import os
import json
import re
import unicodedata
from typing import Any, Dict, Generator, Iterable, Iterator, Optional, Tuple, TypeVar

from tqdm import tqdm
from config import ETLConstants

T = TypeVar("T")

_IDENTIFIER_RE = re.compile(r'^[A-Za-z_][A-Za-z0-9_]*$')

logger = logging.getLogger(__name__)


class ConfigError(Exception):
    """Raised when required configuration is missing or invalid."""
    pass



def validate_environment(required_vars: Dict[str, str], optional_vars: Dict[str, str]) -> None:
    """Validate environment variables against required and optional sets.

    Refer to the ``Environment Variables`` table in ``README.md`` for a
    description of available settings.
    """
    # Check required vars
    missing = []
    for var, desc in required_vars.items():
        if not os.environ.get(var):
            missing.append(f"{var}: {desc}")
    
    if missing:
        raise EnvironmentError(f"Missing required environment variables:\n" + 
                              "\n".join(missing))
    
    # Log optional vars
    for var, desc in optional_vars.items():
        value = os.environ.get(var)
        if value:
            logger.info(f"Using {var}={value}")
        else:
            logger.info(f"{var} not set. {desc}")
            
    # Validate paths
    csv_dir = os.environ.get('EJ_CSV_DIR')
    if not (csv_dir and os.path.isdir(csv_dir)):
        raise EnvironmentError(f"EJ_CSV_DIR directory does not exist: {csv_dir}")

def load_config(config_file: str | None = None, default_config: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    """Load configuration from JSON file or fall back to defaults.

    The expected structure of the configuration file is documented in the
    ``Configuration`` section of ``README.md``.
    """
    config: Dict[str, Any] = default_config or {}
    
    if config_file and os.path.exists(config_file):
        try:
            with open(config_file, 'r', encoding='utf-8') as f:
                file_config = json.load(f)
                config.update(file_config)
            logger.info(f"Loaded configuration from {config_file}")
        except Exception as e:
            logger.error(f"Error loading config file: {e}")
    
    return config

from utils.etl_helpers import execute_sql_with_timeout


def sanitize_sql(
    conn: Any,
    sql_text: str | None,
    params: Optional[Tuple[Any, ...]] = None,
    timeout: int = ETLConstants.DEFAULT_SQL_TIMEOUT,
) -> Any:
    """Execute a SQL statement using parameterized queries.

    Earlier versions sanitized SQL via regular expressions which provided only
    limited protection.  ``sanitize_sql`` now delegates to
    ``execute_sql_with_timeout`` which enforces parameterization.  See the
    ``Security Considerations`` section in ``README.md`` for additional
    guidance.
    """

    if sql_text is None:
        return None

    return execute_sql_with_timeout(conn, sql_text, params=params, timeout=timeout)

def safe_tqdm(iterable: Iterable[T], **kwargs: Any) -> Iterator[T]:
    """Wrapper for ``tqdm`` that degrades gracefully if progress bars fail.

    Progress display is optional and can be tuned as described in the
    ``Performance Tuning`` section of ``README.md``.
    """
    try:
        # First try with default settings
        for item in tqdm(iterable, **kwargs):
            yield item
    except OSError:
        # If that fails, try with a safer configuration
        for item in tqdm(iterable, ascii=True, disable=None, **kwargs):
            yield item
    except:
        # If all tqdm attempts fail, just use the regular iterable
        print(f"Progress bar disabled: {kwargs.get('desc', 'Processing')}")
        for item in iterable:
            yield item


def validate_sql_identifier(identifier: str) -> str:
    """Validate a string for use as a SQL identifier.

    Only allows alphanumeric characters and underscores and must not start with a digit.
    Validation mirrors the guidelines outlined under ``Security Considerations``
    in ``README.md``.

    Args:
        identifier: The identifier to validate.

    Returns:
        The original identifier if valid.

    Raises:
        ValueError: If the identifier is invalid.
    """
    if not isinstance(identifier, str):
        raise ValueError("Identifier must be a string")

    if not _IDENTIFIER_RE.match(identifier):
        raise ValueError(f"Invalid SQL identifier: {identifier}")

    return identifier
