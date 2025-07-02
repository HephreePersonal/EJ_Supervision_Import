import os
from dotenv import load_dotenv
import sqlalchemy
from typing import TYPE_CHECKING, Any, Optional, Union

if TYPE_CHECKING:
    from sqlalchemy.engine import Engine, Connection

Engine = Any
Connection = Any
from config import settings

from sqlalchemy.engine import Engine, Connection
_engine: Union[Engine, None] = None

# Load environment variables from .env file so manual calls behave the same
load_dotenv()

def _get_engine(
    host: str,
    user: str,
    password: str,
    database: str,
    port: int,
) -> Engine:
    """Return (and cache) a SQLAlchemy engine for the given parameters."""
    global _engine
    if _engine is None:
        url = (
            f"mysql+mysqlconnector://{user}:{password}@{host}:{port}/{database}"
        )
        _engine = sqlalchemy.create_engine(
            url,
            pool_size=settings.db_pool_size,
            max_overflow=settings.db_max_overflow,
            pool_timeout=settings.db_pool_timeout,
            pool_pre_ping=True,
        )
    return _engine


def get_mysql_connection(
    host: str = None,
    user: str = None,
    password: str = None,
    database: str = None,
    port: int = None,
) -> Connection:
    """Return a pooled MySQL connection using provided args or configuration."""
    host = host or os.getenv('MYSQL_HOST') or settings.mysql_host
    user = user or os.getenv('MYSQL_USER') or settings.mysql_user
    env_pass = os.getenv('MYSQL_PASSWORD')
    settings_pass = settings.mysql_password.get_secret_value() if settings.mysql_password else None
    password = password or env_pass or settings_pass
    database = database or os.getenv('MYSQL_DATABASE') or settings.mysql_database
    port_value = port or os.getenv('MYSQL_PORT') or settings.mysql_port or 3306
    port = int(port_value)

    if not all([host, user, password, database]):
        raise ValueError("Missing required MySQL connection parameters.")

    engine = _get_engine(host, user, password, database, port)
    return engine.connect()
