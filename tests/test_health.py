import types, sys

# Stub heavy optional dependencies similar to other tests
if "sqlalchemy" not in sys.modules:
    sa_mod = types.ModuleType("sqlalchemy")
    sa_mod.create_engine = lambda *a, **k: None
    sa_mod.MetaData = lambda *a, **k: None
    pool_mod = types.ModuleType("pool")
    pool_mod.NullPool = object
    sa_mod.pool = pool_mod
    engine_mod = types.ModuleType("engine")
    engine_mod.Engine = object
    engine_mod.Connection = object
    engine_mod.URL = types.SimpleNamespace(create=lambda *a, **k: None)
    sa_mod.engine = engine_mod
    sys.modules["sqlalchemy"] = sa_mod
    sys.modules["sqlalchemy.pool"] = pool_mod
    sys.modules["sqlalchemy.engine"] = engine_mod
if "pydantic" not in sys.modules:
    pd_mod = types.ModuleType("pydantic")
    class _SecretStr(str):
        def get_secret_value(self):
            return str(self)
    pd_mod.SecretStr = _SecretStr
    pd_mod.BaseSettings = object
    pd_mod.DirectoryPath = str
    pd_mod.Field = lambda *a, **k: None
    pd_mod.validator = lambda *a, **k: (lambda f: f)
    sys.modules["pydantic"] = pd_mod
    ps_mod = types.ModuleType("pydantic_settings")
    ps_mod.BaseSettings = object
    sys.modules["pydantic_settings"] = ps_mod
if "dotenv" not in sys.modules:
    mod = types.ModuleType("dotenv")
    mod.load_dotenv = lambda *a, **k: None
    sys.modules["dotenv"] = mod
if "mysql" not in sys.modules:
    dummy_mysql = types.ModuleType("mysql")
    dummy_mysql.connector = types.SimpleNamespace(connect=lambda **k: None)
    sys.modules["mysql"] = dummy_mysql
    sys.modules["mysql.connector"] = dummy_mysql.connector

# Stub pyodbc if not present
if "pyodbc" not in sys.modules:
    class _DummyError(Exception):
        pass
    sys.modules["pyodbc"] = types.SimpleNamespace(
        Error=_DummyError,
        connect=lambda *a, **k: types.SimpleNamespace(close=lambda: None)
    )

from db.health import check_connection, check_target_connection
from config import settings
from pydantic import SecretStr


def test_check_connection_success():
    assert check_connection("Driver=SQL;Server=.;Database=db;") is True


def test_check_connection_failure(monkeypatch):
    def boom(*a, **k):
        raise RuntimeError("fail")
    monkeypatch.setattr(sys.modules["pyodbc"], "connect", boom)
    assert check_connection("foo") is False


def test_check_target_connection(monkeypatch):
    monkeypatch.setattr(settings, "mssql_target_conn_str", SecretStr("Driver=SQL;"))
    assert check_target_connection()

