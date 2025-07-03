import sys, types
if "mysql" not in sys.modules:
    dummy_mysql = types.ModuleType("mysql")
    dummy_mysql.connector = types.SimpleNamespace(connect=lambda **k: None)
    sys.modules["mysql"] = dummy_mysql
    sys.modules["mysql.connector"] = dummy_mysql.connector
if "dotenv" not in sys.modules:
    mod=types.ModuleType("dotenv")
    mod.load_dotenv=lambda *a, **k: None
    sys.modules["dotenv"] = mod
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
    sa_mod.engine = engine_mod
    sys.modules["sqlalchemy"] = sa_mod
    sys.modules["sqlalchemy.engine"] = engine_mod
    sys.modules["sqlalchemy.pool"] = pool_mod
if "pyodbc" not in sys.modules:
    sys.modules["pyodbc"] = types.SimpleNamespace(connect=lambda *a, **k: None)
if "pydantic" not in sys.modules:
    pd_mod = types.ModuleType("pydantic")
    class _BaseSettings:
        def __init__(self, **values):
            for k, v in values.items():
                setattr(self, k, v)
    pd_mod.BaseSettings = _BaseSettings
    pd_mod.DirectoryPath = str
    pd_mod.Field = lambda *a, **k: None
    class _SecretStr(str):
        def get_secret_value(self):
            return str(self)
    pd_mod.SecretStr = _SecretStr
    def _validator(*a, **k):
        def dec(func):
            return func
        return dec
    pd_mod.validator = _validator
    sys.modules["pydantic"] = pd_mod
    ps_mod = types.ModuleType("pydantic_settings")
    ps_mod.BaseSettings = _BaseSettings
    sys.modules["pydantic_settings"] = ps_mod
import db.mssql as mssql
from config import settings

class DummyConn:
    pass

class DummyEngine:
    def __init__(self):
        self.url = None
    def connect(self):
        return DummyConn()

def test_get_target_connection(monkeypatch):
    conn_str = 'DRIVER=SQL;SERVER=server;DATABASE=db;'

    created = {}

    def fake_create_engine(url, **kwargs):
        created['url'] = url
        created['kwargs'] = kwargs
        return DummyEngine()

    from pydantic import SecretStr
    monkeypatch.setattr(settings, 'mssql_target_conn_str', SecretStr(conn_str))
    monkeypatch.setattr(settings, 'db_pool_size', 5, raising=False)
    monkeypatch.setattr(settings, 'db_max_overflow', 10, raising=False)
    monkeypatch.setattr(settings, 'db_pool_timeout', 30, raising=False)
    monkeypatch.setattr(mssql.sqlalchemy, 'create_engine', fake_create_engine, raising=False)

    conn = mssql.get_target_connection()
    assert isinstance(conn, DummyConn)
    assert created['kwargs']['pool_size'] == settings.db_pool_size
