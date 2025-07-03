import sys, types
if "dotenv" not in sys.modules:
    mod=types.ModuleType("dotenv")
    mod.load_dotenv=lambda *a, **k: None
    sys.modules["dotenv"] = mod
if "sqlalchemy" not in sys.modules:
    sa_mod = types.ModuleType("sqlalchemy")
    sa_mod.create_engine = lambda *a, **k: None
    sa_mod.engine = types.SimpleNamespace(URL=types.SimpleNamespace(create=lambda *a, **k: None))
    sa_mod.MetaData = lambda *a, **k: None
    pool_mod = types.ModuleType("pool")
    pool_mod.NullPool = object
    sa_mod.pool = pool_mod
    engine_mod = types.ModuleType("engine")
    engine_mod.Engine = object
    engine_mod.Connection = object
    sa_mod.engine = engine_mod
    sys.modules["sqlalchemy"] = sa_mod
    sys.modules["sqlalchemy.pool"] = pool_mod
    sys.modules["sqlalchemy.engine"] = engine_mod
if "pyodbc" not in sys.modules:
    sys.modules["pyodbc"] = types.SimpleNamespace(connect=lambda *a, **k: None)
if "mysql" not in sys.modules:
    dummy_mysql = types.ModuleType("mysql")
    dummy_mysql.connector = types.SimpleNamespace(connect=lambda **k: None)
    sys.modules["mysql"] = dummy_mysql
    sys.modules["mysql.connector"] = dummy_mysql.connector
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

import pytest

import db.mysql as mysql

class DummyConn:
    pass

class DummyEngine:
    def connect(self):
        return DummyConn()

def test_get_mysql_connection_env(monkeypatch):
    monkeypatch.setenv('MYSQL_HOST', 'localhost')
    monkeypatch.setenv('MYSQL_USER', 'user')
    monkeypatch.setenv('MYSQL_PASSWORD', 'pass')
    monkeypatch.setenv('MYSQL_DATABASE', 'db')
    monkeypatch.setenv('MYSQL_PORT', '3307')

    called = {}

    def fake_create_engine(url, **kwargs):
        called['url'] = url
        called['kwargs'] = kwargs
        return DummyEngine()

    monkeypatch.setattr(mysql.sqlalchemy, 'create_engine', fake_create_engine, raising=False)
    monkeypatch.setattr(mysql, '_engine', None, raising=False)

    conn = mysql.get_mysql_connection()
    assert isinstance(conn, DummyConn)
    assert called['kwargs']['pool_size'] == mysql.settings.db_pool_size


def test_get_mysql_connection_missing(monkeypatch):
    monkeypatch.delenv('MYSQL_HOST', raising=False)
    monkeypatch.delenv('MYSQL_USER', raising=False)
    monkeypatch.delenv('MYSQL_PASSWORD', raising=False)
    monkeypatch.delenv('MYSQL_DATABASE', raising=False)

    with pytest.raises(ValueError):
        mysql.get_mysql_connection()
