import sys
import db.connections as connections
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
    monkeypatch.setattr(connections.sqlalchemy, 'create_engine', fake_create_engine, raising=False)

    conn = connections.get_target_connection()
    assert isinstance(conn, DummyConn)
    assert created['kwargs']['pool_size'] == settings.db_pool_size
