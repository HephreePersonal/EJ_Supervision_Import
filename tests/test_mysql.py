import sys

import pytest

import db.connections as connections

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

    monkeypatch.setattr(connections.sqlalchemy, 'create_engine', fake_create_engine, raising=False)
    monkeypatch.setattr(connections, '_engines', {}, raising=False)

    conn = connections.get_mysql_connection()
    assert isinstance(conn, DummyConn)
    assert called['kwargs']['pool_size'] == connections.settings.db_pool_size


def test_get_mysql_connection_missing(monkeypatch):
    monkeypatch.delenv('MYSQL_HOST', raising=False)
    monkeypatch.delenv('MYSQL_USER', raising=False)
    monkeypatch.delenv('MYSQL_PASSWORD', raising=False)
    monkeypatch.delenv('MYSQL_DATABASE', raising=False)

    with pytest.raises(ValueError):
        connections.get_mysql_connection()
