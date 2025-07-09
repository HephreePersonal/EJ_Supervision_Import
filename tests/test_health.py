import sys


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

