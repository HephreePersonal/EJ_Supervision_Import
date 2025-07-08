import pytest
import sys

from config import ETLConstants
from utils.etl_helpers import (
    run_sql_step,
    run_sql_script,
    run_sql_step_with_retry,
    load_sql,
    SQLExecutionError,
    transaction_scope,
)

class DummyCursor:
    def __init__(self, fail=False, fail_sql=None, conn=None):
        self.fail = fail
        self.fail_sql = fail_sql
        self.conn = conn
    def execute(self, sql, params=None):
        if 'SET LOCK_TIMEOUT' in sql:
            return
        if (
            self.fail
            or (self.fail_sql and sql.strip() == self.fail_sql)
            or (self.conn and self.conn.fail_times > 0)
        ):
            if self.conn and self.conn.fail_times > 0:
                self.conn.fail_times -= 1
            raise sys.modules["pyodbc"].Error("boom")
    def fetchall(self):
        return [('row',)]
    def __enter__(self):
        return self
    def __exit__(self, exc_type, exc, tb):
        pass

class DummyConn:
    def __init__(self, fail=False, fail_sql=None, fail_times=0):
        self.fail = fail
        self.fail_sql = fail_sql
        self.fail_times = fail_times
        self.autocommit = True
        self.commits = 0
        self.rollbacks = 0

    def cursor(self):
        return DummyCursor(self.fail, self.fail_sql, conn=self)

    def commit(self):
        self.commits += 1

    def rollback(self):
        self.rollbacks += 1


class DummyConnNoAutocommit(DummyConn):
    """Dummy connection without an ``autocommit`` attribute."""

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        del self.autocommit


def test_run_sql_step_success():
    conn = DummyConn()
    result = run_sql_step(conn, 'test', 'SELECT 1')
    assert result == [('row',)]


def test_run_sql_step_failure():
    conn = DummyConn(fail=True)
    with pytest.raises(SQLExecutionError) as exc:
        run_sql_step(conn, 'table', 'SELECT 1')
    assert exc.value.sql == 'SELECT 1'
    assert exc.value.table_name == 'table'


def test_run_sql_script_failure(monkeypatch):
    sql = 'SELECT 1; FAIL; SELECT 2'
    conn = DummyConn(fail_sql='FAIL')
    monkeypatch.setattr('utils.etl_helpers.has_migration', lambda c, n: False)
    with pytest.raises(SQLExecutionError) as exc:
        run_sql_script(conn, 'table', sql)
    assert exc.value.sql.strip() == 'FAIL'
    assert exc.value.table_name == 'table'


def test_run_sql_step_with_retry_success():
    conn = DummyConn()
    result = run_sql_step_with_retry(conn, 'test', 'SELECT 1')
    assert result == [('row',)]


def test_run_sql_step_with_retry_retries(monkeypatch):
    conn = DummyConn(fail_times=2)
    result = run_sql_step_with_retry(
        conn, 'test', 'SELECT 1', max_retries=ETLConstants.MAX_RETRY_ATTEMPTS
    )
    assert result == [('row',)]


def test_run_sql_step_with_retry_deadlock(monkeypatch):
    class DeadlockError(sys.modules["pyodbc"].Error):
        pass

    conn = DummyConn()
    calls = {'count': 0}

    def fake_step(c, name, sql, timeout):
        calls['count'] += 1
        if calls['count'] == 1:
            raise SQLExecutionError(sql, DeadlockError('deadlock'), table_name=name)
        return [('row',)]

    monkeypatch.setattr('utils.etl_helpers.run_sql_step', fake_step)
    result = run_sql_step_with_retry(conn, 'dead', 'SELECT 1', max_retries=2)
    assert result == [('row',)]
    assert calls['count'] == 2


def test_load_sql_valid_path():
    sql = load_sql('misc/gather_lobs.sql')
    assert 'CREATE TABLE' in sql


def test_load_sql_path_traversal():
    with pytest.raises(ValueError):
        load_sql('../utils/etl_helpers.py')


def test_load_sql_invalid_parent_reference():
    with pytest.raises(ValueError):
        load_sql('../etc/passwd')


def test_transaction_scope_commit_and_restore():
    conn = DummyConn()
    assert conn.autocommit is True
    with transaction_scope(conn):
        assert conn.autocommit is False
    assert conn.autocommit is True
    assert conn.commits == 1
    assert conn.rollbacks == 0


def test_transaction_scope_rollback_on_error():
    conn = DummyConn()
    with pytest.raises(RuntimeError):
        with transaction_scope(conn):
            assert conn.autocommit is False
            raise RuntimeError('boom')
    assert conn.autocommit is True
    assert conn.commits == 0
    assert conn.rollbacks == 1


def test_transaction_scope_no_autocommit_attribute_commit():
    conn = DummyConnNoAutocommit()
    assert not hasattr(conn, 'autocommit')
    with transaction_scope(conn):
        assert not hasattr(conn, 'autocommit')
    assert not hasattr(conn, 'autocommit')
    assert conn.commits == 1
    assert conn.rollbacks == 0


def test_transaction_scope_no_autocommit_attribute_rollback():
    conn = DummyConnNoAutocommit()
    with pytest.raises(RuntimeError):
        with transaction_scope(conn):
            raise RuntimeError('boom')
    assert not hasattr(conn, 'autocommit')
    assert conn.commits == 0
    assert conn.rollbacks == 1
