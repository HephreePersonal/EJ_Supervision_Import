import sys, types

if "pyodbc" not in sys.modules:
    class _DummyError(Exception):
        pass
    sys.modules["pyodbc"] = types.SimpleNamespace(Error=_DummyError)

import timeit
import pytest
from utils.etl_helpers import run_sql_step_with_retry

class DummyCursor:
    def __init__(self, conn=None):
        self.conn = conn
    def execute(self, sql, params=None):
        if 'SET LOCK_TIMEOUT' in sql:
            return
    def fetchall(self):
        return [('row',)]
    def __enter__(self):
        return self
    def __exit__(self, exc_type, exc, tb):
        pass

class DummyConn:
    def cursor(self):
        return DummyCursor()
    def commit(self):
        pass
    def rollback(self):
        pass


@pytest.fixture
def benchmark_simple():
    def run(func, repeat=1000):
        return min(timeit.repeat(func, number=1, repeat=repeat))
    return run


def test_run_sql_step_with_retry_benchmark(benchmark_simple):
    conn = DummyConn()
    duration = benchmark_simple(lambda: run_sql_step_with_retry(conn, 'bench', 'SELECT 1'))
    assert duration >= 0
