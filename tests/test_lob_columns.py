import importlib
import math
import sys
import types
import time

# Stub optional dependencies like in other tests
dummy = types.ModuleType("tqdm")
class _DummyTqdm:
    def __init__(self, iterable=None, **kwargs):
        self.iterable = iterable or []
    def __iter__(self):
        return iter(self.iterable)
    def update(self, n=1):
        pass
    def close(self):
        pass
def _tqdm(iterable=None, **kwargs):
    return _DummyTqdm(iterable)
dummy.tqdm = _tqdm
sys.modules["tqdm"] = dummy
if "pyodbc" not in sys.modules:
    class _DummyError(Exception):
        pass
    sys.modules["pyodbc"] = types.SimpleNamespace(Error=_DummyError)
if "pandas" not in sys.modules:
    sys.modules["pandas"] = types.ModuleType("pandas")
if "sqlalchemy" not in sys.modules:
    sa_mod = types.ModuleType("sqlalchemy")
    sa_mod.types = types.SimpleNamespace(Text=lambda *a, **k: None)
    sys.modules["sqlalchemy"] = sa_mod
    sys.modules["sqlalchemy.types"] = sa_mod.types
if "mysql" not in sys.modules:
    dummy_mysql = types.ModuleType("mysql")
    dummy_mysql.connector = types.SimpleNamespace(connect=lambda **k: None)
    sys.modules["mysql"] = dummy_mysql
    sys.modules["mysql.connector"] = dummy_mysql.connector
if "dotenv" not in sys.modules:
    mod = types.ModuleType("dotenv")
    mod.load_dotenv = lambda *a, **k: None
    sys.modules["dotenv"] = mod
if "pydantic" not in sys.modules:
    pd_mod = types.ModuleType("pydantic")
    class _BaseSettings:
        def __init__(self, **values):
            for k, v in values.items():
                setattr(self, k, v)
    pd_mod.BaseSettings = _BaseSettings
    pd_mod.DirectoryPath = str
    pd_mod.Field = lambda *a, **k: None
    def _validator(*a, **k):
        def dec(func):
            return func
        return dec
    pd_mod.validator = _validator
    sys.modules["pydantic"] = pd_mod

lob = importlib.import_module("04_LOBColumns")

class DummySelectCursor:
    def __init__(self, rows):
        self.rows = rows
        self.index = 0
        self.description = [("SchemaName",), ("TableName",), ("ColumnName",), ("DataType",), ("CurrentLength",), ("RowCnt",)]

    def fetchmany(self, size):
        if self.index >= len(self.rows):
            return []
        res = self.rows[self.index : self.index + size]
        self.index += len(res)
        return res

class DummyUpdateCursor:
    def __init__(self, conn):
        self.conn = conn
        self.executed = []
        self.fast_executemany = False

    def executemany(self, sql, params):
        self.executed.extend(params)

    def execute(self, sql, params=None):
        if params:
            self.executed.append(params)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        pass

class DummyConn:
    def __init__(self):
        self.autocommit = True
        self.commits = 0
        self.rollbacks = 0
        self.last_cursor = None

    def cursor(self):
        self.last_cursor = DummyUpdateCursor(self)
        return self.last_cursor

    def commit(self):
        self.commits += 1

    def rollback(self):
        self.rollbacks += 1

def test_gather_lob_columns_batches(monkeypatch):
    rows = [("s", "t", f"c{i}", "text", None, 1) for i in range(250)]
    select_cursor = DummySelectCursor(rows)
    monkeypatch.setattr(lob, "execute_sql_with_timeout", lambda conn, q, timeout: select_cursor)
    monkeypatch.setattr(lob, "get_max_length", lambda *a, **k: 10)

    conn = DummyConn()
    cfg = {"include_empty_tables": True, "sql_timeout": 30, "batch_size": 100}

    start = time.perf_counter()
    lob.gather_lob_columns(conn, cfg, "log.txt")
    elapsed = time.perf_counter() - start

    expected_batches = math.ceil(len(rows) / cfg["batch_size"]) + 1  # final commit from transaction_scope
    assert conn.commits == expected_batches
    assert len(conn.last_cursor.executed) == len(rows)
    assert elapsed < 1.0


def test_gather_lob_columns_override(monkeypatch):
    rows = [("s", "t", "c", "text", None, 0)]
    select_cursor = DummySelectCursor(rows)
    monkeypatch.setattr(lob, "execute_sql_with_timeout", lambda conn, q, timeout: select_cursor)
    monkeypatch.setattr(lob, "get_max_length", lambda *a, **k: 10)

    conn = DummyConn()
    cfg = {
        "include_empty_tables": False,
        "always_include_tables": ["s.t"],
        "sql_timeout": 30,
        "batch_size": 10,
    }

    lob.gather_lob_columns(conn, cfg, "log.txt")
    assert len(conn.last_cursor.executed) == len(rows)
