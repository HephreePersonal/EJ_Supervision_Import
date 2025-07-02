import os
import pytest
import sys, types
import sqlite3

if "tqdm" not in sys.modules:
    dummy = types.ModuleType("tqdm")
    def _tqdm(iterable, **kwargs):
        for item in iterable:
            yield item
    dummy.tqdm = _tqdm
    sys.modules["tqdm"] = dummy

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

from etl.core import sanitize_sql


def test_sanitize_sql_executes_parameterized():
    conn = sqlite3.connect(":memory:")
    conn.execute("CREATE TABLE t(val TEXT)")
    sanitize_sql(conn, "INSERT INTO t(val) VALUES (?)", params=("ok",))
    count = conn.execute("SELECT COUNT(*) FROM t").fetchone()[0]
    assert count == 1


def test_sanitize_sql_prevents_injection():
    conn = sqlite3.connect(":memory:")
    conn.execute("CREATE TABLE t(val TEXT)")
    malicious = "'); DROP TABLE t; --"
    sanitize_sql(conn, "INSERT INTO t(val) VALUES (?)", params=(malicious,))
    # Table should still exist
    count = conn.execute("SELECT COUNT(*) FROM t").fetchone()[0]
    assert count == 1


def test_sanitize_sql_randomized_inputs():
    """Fuzz user supplied values to ensure no injection is possible."""
    import random
    import string

    conn = sqlite3.connect(":memory:")
    conn.execute("CREATE TABLE t(val TEXT)")

    def rand_text():
        chars = string.ascii_letters + string.digits + string.punctuation
        return "".join(random.choice(chars) for _ in range(random.randint(5, 20)))

    for _ in range(50):
        value = rand_text()
        sanitize_sql(conn, "INSERT INTO t(val) VALUES (?)", params=(value,))

    count = conn.execute("SELECT COUNT(*) FROM t").fetchone()[0]
    assert count == 50
