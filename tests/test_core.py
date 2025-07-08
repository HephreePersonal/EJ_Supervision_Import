import pytest
import sqlite3

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
