"""Basic end-to-end integration test for a database importer."""

import sqlite3
import argparse
import sys
import types

# Stub heavy optional dependencies if they are missing.  This mirrors the
# approach used in the unit tests for the other modules.
if "pandas" not in sys.modules:
    sys.modules["pandas"] = types.ModuleType("pandas")
if "sqlalchemy" not in sys.modules:
    sa_mod = types.ModuleType("sqlalchemy")
    sa_mod.types = types.SimpleNamespace(Text=lambda *a, **k: None)
    sa_mod.MetaData = lambda *a, **k: None
    pool_mod = types.ModuleType("pool")
    pool_mod.NullPool = object
    sa_mod.pool = pool_mod
    engine_mod = types.ModuleType("engine")
    engine_mod.Engine = object
    engine_mod.Connection = object
    engine_mod.URL = types.SimpleNamespace(create=lambda *a, **k: None)
    sa_mod.engine = engine_mod
    sys.modules["sqlalchemy"] = sa_mod
    sys.modules["sqlalchemy.types"] = sa_mod.types
    sys.modules["sqlalchemy.pool"] = pool_mod
    sys.modules["sqlalchemy.engine"] = engine_mod
if "tqdm" not in sys.modules:
    dummy = types.ModuleType("tqdm")
    dummy.tqdm = lambda it, **kw: it
    sys.modules["tqdm"] = dummy
if "pyodbc" not in sys.modules:
    class _DummyError(Exception):
        pass
    sys.modules["pyodbc"] = types.SimpleNamespace(Error=_DummyError)
if "dotenv" not in sys.modules:
    mod = types.ModuleType("dotenv")
    mod.load_dotenv = lambda *a, **k: None
    sys.modules["dotenv"] = mod
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

from etl.base_importer import BaseDBImporter
import db.connections as connections


class MiniImporter(BaseDBImporter):
    """Very small importer used for testing the run() workflow."""

    DB_TYPE = "Mini"
    DEFAULT_LOG_FILE = "mini.log"

    def parse_args(self):
        """Return a dummy args namespace expected by ``BaseDBImporter``."""
        return argparse.Namespace(
            log_file=None,
            csv_file=None,
            include_empty=False,
            skip_pk_creation=False,
            config_file=None,
            verbose=False,
        )

    # The following hooks implement a trivial workflow that simply creates
    # and populates a table in the temporary database.  All other optional
    # steps are skipped by overriding with no-op implementations.
    def execute_preprocessing(self, conn):
        conn.execute("CREATE TABLE numbers (id INTEGER PRIMARY KEY, num INTEGER)")

    def prepare_drop_and_select(self, conn):
        pass

    def update_joins_in_tables(self, conn):
        pass

    def execute_table_operations(self, conn):
        conn.executemany(
            "INSERT INTO numbers(num) VALUES (?)",
            [(1,), (2,)],
        )

    def import_joins(self):  # pragma: no cover - not needed for this test
        pass

    def create_primary_keys(self, conn):  # pragma: no cover - PK already set
        pass

    def get_next_step_name(self):
        return None

    def show_completion_message(self, next_step_name=None):  # pragma: no cover
        return False


class FullImporter(BaseDBImporter):
    """Importer exercising the full ``run`` workflow."""

    DB_TYPE = "Full"
    DEFAULT_LOG_FILE = "full.log"

    def parse_args(self):
        return argparse.Namespace(
            log_file=None,
            csv_file=None,
            include_empty=False,
            skip_pk_creation=False,
            config_file=None,
            verbose=False,
        )

    def execute_preprocessing(self, conn):
        conn.execute("CREATE TABLE src (id INTEGER PRIMARY KEY, val INTEGER)")
        conn.executemany("INSERT INTO src VALUES (?, ?)", [(1, 10), (2, 20)])

    def prepare_drop_and_select(self, conn):
        conn.execute(
            "CREATE TABLE tables (Drop_IfExists TEXT, Select_Into TEXT, ScopeRowCount INTEGER)"
        )
        conn.execute(
            "INSERT INTO tables VALUES ('DROP TABLE IF EXISTS dest', 'CREATE TABLE dest AS SELECT * FROM src', 2)"
        )

    def update_joins_in_tables(self, conn):
        pass

    def execute_table_operations(self, conn):
        row = conn.execute("SELECT Drop_IfExists, Select_Into FROM tables").fetchone()
        conn.execute(row[0])
        conn.execute(row[1])

    def import_joins(self):  # pragma: no cover - not needed
        pass

    def create_primary_keys(self, conn):  # pragma: no cover - PK already set
        pass

    def get_next_step_name(self):
        return None

    def show_completion_message(self, next_step_name=None):  # pragma: no cover
        return False


def test_end_to_end_mini_importer(monkeypatch, tmp_path):
    """Run the ``MiniImporter`` using an in-memory SQLite database."""

    # Provide required environment variables for ``validate_environment``.
    monkeypatch.setenv("MSSQL_TARGET_CONN_STR", "Driver=SQLite;Database=:memory:")
    monkeypatch.setenv("EJ_CSV_DIR", str(tmp_path))
    monkeypatch.setenv("EJ_LOG_DIR", str(tmp_path))

    # Use an in-memory SQLite database instead of MSSQL.
    conn = sqlite3.connect(":memory:")

    # Patch the connection retrieval used inside BaseDBImporter
    monkeypatch.setattr(connections, "get_target_connection", lambda: conn)
    monkeypatch.setattr("etl.base_importer.get_target_connection", lambda: conn)

    importer = MiniImporter()

    # ``run`` should complete successfully and return ``False`` since our
    # ``show_completion_message`` always opts not to continue.
    assert importer.run() is False

    # Verify the table was created and populated.
    cur = conn.cursor()
    cur.execute("SELECT COUNT(*) FROM numbers")
    count = cur.fetchone()[0]
    cur.close()
    assert count == 2

    # Explicitly close the connection to release resources.
    conn.close()


def test_end_to_end_full_importer(monkeypatch, tmp_path):
    """Run the ``FullImporter`` to exercise the complete workflow."""

    monkeypatch.setenv("MSSQL_TARGET_CONN_STR", "Driver=SQLite;Database=:memory:")
    monkeypatch.setenv("EJ_CSV_DIR", str(tmp_path))
    monkeypatch.setenv("EJ_LOG_DIR", str(tmp_path))

    conn = sqlite3.connect(":memory:")

    monkeypatch.setattr(connections, "get_target_connection", lambda: conn)
    monkeypatch.setattr("etl.base_importer.get_target_connection", lambda: conn)

    importer = FullImporter()

    assert importer.run() is False

    rows = conn.execute("SELECT id, val FROM dest ORDER BY id").fetchall()
    assert rows == [(1, 10), (2, 20)]

    conn.close()

