"""Basic end-to-end integration test for a database importer."""

import sqlite3
import argparse
import sys
import types


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

