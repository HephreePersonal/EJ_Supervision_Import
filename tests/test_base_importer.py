import os
import pytest
import sys, types
import argparse


from etl.base_importer import BaseDBImporter
from utils.progress_tracker import ProgressTracker


def test_validate_environment_missing_all(monkeypatch):
    monkeypatch.delenv('MSSQL_TARGET_CONN_STR', raising=False)
    monkeypatch.delenv('EJ_CSV_DIR', raising=False)
    with pytest.raises(EnvironmentError):
        BaseDBImporter().validate_environment()


def test_validate_environment_missing_csv_dir(monkeypatch):
    monkeypatch.setenv('MSSQL_TARGET_CONN_STR', 'Driver=SQL;Server=.;Database=db;')
    monkeypatch.delenv('EJ_CSV_DIR', raising=False)
    with pytest.raises(EnvironmentError):
        BaseDBImporter().validate_environment()


def test_validate_environment_invalid_csv_dir(monkeypatch, tmp_path):
    monkeypatch.setenv('MSSQL_TARGET_CONN_STR', 'Driver=SQL;Server=.;Database=db;')
    invalid_path = tmp_path / 'missing'
    monkeypatch.setenv('EJ_CSV_DIR', str(invalid_path))
    with pytest.raises(EnvironmentError):
        BaseDBImporter().validate_environment()


def test_load_config_env_and_args(monkeypatch, tmp_path):
    monkeypatch.setenv('MSSQL_TARGET_CONN_STR', 'Driver=SQL;Server=.;Database=db;')
    monkeypatch.setenv('EJ_CSV_DIR', str(tmp_path))
    monkeypatch.setenv('EJ_LOG_DIR', str(tmp_path))
    monkeypatch.setenv('SQL_TIMEOUT', '200')
    monkeypatch.setenv('INCLUDE_EMPTY_TABLES', '1')
    monkeypatch.setenv('CSV_CHUNK_SIZE', '1234')

    args = argparse.Namespace(
        log_file=None,
        csv_file=None,
        include_empty=False,
        skip_pk_creation=True,
        config_file=None,
        verbose=False,
    )

    importer = BaseDBImporter()
    importer.load_config(args)

    assert importer.config['include_empty_tables'] is True
    assert importer.config['skip_pk_creation'] is True
    assert importer.config['sql_timeout'] == 200
    assert importer.config['csv_file'].endswith(importer.DEFAULT_CSV_FILE)
    assert importer.config['log_file'].endswith(importer.DEFAULT_LOG_FILE)
    assert importer.config['csv_chunk_size'] == 1234


def test_show_completion_message(monkeypatch):
    importer = BaseDBImporter()

    dummy_tk = types.SimpleNamespace(withdraw=lambda: None, destroy=lambda: None)
    monkeypatch.setattr('etl.base_importer.tk.Tk', lambda: dummy_tk)
    monkeypatch.setattr('etl.base_importer.messagebox.askyesno', lambda *a, **k: True)

    assert importer.show_completion_message('Next') is True

    info_called = {}
    monkeypatch.setattr('etl.base_importer.messagebox.showinfo', lambda *a, **k: info_called.setdefault('called', True))
    assert importer.show_completion_message(None) is False
    assert info_called.get('called')


def test_process_table_row_validation(tmp_path, monkeypatch):
    importer = BaseDBImporter()
    importer.config = {
        'sql_timeout': 100,
        'include_empty_tables': True,
        'log_file': str(tmp_path / 'err.log'),
    }
    importer.db_name = 'main'

    import sqlite3

    conn = sqlite3.connect(':memory:')
    conn.execute('CREATE TABLE src(id INTEGER)')
    conn.executemany('INSERT INTO src VALUES (?)', [(1,), (2,)])
    conn.execute("CREATE TABLE 'main.dbo.TablesToConvert_base'(RowID INTEGER PRIMARY KEY, ScopeRowCount INTEGER)")
    conn.execute("INSERT INTO 'main.dbo.TablesToConvert_base' VALUES (1, 3)")

    def fake_exec(c, sql, params=None, timeout=100):
        if params:
            return c.execute(sql, params)
        return c.execute(sql)

    monkeypatch.setattr('utils.etl_helpers.execute_sql_with_timeout', fake_exec)
    monkeypatch.setattr('etl.base_importer.execute_sql_with_timeout', fake_exec)
    def fake_sanitize(c, sql, params=None, timeout=100):
        sql = sql.replace("main.dbo.TablesToConvert_base", "'main.dbo.TablesToConvert_base'")
        return fake_exec(c, sql, params)
    monkeypatch.setattr('etl.base_importer.sanitize_sql', fake_sanitize)

    row = {
        'RowID': 1,
        'Drop_IfExists': 'DROP TABLE IF EXISTS dest',
        'Select_Into': 'CREATE TABLE dest AS SELECT * FROM src',
        'TableName': 'dest',
        'SchemaName': 'main',
        'ScopeRowCount': 3,
        'fConvert': 1,
    }

    result = importer._process_table_operation_row(conn, row, 1, importer.config['log_file'])
    assert result is True
    assert conn.execute('SELECT COUNT(*) FROM dest').fetchone()[0] == 2
    assert conn.execute("SELECT ScopeRowCount FROM 'main.dbo.TablesToConvert_base' WHERE RowID=1").fetchone()[0] == 2


def test_progress_helpers(tmp_path):
    path = tmp_path / "prog.json"
    tracker = ProgressTracker(str(path))

    assert tracker.get("table_operations") == 0
    tracker.update("table_operations", 5)
    assert tracker.get("table_operations") == 5
    tracker.delete()
    assert not path.exists()


def test_should_process_table_overrides():
    importer = BaseDBImporter()
    importer.config = {
        'include_empty_tables': False,
        'always_include_tables': ['s.t'],
    }

    assert importer._should_process_table(0, 's', 't') is True
    assert importer._should_process_table(0, 'x', 'y') is False


def test_drop_empty_tables(tmp_path, monkeypatch):
    importer = BaseDBImporter()
    importer.config = {
        'sql_timeout': 100,
        'include_empty_tables': False,
        'log_file': str(tmp_path / 'err.log'),
        'always_include_tables': [],
    }
    importer.db_name = 'main'

    import sqlite3

    conn = sqlite3.connect(':memory:')
    conn.execute('CREATE TABLE dest(id INTEGER)')
    conn.execute("CREATE TABLE 'main.dbo.TablesToConvert_base'(RowID INTEGER PRIMARY KEY, SchemaName TEXT, TableName TEXT, fConvert INTEGER, ScopeRowCount INTEGER)")
    conn.execute("INSERT INTO 'main.dbo.TablesToConvert_base' VALUES (1, 'main', 'dest', 1, 0)")

    def fake_exec(c, sql, params=None, timeout=100):
        sql = sql.replace('ISNULL', 'IFNULL')
        sql = sql.replace("main.dbo.TablesToConvert_base", "'main.dbo.TablesToConvert_base'")
        if params:
            return c.execute(sql, params)
        return c.execute(sql)

    monkeypatch.setattr('utils.etl_helpers.execute_sql_with_timeout', fake_exec)
    monkeypatch.setattr('etl.base_importer.execute_sql_with_timeout', fake_exec)
    monkeypatch.setattr('etl.base_importer.sanitize_sql', fake_exec)

    importer.drop_empty_tables(conn)

    remaining = conn.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='dest'").fetchall()
    assert remaining == []


def test_process_table_row_error_propagates(tmp_path, monkeypatch):
    importer = BaseDBImporter()
    importer.config = {
        "sql_timeout": 100,
        "include_empty_tables": True,
        "log_file": str(tmp_path / "err.log"),
    }
    importer.db_name = "main"

    import sqlite3

    conn = sqlite3.connect(":memory:")

    class DummyCursor:
        def fetchone(self):
            return (0,)

    def fake_exec(*args, **kwargs):
        return DummyCursor()

    monkeypatch.setattr("utils.etl_helpers.execute_sql_with_timeout", fake_exec)
    monkeypatch.setattr("etl.base_importer.execute_sql_with_timeout", fake_exec)

    from utils.etl_helpers import SQLExecutionError

    def fake_sanitize(*args, **kwargs):
        raise SQLExecutionError("DROP", Exception("boom"), table_name="dbo.dest")

    monkeypatch.setattr("etl.base_importer.sanitize_sql", fake_sanitize)

    row = {
        "RowID": 1,
        "Drop_IfExists": "DROP TABLE dest",
        "Select_Into": "CREATE TABLE dest AS SELECT * FROM src",
        "TableName": "dest",
        "SchemaName": "dbo",
        "ScopeRowCount": 1,
        "fConvert": 1,
    }

    with pytest.raises(SQLExecutionError):
        importer._process_table_operation_row(conn, row, 1, importer.config["log_file"])
