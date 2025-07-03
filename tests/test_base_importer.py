import os
import pytest
import sys, types
import argparse

if "tqdm" not in sys.modules:
    dummy = types.ModuleType("tqdm")
    def _tqdm(iterable, **kwargs):
        for item in iterable:
            yield item
    dummy.tqdm = _tqdm
    sys.modules["tqdm"] = dummy

if "pandas" not in sys.modules:
    sys.modules["pandas"] = types.ModuleType("pandas")

if "sqlalchemy" not in sys.modules:
    sa_mod = types.ModuleType("sqlalchemy")
    types_mod = types.SimpleNamespace(Text=lambda *a, **k: None)
    sa_mod.types = types_mod
    sa_mod.MetaData = lambda *a, **k: None
    pool_mod = types.ModuleType("pool")
    pool_mod.NullPool = object
    sa_mod.pool = pool_mod
    engine_mod = types.ModuleType("engine")
    engine_mod.Engine = object
    engine_mod.Connection = object
    sa_mod.engine = engine_mod
    sys.modules["sqlalchemy"] = sa_mod
    sys.modules["sqlalchemy.types"] = types_mod
    sys.modules["sqlalchemy.pool"] = pool_mod
    sys.modules["sqlalchemy.engine"] = engine_mod

if "pyodbc" not in sys.modules:
    class _DummyError(Exception):
        pass
    sys.modules["pyodbc"] = types.SimpleNamespace(
        Error=_DummyError, connect=lambda *a, **k: None
    )

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

if "dotenv" not in sys.modules:
    mod = types.ModuleType("dotenv")
    mod.load_dotenv = lambda *a, **k: None
    sys.modules["dotenv"] = mod

from etl.base_importer import BaseDBImporter


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

    assert importer._process_table_operation_row(conn, row, 1, importer.config['log_file']) is True
    assert conn.execute('SELECT COUNT(*) FROM dest').fetchone()[0] == 2
    assert conn.execute("SELECT ScopeRowCount FROM 'main.dbo.TablesToConvert_base' WHERE RowID=1").fetchone()[0] == 2


def test_progress_helpers(tmp_path):
    importer = BaseDBImporter()
    importer.progress_file = str(tmp_path / "prog.json")

    assert importer._get_progress("table_operations") == 0
    importer._update_progress("table_operations", 5)
    assert importer._get_progress("table_operations") == 5


def test_should_process_table_overrides():
    importer = BaseDBImporter()
    importer.config = {
        'include_empty_tables': False,
        'always_include_tables': ['s.t'],
    }

    assert importer._should_process_table(0, 's', 't') is True
    assert importer._should_process_table(0, 'x', 'y') is False
