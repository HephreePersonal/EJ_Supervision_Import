import sys
import types

import pytest

# Reuse DummyConn and DummyCursor from test_etl_helpers
from tests.test_etl_helpers import DummyConn, DummyCursor
from utils import etl_helpers
from db import migrations


def test_run_sql_script_idempotent(monkeypatch):
    executed = []

    class TrackCursor(DummyCursor):
        def execute(self, sql, params=None):
            if "SET LOCK_TIMEOUT" not in sql:
                executed.append(sql)
            super().execute(sql, params)

    class TrackConn(DummyConn):
        def cursor(self):
            return TrackCursor(self.fail, self.fail_sql, conn=self)

    applied = {}

    monkeypatch.setattr(etl_helpers, "ensure_version_table", lambda conn: None)
    monkeypatch.setattr(
        etl_helpers, "has_migration", lambda conn, name: applied.get(name, False)
    )
    monkeypatch.setattr(
        etl_helpers,
        "record_migration",
        lambda conn, name: applied.setdefault(name, True),
    )

    conn = TrackConn()
    etl_helpers.run_sql_script(conn, "script1", "SELECT 1;")
    assert applied.get("script1") is True
    assert executed == ["SELECT 1"]

    etl_helpers.run_sql_script(conn, "script1", "SELECT 1;")
    # No new statements should be executed on second run
    assert executed == ["SELECT 1"]


def test_has_migration_true(monkeypatch):
    calls = {}

    def fake_execute(conn, sql, params=None, fetch=False):
        calls["params"] = params
        calls["fetch"] = fetch
        return [(1,)] if fetch else None

    monkeypatch.setattr(migrations, "_execute", fake_execute)

    assert migrations.has_migration(object(), "script") is True
    assert calls["params"] == ("script",)
    assert calls["fetch"] is True


def test_has_migration_false(monkeypatch):
    monkeypatch.setattr(
        migrations, "_execute", lambda conn, sql, params=None, fetch=False: []
    )

    assert migrations.has_migration(object(), "script") is False
