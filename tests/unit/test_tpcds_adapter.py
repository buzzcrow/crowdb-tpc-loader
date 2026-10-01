"""DuckDB adapter control-flow tests with an explicit connection double, not dsdgen."""

from dataclasses import replace
from pathlib import Path
from types import SimpleNamespace
import sys

import pytest

from crowdb_tpc_loader.errors import GenerationError
from crowdb_tpc_loader.generators import tpcds
from crowdb_tpc_loader.schemas import TPCDS


class ConnectionDouble:
    def __init__(self, fail_first_load=False, missing_table=False, fail_copy=False):
        self.statements = []
        self.fail_first_load = fail_first_load
        self.missing_table = missing_table
        self.fail_copy = fail_copy
        self.closed = False
        self.row = None
        self.rows = []
        self.description = []

    def execute(self, sql):
        self.statements.append(sql)
        if sql == "LOAD tpcds" and self.fail_first_load:
            self.fail_first_load = False
            raise RuntimeError("extension absent in explicit test double")
        if "duckdb_extensions()" in sql:
            self.description = [(key,) for key in ("loaded", "extension_version", "installed_from")]
            self.row = (True, "double-version", "core")
        elif sql == "PRAGMA version":
            self.row = ("v1.5.6-test-double", "fake-build")
        elif sql == "SHOW TABLES":
            names = list(TPCDS)
            self.rows = [(name,) for name in (names[:-1] if self.missing_table else names)]
        elif sql.startswith("SELECT count(*)"):
            self.row = (0,)  # Empty tables MUST still receive a COPY statement.
        elif sql.startswith("COPY ") and self.fail_copy:
            raise OSError("test export failure")
        return self

    def fetchone(self):
        return self.row

    def fetchall(self):
        return self.rows

    def close(self):
        self.closed = True


def adapter(monkeypatch, options, redactor, connection, version="1.5.6"):
    calls = []

    def connect(path, config):
        calls.append((path, config))
        return connection

    monkeypatch.setitem(sys.modules, "duckdb", SimpleNamespace(connect=connect))
    monkeypatch.setattr(tpcds.importlib.metadata, "version", lambda name: version)
    result = tpcds.TPCDSGenerator(replace(options, benchmark="tpcds"), lambda message: None, redactor)
    return result, calls


def test_disk_backed_generator_and_all_empty_tables_exported(monkeypatch, options, redactor, tmp_path):
    connection = ConnectionDouble()
    generator, calls = adapter(monkeypatch, options, redactor, connection)
    prepared = generator.prepare(tmp_path / "scratch")
    assert Path(calls[0][0]).name == "tpcds.duckdb"
    assert calls[0][1]["memory_limit"] == options.memory_limit
    assert calls[0][1]["threads"] == str(options.threads)
    assert prepared.details["extension_version"] == "double-version"
    result = generator.generate(tmp_path / "out", tmp_path / "scratch")
    assert result.source_row_counts == dict.fromkeys(TPCDS, 0)
    assert len([sql for sql in connection.statements if sql.startswith("COPY ")]) == 24
    assert "CALL dsdgen(sf = 1)" in connection.statements
    assert connection.closed and generator.connection is None


def test_first_use_installs_only_official_extension(monkeypatch, options, redactor, tmp_path):
    connection = ConnectionDouble(fail_first_load=True)
    generator, _ = adapter(monkeypatch, options, redactor, connection)
    generator.prepare(tmp_path)
    assert connection.statements[:3] == ["LOAD tpcds", "INSTALL tpcds FROM core", "LOAD tpcds"]
    generator.close()
    generator.close()
    assert connection.closed


def test_no_download_missing_extension_closes_connection(monkeypatch, options, redactor, tmp_path):
    connection = ConnectionDouble(fail_first_load=True)
    generator, _ = adapter(monkeypatch, replace(options, no_download=True), redactor, connection)
    with pytest.raises(GenerationError, match="--no-download"):
        generator.prepare(tmp_path)
    assert connection.statements == ["LOAD tpcds"] and connection.closed


@pytest.mark.parametrize("version", ["1.3.2", "1.6.0", "2.0.0"])
def test_unverified_generator_profile_rejected(monkeypatch, options, redactor, tmp_path, version):
    generator, calls = adapter(monkeypatch, options, redactor, ConnectionDouble(), version)
    with pytest.raises(GenerationError, match="outside"):
        generator.prepare(tmp_path)
    assert not calls


def test_missing_source_table_refuses_any_export(monkeypatch, options, redactor, tmp_path):
    connection = ConnectionDouble(missing_table=True)
    generator, _ = adapter(monkeypatch, options, redactor, connection)
    generator.prepare(tmp_path / "scratch")
    with pytest.raises(GenerationError, match="inventory mismatch"):
        generator.generate(tmp_path / "out", tmp_path / "scratch")
    assert not any(sql.startswith("COPY ") for sql in connection.statements)
    assert connection.closed


def test_export_failure_is_explicit_and_closes_connection(monkeypatch, options, redactor, tmp_path):
    connection = ConnectionDouble(fail_copy=True)
    generator, _ = adapter(monkeypatch, options, redactor, connection)
    generator.prepare(tmp_path / "scratch")
    with pytest.raises(GenerationError, match="test export failure"):
        generator.generate(tmp_path / "out", tmp_path / "scratch")
    assert connection.closed
