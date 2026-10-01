"""DuckDB TPC-DS generation in an isolated, disk-backed native database."""

from __future__ import annotations

import importlib.metadata
from pathlib import Path
from typing import Callable

from ..errors import GenerationError
from ..models import Generated, Options
from ..schemas import TPCDS
from ..security import Redactor
from ..util import ensure_space, heartbeat


def sql_literal(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


class TPCDSGenerator:
    def __init__(self, options: Options, emit: Callable[[str], None], redactor: Redactor):
        self.options, self.emit, self.redactor = options, emit, redactor
        self.connection = None
        self.version = ""
        self.details: dict = {}

    def prepare(self, scratch: Path) -> Generated:
        try:
            import duckdb
            from packaging.version import Version
        except ImportError as exc:
            raise GenerationError("DuckDB is required for TPC-DS; install the package dependencies") from exc
        self.version = importlib.metadata.version("duckdb")
        if not Version("1.4") <= Version(self.version) < Version("1.6"):
            raise GenerationError(
                f"DuckDB {self.version} is outside the supported 1.4/1.5 generator profile. "
                "DuckDB 2.x changes the TPC-DS generator; do not silently mix datasets."
            )
        scratch.mkdir(parents=True, exist_ok=True)
        spill = scratch / "duckdb-spill"
        spill.mkdir(exist_ok=True)
        try:
            self.connection = duckdb.connect(
                str(scratch / "tpcds.duckdb"),
                config={
                    "memory_limit": self.options.memory_limit,
                    "threads": str(self.options.threads),
                    "temp_directory": str(spill),
                    "preserve_insertion_order": "false",
                },
            )
            try:
                self.connection.execute("LOAD tpcds")
            except Exception:
                if self.options.no_download:
                    raise GenerationError(
                        "DuckDB tpcds extension is not available locally and --no-download was specified"
                    )
                self.emit(
                    "Installing DuckDB's official tpcds extension (first use may require network access)"
                )
                self.connection.execute("INSTALL tpcds FROM core")
                self.connection.execute("LOAD tpcds")
            cursor = self.connection.execute(
                "SELECT * FROM duckdb_extensions() WHERE extension_name = 'tpcds'"
            )
            row = cursor.fetchone()
            if row is None:
                raise GenerationError("DuckDB did not report a loaded tpcds extension")
            extension = dict(zip([item[0] for item in cursor.description], row))
            if not extension.get("loaded"):
                raise GenerationError("DuckDB tpcds extension is not loaded")
            self.details = {
                "duckdb_version": self.version,
                "extension_version": extension.get("extension_version") or "not reported by DuckDB",
                "extension_source": extension.get("installed_from") or "bundled/local cache",
                "generator_profile": "DuckDB 1.x TPC-DS (not the DuckDB 2.x / TPC-DS v4 generator)",
                "memory_limit": self.options.memory_limit,
                "threads": self.options.threads,
            }
            build = self.connection.execute("PRAGMA version").fetchone()
            if build:
                self.details["duckdb_build"] = [str(value) for value in build]
        except GenerationError:
            self.close()
            raise
        except Exception as exc:
            self.close()
            raise GenerationError(
                f"DuckDB/TPC-DS extension initialization failed: {exc}. "
                "Check extension download access, the local extension cache and DuckDB version. "
                "No substitute or incomplete dataset will be emitted."
            ) from exc
        return Generated("duckdb-tpcds", self.version, self.details)

    def generate(self, output: Path, scratch: Path) -> Generated:
        if self.connection is None:
            raise GenerationError("TPC-DS generator was not initialized")
        self.emit(f"Generate TPC-DS SF={self.options.sf} using DuckDB {self.version}")
        counts: dict[str, int] = {}
        try:
            with heartbeat(self.emit, "TPC-DS dsdgen"):
                self.connection.execute(f"CALL dsdgen(sf = {self.options.sf})")
            tables = {row[0] for row in self.connection.execute("SHOW TABLES").fetchall()}
            if tables != set(TPCDS):
                raise GenerationError(
                    f"TPC-DS source inventory mismatch: missing={sorted(set(TPCDS) - tables)}, "
                    f"unexpected={sorted(tables - set(TPCDS))}"
                )
            output.mkdir(parents=True, exist_ok=True)
            for index, name in enumerate(TPCDS, 1):
                ensure_space(output, 64 * 1024 * 1024)
                counts[name] = int(self.connection.execute(f'SELECT count(*) FROM "{name}"').fetchone()[0])
                directory = output / name
                directory.mkdir(exist_ok=False)
                path = directory / "part-00000.parquet"
                self.emit(f"Export TPC-DS {index}/24: {name} ({counts[name]:,} rows)")
                with heartbeat(self.emit, f"TPC-DS export {name}"):
                    self.connection.execute(
                        f'COPY "{name}" TO {sql_literal(str(path))} '
                        "(FORMAT PARQUET, COMPRESSION ZSTD, ROW_GROUP_SIZE 122880)"
                    )
            return Generated("duckdb-tpcds", self.version, self.details, counts)
        except GenerationError:
            raise
        except Exception as exc:
            raise GenerationError(f"TPC-DS generation/export failed: {exc}; staging is retained") from exc
        finally:
            self.close()

    def close(self) -> None:
        if self.connection is not None:
            connection, self.connection = self.connection, None
            connection.close()
