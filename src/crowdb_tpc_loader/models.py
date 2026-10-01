"""Small data objects. No runtime dependencies are needed to import them."""

from __future__ import annotations

from dataclasses import dataclass, field
from decimal import Decimal
from pathlib import Path
from typing import Any


@dataclass(frozen=True)
class Column:
    name: str
    kind: str
    nullable: bool = True


@dataclass(frozen=True)
class ParquetPart:
    path: Path
    rows: int
    size_bytes: int
    schema: Any = field(repr=False)


@dataclass(frozen=True)
class TableData:
    name: str
    parts: tuple[ParquetPart, ...]
    schema: Any = field(repr=False)

    @property
    def rows(self) -> int:
        return sum(part.rows for part in self.parts)

    @property
    def size_bytes(self) -> int:
        return sum(part.size_bytes for part in self.parts)


@dataclass(frozen=True)
class Generated:
    implementation: str
    version: str
    details: dict[str, Any] = field(default_factory=dict)
    source_row_counts: dict[str, int] = field(default_factory=dict)


@dataclass(frozen=True)
class Options:
    command: str
    benchmark: str
    sf: Decimal
    output_dir: Path | None = None
    work_dir: Path | None = None
    keep_files: bool = False
    catalog_uri: str | None = None
    token: str | None = field(default=None, repr=False)
    namespace: tuple[str, ...] = ()
    on_exists: str = "error"
    report_file: Path | None = None
    tpchgen: Path | None = None
    no_download: bool = False
    memory_limit: str = "1GB"
    threads: int = 2
    upload_buffer_mib: int = 8
    upload_workers: int = 8
    timeout: float = 60.0
    catalog_properties: dict[str, str] = field(default_factory=dict, repr=False)
    quiet: bool = False
