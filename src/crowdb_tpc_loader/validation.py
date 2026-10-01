"""Parquet footer validation; table data is never materialized in Python."""

from __future__ import annotations

import re
from decimal import Decimal
from pathlib import Path
from typing import Any, Callable

from .errors import ValidationError
from .models import Column, Generated, ParquetPart, TableData
from .schemas import inventory


def require_arrow():
    try:
        import pyarrow as pa
        import pyarrow.parquet as pq
    except ImportError as exc:
        raise ValidationError(
            "PyArrow is required. Install the package with its declared dependencies."
        ) from exc
    return pa, pq


def compatible_kind(dtype: Any, kind: str) -> bool:
    pa, _ = require_arrow()
    if kind == "I":
        return pa.types.is_int32(dtype) or pa.types.is_int64(dtype)
    if kind == "L":
        return pa.types.is_int64(dtype)
    if kind == "S":
        return pa.types.is_string(dtype) or pa.types.is_large_string(dtype)
    if kind == "D":
        return pa.types.is_date32(dtype)
    if kind.startswith("D") and "_" in kind:
        precision, scale = map(int, kind[1:].split("_"))
        return pa.types.is_decimal(dtype) and dtype.precision == precision and dtype.scale == scale
    raise ValueError(f"Unknown type descriptor: {kind}")


def validate_schema(schema: Any, expected: tuple[Column, ...], table: str) -> None:
    wanted = [c.name for c in expected]
    if schema.names != wanted:
        raise ValidationError(f"{table}: column names/order mismatch; expected {wanted}, got {schema.names}")
    for actual, wanted_col in zip(schema, expected):
        if not compatible_kind(actual.type, wanted_col.kind):
            raise ValidationError(
                f"{table}.{actual.name}: expected {wanted_col.kind}, got {actual.type}. "
                "No implicit numeric/date conversions are performed; inspect the generator version."
            )
        metadata = actual.metadata or {}
        if b"PARQUET:field_id" in metadata:
            raise ValidationError(
                f"{table}.{actual.name}: generator file has Parquet field IDs; this release only imports "
                "generator files without field IDs (PyIceberg add_files requirement)."
            )


def identify_table(path: Path, root: Path, names: tuple[str, ...]) -> str:
    relative = path.relative_to(root)
    directory_match = [name for name in names if name in relative.parts[:-1]]
    # Recognizes e.g. lineitem.parquet, lineitem.1.parquet, lineitem_0001.parquet.
    filename_match = [
        name for name in names if re.fullmatch(re.escape(name) + r"(?:[._-]\d+)*\.parquet", path.name)
    ]
    # "part-00000" is a generic shard filename, not the TPCH `part` table,
    # when the enclosing directory already identifies a different table.
    if len(directory_match) == 1 and re.fullmatch(r"part[._-]\d+\.parquet", path.name):
        filename_match = []
    matches = set(directory_match + filename_match)
    if len(matches) != 1:
        raise ValidationError(f"Cannot unambiguously assign Parquet file to a standard table: {relative}")
    return matches.pop()


def discover_parts(root: Path, benchmark: str) -> dict[str, list[Path]]:
    names = tuple(inventory(benchmark))
    found: dict[str, list[Path]] = {name: [] for name in names}
    resolved_root = root.resolve()
    seen: set[tuple[int, int]] = set()
    # os.walk defaults to not following symbolic-link directories; reject any explicitly.
    import os

    for base, dirs, files in os.walk(root, followlinks=False):
        for dirname in dirs:
            if (Path(base) / dirname).is_symlink():
                raise ValidationError("Generator output must not contain symbolic-link directories")
        for name in sorted(files):
            path = Path(base) / name
            if path.suffix.lower() != ".parquet":
                continue
            if path.is_symlink() or not path.resolve().is_relative_to(resolved_root):
                raise ValidationError(f"Generator output escapes its directory: {path.name}")
            stat = path.stat()
            key = (stat.st_dev, stat.st_ino)
            if stat.st_ino and key in seen:
                raise ValidationError("Duplicate/hard-linked Parquet part detected")
            seen.add(key)
            found[identify_table(path, root, names)].append(path)
    missing = [name for name, paths in found.items() if not paths]
    if missing:
        raise ValidationError(
            "Incomplete dataset; tables have no Parquet file (including empty tables): " + ", ".join(missing)
        )
    return {name: sorted(paths) for name, paths in found.items()}


def validate_tpch_counts(tables: dict[str, TableData], sf: Decimal) -> None:
    expected = {
        "region": 5,
        "nation": 25,
        "supplier": int(10_000 * sf),
        "customer": int(150_000 * sf),
        "part": int(200_000 * sf),
        "partsupp": 4 * int(200_000 * sf),
        "orders": int(1_500_000 * sf),
    }
    if sf == 1:
        expected["lineitem"] = 6_001_215
    for name, rows in expected.items():
        if tables[name].rows != rows:
            raise ValidationError(f"{name}: expected {rows:,} rows at SF={sf}, got {tables[name].rows:,}")
    if sf != 1:
        orders = tables["orders"].rows
        if not orders <= tables["lineitem"].rows <= 7 * orders:
            raise ValidationError("lineitem: row count is outside the TPC-H 1–7 line items per order bounds")


def validate_dataset(
    root: Path,
    benchmark: str,
    sf: Decimal,
    generated: Generated,
    emit: Callable[[str], None],
) -> dict[str, TableData]:
    _, pq = require_arrow()
    definitions = inventory(benchmark)
    parts = discover_parts(root, benchmark)
    result: dict[str, TableData] = {}
    for name, paths in parts.items():
        canonical = None
        validated = []
        for path in paths:
            try:
                footer = pq.read_metadata(path)
                schema = footer.schema.to_arrow_schema()
            except Exception as exc:
                raise ValidationError(f"{name}: cannot read Parquet footer for {path.name}: {exc}") from exc
            validate_schema(schema, definitions[name], name)
            if canonical is not None and not schema.equals(canonical, check_metadata=False):
                raise ValidationError(
                    f"{name}: incompatible Parquet schemas across parts; first mismatch: {path.name}"
                )
            canonical = schema
            if footer.num_rows < 0:
                raise ValidationError(f"{name}: invalid negative footer row count")
            validated.append(ParquetPart(path, footer.num_rows, path.stat().st_size, schema))
        assert canonical is not None
        table = TableData(name, tuple(validated), canonical)
        source_count = generated.source_row_counts.get(name)
        if source_count is not None and source_count != table.rows:
            raise ValidationError(
                f"{name}: source database has {source_count:,} rows, exported footer reports {table.rows:,}"
            )
        result[name] = table
        emit(f"Validate {name}: {len(validated)} file(s), {table.rows:,} rows, {table.size_bytes:,} bytes")
    if benchmark == "tpch":
        validate_tpch_counts(result, sf)
    elif set(generated.source_row_counts) != set(definitions):
        raise ValidationError("TPC-DS generator did not report source row counts for all 24 tables")
    return result
