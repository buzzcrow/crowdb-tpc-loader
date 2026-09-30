"""Bounded byte-for-byte durable uploads, probe checks and remote footer verification."""
from __future__ import annotations

import time
from pathlib import Path
from typing import Any, Callable

from .backend import data_location
from .errors import CompatibilityError, LoadError
from .models import ParquetPart
from .report import RunReport
from .util import copy_stream, format_bytes, hash_stream, remote_uri


def upload_part(
    table: Any, part: ParquetPart, uri: str, buffer_size: int, emit: Callable[[str], None],
) -> str:
    import pyarrow.parquet as pq

    remote_uri(uri)
    last_progress = time.monotonic()

    def progress(total: int) -> None:
        nonlocal last_progress
        now = time.monotonic()
        if now - last_progress >= 5:
            emit(f"Upload {part.path.name}: {format_bytes(total)} / {format_bytes(part.size_bytes)}")
            last_progress = now

    output = table.io.new_output(uri)
    if output.exists():
        raise LoadError("Unique target data URI unexpectedly already exists; refusing to overwrite")
    with part.path.open("rb") as source, output.create(overwrite=False) as target:
        count, digest = copy_stream(source, target, buffer_size, progress)
    if count != part.size_bytes:
        raise LoadError("Local Parquet file changed size after validation; uploaded object will not be registered")
    remote = table.io.new_input(uri)
    if len(remote) != part.size_bytes:
        raise LoadError(f"Remote upload size mismatch for {part.path.name}")
    # A full streaming checksum also catches corruption outside the Parquet footer.
    emit(f"Verify upload {part.path.name}: size, streaming SHA-256 and Parquet footer")
    with remote.open() as stream:
        remote_count, remote_digest = hash_stream(stream, buffer_size)
    if remote_count != count or remote_digest != digest:
        raise LoadError(f"Remote upload content checksum mismatch for {part.path.name}")
    with remote.open() as stream:
        footer = pq.read_metadata(stream)
    schema = footer.schema.to_arrow_schema()
    if footer.num_rows != part.rows or not schema.equals(part.schema, check_metadata=False):
        raise LoadError(f"Remote Parquet row count/schema differs from the validated local part: {part.path.name}")
    return digest


def probe_fileio(backend: Any, scratch: Path, report: RunReport, buffer_size: int,
                 emit: Callable[[str], None]) -> None:
    import pyarrow as pa
    import pyarrow.parquet as pq

    probe_table = backend.stage_probe(report.data["run_id"])
    local = scratch / "fileio-probe.parquet"
    probe_schema = pa.schema([pa.field("probe", pa.int64(), nullable=True)])
    pq.write_table(pa.Table.from_pydict({"probe": [1]}, schema=probe_schema), local)
    part = ParquetPart(local, 1, local.stat().st_size, probe_schema)
    uri = data_location(probe_table, f"crowdb-tpc-probe-{report.data['run_id']}.parquet")
    record = {"uri": uri, "state": "upload_started", "uncommitted_stage": True}
    report.data["probes"].append(record)
    report.save()
    try:
        emit("FileIO preflight: write/read/checksum/footer/import-conversion probe (no table commit)")
        upload_part(probe_table, part, uri, buffer_size, emit)
        backend.validate_import(probe_table, [uri])
        record["state"] = "verified"
    except Exception as exc:
        raise CompatibilityError(
            f"Remote FileIO preflight failed: {exc}. REST access alone is insufficient; check storage "
            "credentials, returned storage endpoints and writable FileIO support. No benchmark table was committed."
        ) from exc
    finally:
        # This unique probe has NEVER been passed to add_files / committed; it cannot be snapshot data.
        try:
            probe_table.io.delete(uri)
            record["state"] = "deleted"
        except FileNotFoundError:
            record["state"] = "deleted"
        except Exception as exc:
            record["cleanup_error"] = str(exc)
            report.data["warnings"].append("Uncommitted preflight probe could not be deleted; see probe_cleanup_candidates")
        local.unlink(missing_ok=True)
        report.save()
    emit("FileIO preflight passed. Backend free-space information is unavailable; upload errors will be reported.")
