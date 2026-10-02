"""Bounded streaming uploads and FileIO probe checks."""

from __future__ import annotations

import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable

from .backend import data_location
from pyiceberg.io.pyarrow import PyArrowFileIO
from .errors import CompatibilityError, LoadError
from .models import ParquetPart
from .report import RunReport
from .util import copy_stream, format_bytes, remote_uri


@dataclass(frozen=True)
class UploadResult:
    md5: str
    md5_seconds: float | None
    transfer_seconds: float


def upload_part(
    table: Any,
    part: ParquetPart,
    uri: str,
    buffer_size: int,
    emit: Callable[[str], None],
    transport=None,
) -> UploadResult:
    remote_uri(uri)
    last_progress = time.monotonic()

    def progress(total: int) -> None:
        nonlocal last_progress
        now = time.monotonic()
        if now - last_progress >= 5:
            emit(f"Upload {part.path.name}: {format_bytes(total)} / {format_bytes(part.size_bytes)}")
            last_progress = now

    md5_seconds = None
    transfer_started = time.monotonic()
    try:
        if (
            isinstance(table.io, PyArrowFileIO)
            and all(
                table.io.properties.get(key)
                for key in ("s3.endpoint", "s3.access-key-id", "s3.secret-access-key")
            )
            and uri.startswith("s3://")
        ):
            from .s3_upload import file_checksums, upload_file

            hash_started = time.monotonic()
            count, digest, part_digests = file_checksums(part.path, part.size_bytes, buffer_size)
            md5_seconds = round(time.monotonic() - hash_started, 6)
            if count != part.size_bytes:
                raise LoadError(
                    "Local Parquet file changed size after validation; uploaded object will not be registered"
                )
            transfer_started = time.monotonic()
            upload_file(
                table.io.properties,
                uri,
                part.path,
                count,
                progress,
                md5=digest,
                part_digests=part_digests,
                client=transport.bind(table.io.properties) if transport else None,
                buffer_size=buffer_size,
            )
        else:
            output = table.io.new_output(uri)
            with part.path.open("rb") as source, output.create(overwrite=False) as target:
                count, digest = copy_stream(source, target, buffer_size, progress)
    except FileExistsError as exc:
        raise LoadError("Unique target data URI unexpectedly already exists; refusing to overwrite") from exc
    if count != part.size_bytes:
        raise LoadError(
            "Local Parquet file changed size after validation; uploaded object will not be registered"
        )
    return UploadResult(digest, md5_seconds, round(time.monotonic() - transfer_started, 6))


def probe_fileio(
    backend: Any, scratch: Path, report: RunReport, buffer_size: int, emit: Callable[[str], None]
) -> None:
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
        emit("FileIO preflight: write/import-conversion probe (no table commit)")
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
            report.data["warnings"].append(
                "Uncommitted preflight probe could not be deleted; see probe_cleanup_candidates"
            )
        local.unlink(missing_ok=True)
        report.save()
    emit(
        "FileIO preflight passed. Backend free-space information is unavailable; upload errors will be reported."
    )
