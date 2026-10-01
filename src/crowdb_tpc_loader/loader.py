"""Bounded file uploads with ordered, uncertainty-aware table commits."""
from __future__ import annotations

import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Any, Callable

from .backend import Inventory, data_location, table_uuid
from .errors import CommitUncertainError, LoadError
from .models import TableData
from .report import RunReport
from .transfers import upload_part
from .util import format_bytes


def verify_inventory(state: Inventory, expected: dict[str, tuple[int, int]]) -> None:
    actual = set(state.files)
    if actual != set(expected):
        raise LoadError(f"Committed file inventory mismatch: {len(set(expected) - actual)} missing, "
                        f"{len(actual - set(expected))} unexpected")
    if state.snapshot_id is None:
        raise LoadError("No committed snapshot found")
    for uri, (rows, size) in expected.items():
        item = state.files[uri]
        if (item.rows, item.size_bytes) != (rows, size):
            raise LoadError("Committed manifest row count/file size differs from the validated Parquet footer")


def _timed_upload(uploader: Callable, table: Any, part: Any, uri: str,
                  buffer_size: int, emit: Callable[[str], None]) -> tuple[str, float]:
    started = time.monotonic()
    digest = uploader(table, part, uri, buffer_size, emit)
    return digest, round(time.monotonic() - started, 3)


def reconcile(
    backend: Any, table: Any, expected: dict[str, tuple[int, int]], row: dict,
    report: RunReport, original_error: Exception | None, emit: Callable[[str], None],
    sleep: Callable[[float], None] = time.sleep,
) -> Inventory:
    state = None
    last_error = None
    for attempt in range(3):
        if attempt:
            sleep(float(attempt))
        try:
            _, state = backend.reload_inventory(table)
            verify_inventory(state, expected)
            for part in row["files"]:
                if part.get("remote_uri") in expected:
                    part["state"] = "registered"
            if original_error:
                report.data["warnings"].append(
                    f"Commit response failed but registration was verified in the catalog: {original_error}"
                )
                emit("Commit response was ambiguous; the complete snapshot was found. add_files was NOT retried.")
            return state
        except Exception as exc:
            last_error = exc
    # Mark only positively observed registrations as registered. Never classify unknown as safe to delete.
    known = state.files if state is not None else {}
    rejected = original_error is not None and backend.definite_rejection(original_error)
    for part in row["files"]:
        uri = part.get("remote_uri")
        if uri in known:
            part["state"] = "registered"
        elif rejected and state is not None:
            part["state"] = "uploaded_unregistered"
        else:
            part["state"] = "commit_unknown"
    if state is not None and set(expected).issubset(known):
        row["status"] = "failed"
        raise LoadError(f"Files were registered but post-commit validation failed: {last_error}") from last_error
    if rejected and state is not None:
        row["status"] = "failed"
        raise LoadError(f"Catalog rejected the commit: {original_error}; reconciliation: {last_error}") from original_error
    row["status"] = "uncertain"
    raise CommitUncertainError(
        f"Commit outcome could not be established: {original_error or last_error}. "
        "Do not delete uncertain uploads or blindly retry add_files. Inspect the saved run ID and all snapshots."
    ) from last_error


def load_tables(
    backend: Any, tables: dict[str, TableData], report: RunReport, buffer_size: int,
    emit: Callable[[str], None], uploader: Callable = upload_part, upload_workers: int = 1,
) -> None:
    if upload_workers > 1:
        _load_parallel(backend, tables, report, buffer_size, emit, uploader, upload_workers)
        return
    run_id = report.data["run_id"]
    for name, data in tables.items():
        row = report.table(name)
        if row["status"] == "skipped":
            continue
        started = time.monotonic()
        row["status"] = "creating"
        row["table_identifier"] = [*report.data["namespace"], name]
        row["table_creation_state"] = "unknown_or_unvalidated"
        report.save()
        try:
            emit(f"Create {name}: {data.rows:,} rows, {len(data.parts)} file(s), {format_bytes(data.size_bytes)}")
            table = backend.create(data, run_id, report.data["generator"])
            if table is None:
                row["status"] = "skipped"
                row["table_creation_state"] = "existing_skipped"
                row["reason"] = "table appeared concurrently; --on-exists skip left it unchanged"
                report.save()
                continue
            row["table_uuid"] = table_uuid(table)
            row["table_created"] = True
            row["table_creation_state"] = "created_and_validated"
            report.save()
            expected: dict[str, tuple[int, int]] = {}
            for index, (part, item) in enumerate(zip(data.parts, row["files"])):
                uri = data_location(table, f"crowdb-tpc-{run_id}-{index:06d}.parquet")
                if uri in expected:
                    raise LoadError("Catalog location provider returned duplicate data file URIs")
                expected[uri] = (part.rows, part.size_bytes)
                item.update({"remote_uri": uri, "state": "upload_started"})
                row["status"] = "uploading"
                # Crash-safe journal entry BEFORE remotely creating any object.
                report.save()
                emit(f"Upload {name}: part {index + 1}/{len(data.parts)}")
                item["sha256"], item["upload_duration_seconds"] = _timed_upload(
                    uploader, table, part, uri, buffer_size, emit,
                )
                item["state"] = "uploaded_unregistered"
                report.save()
            _commit_table(backend, table, expected, row, report, run_id, name, data.rows, started, emit)
        except Exception as exc:
            if row["status"] not in {"uncertain", "failed"}:
                row["status"] = "failed"
            row["error"] = str(exc)
            row["duration_seconds"] = round(time.monotonic() - started, 3)
            report.save()
            if isinstance(exc, LoadError):
                raise
            raise LoadError(f"Table {name} failed; no further tables were loaded: {exc}") from exc


def _commit_table(backend: Any, table: Any, expected: dict[str, tuple[int, int]], row: dict,
                  report: RunReport, run_id: str, name: str, rows: int, started: float,
                  emit: Callable[[str], None]) -> None:
    uris = list(expected)
    table = backend.assert_empty(table)
    row["status"] = "committing"
    for item in row["files"]:
        item["state"] = "commit_unknown"
    report.save()
    emit(f"Register {name}: {len(uris)} remote Parquet file(s)")
    error = None
    try:
        backend.register(table, uris, run_id)
    except Exception as exc:
        error = exc
    state = reconcile(backend, table, expected, row, report, error, emit)
    row.update({"status": "succeeded", "snapshot_id": state.snapshot_id,
                "duration_seconds": round(time.monotonic() - started, 3)})
    report.save()
    emit(f"Committed {name}: snapshot {state.snapshot_id}; {rows:,} rows verified")


def _load_parallel(backend: Any, tables: dict[str, TableData], report: RunReport,
                   buffer_size: int, emit: Callable[[str], None], uploader: Callable,
                   workers: int) -> None:
    run_id = report.data["run_id"]
    prepared = []
    futures = {}
    first_error = None
    with ThreadPoolExecutor(max_workers=workers) as pool:
        for name, data in tables.items():
            row = report.table(name)
            if row["status"] == "skipped":
                continue
            started = time.monotonic()
            row.update(status="creating", table_identifier=[*report.data["namespace"], name],
                       table_creation_state="unknown_or_unvalidated")
            report.save()
            try:
                emit(f"Create {name}: {data.rows:,} rows, {len(data.parts)} file(s), {format_bytes(data.size_bytes)}")
                table = backend.create(data, run_id, report.data["generator"])
                if table is None:
                    row.update(status="skipped", table_creation_state="existing_skipped",
                               reason="table appeared concurrently; --on-exists skip left it unchanged")
                    report.save()
                    continue
                row.update(table_uuid=table_uuid(table), table_created=True,
                           table_creation_state="created_and_validated", status="uploading")
                expected = {}
                for index, (part, item) in enumerate(zip(data.parts, row["files"])):
                    uri = data_location(table, f"crowdb-tpc-{run_id}-{index:06d}.parquet")
                    if uri in expected:
                        raise LoadError("Catalog location provider returned duplicate data file URIs")
                    expected[uri] = (part.rows, part.size_bytes)
                    item.update(remote_uri=uri, state="upload_started")
                    report.save()  # Journal the remote URI before scheduling its upload.
                    emit(f"Upload {name}: part {index + 1}/{len(data.parts)}")
                    futures[pool.submit(_timed_upload, uploader, table, part, uri, buffer_size, emit)] = (
                        row, item, name,
                    )
                prepared.append((name, data, table, row, expected, started))
            except Exception as exc:
                row.update(status="failed", error=str(exc),
                           duration_seconds=round(time.monotonic() - started, 3))
                report.save()
                first_error = first_error or (name, exc)
                break
        for future in as_completed(futures):
            row, item, name = futures[future]
            try:
                item["sha256"], item["upload_duration_seconds"] = future.result()
                item["state"] = "uploaded_unregistered"
            except Exception as exc:
                row.update(status="failed", error=str(exc))
                first_error = first_error or (name, exc)
            report.save()
    if first_error:
        for name, _, _, row, _, started in prepared:
            if row["status"] == "uploading":
                row.update(status="failed", error=f"Commit skipped after upload failure in {first_error[0]}",
                           duration_seconds=round(time.monotonic() - started, 3))
        report.save()
        raise LoadError(f"Table {first_error[0]} failed; no table in this batch was committed: {first_error[1]}")
    for index, (name, data, table, row, expected, started) in enumerate(prepared):
        try:
            _commit_table(backend, table, expected, row, report, run_id, name, data.rows, started, emit)
        except Exception as exc:
            if row["status"] not in {"uncertain", "failed"}:
                row["status"] = "failed"
            row["error"] = str(exc)
            row["duration_seconds"] = round(time.monotonic() - started, 3)
            for later_name, _, _, later_row, _, later_started in prepared[index + 1:]:
                later_row.update(status="failed", error=f"Commit skipped after failure in {name}",
                                 duration_seconds=round(time.monotonic() - later_started, 3))
            report.save()
            if isinstance(exc, LoadError):
                raise
            raise LoadError(f"Table {name} failed; later tables were not committed: {exc}") from exc
