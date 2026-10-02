"""Concurrent per-table Iceberg writes with crash-safe, serialized checkpoints."""

from __future__ import annotations

import time
from concurrent.futures import ThreadPoolExecutor
from copy import deepcopy
from queue import Empty, Queue
from threading import Event
from typing import Any, Callable

from .backend import Inventory, data_location, table_uuid
from .errors import CommitUncertainError, LoadError
from .models import TableData
from .report import RunReport
from .util import format_bytes


def verify_inventory(state: Inventory, expected: dict[str, tuple[int, int]]) -> None:
    actual = set(state.files)
    if actual != set(expected):
        raise LoadError(
            f"Committed file inventory mismatch: {len(set(expected) - actual)} missing, "
            f"{len(actual - set(expected))} unexpected"
        )
    if state.snapshot_id is None:
        raise LoadError("No committed snapshot found")
    for uri, (rows, size) in expected.items():
        item = state.files[uri]
        if (item.rows, item.size_bytes) != (rows, size):
            raise LoadError(
                "Committed manifest row count/file size differs from the validated Parquet footer"
            )


def reconcile(
    backend: Any,
    table: Any,
    expected: dict[str, tuple[int, int]],
    row: dict,
    checkpoint: Callable[[dict, str | None], None],
    original_error: Exception | None,
    emit: Callable[[str], None],
    sleep: Callable[[float], None] = time.sleep,
) -> Inventory:
    state = None
    last_error = None
    for attempt in range(3):
        if attempt:
            sleep(float(attempt))
        try:
            if attempt == 0 and original_error is None and hasattr(backend, "current_inventory"):
                state = backend.current_inventory(table)
            else:
                _, state = backend.reload_inventory(table)
            verify_inventory(state, expected)
            for part in row["files"]:
                if part.get("remote_uri") in expected:
                    part["state"] = "registered"
            warning = None
            if original_error:
                warning = (
                    f"Commit response failed but registration was verified in the catalog: {original_error}"
                )
                emit(
                    "Commit response was ambiguous; the complete snapshot was found. add_files was NOT retried."
                )
            if warning:
                checkpoint(row, warning)
            return state
        except Exception as exc:
            last_error = exc
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
        checkpoint(row, None)
        raise LoadError(
            f"Files were registered but post-commit validation failed: {last_error}"
        ) from last_error
    if rejected and state is not None:
        row["status"] = "failed"
        checkpoint(row, None)
        raise LoadError(
            f"Catalog rejected the commit: {original_error}; reconciliation: {last_error}"
        ) from original_error
    row["status"] = "uncertain"
    checkpoint(row, None)
    raise CommitUncertainError(
        f"Commit outcome could not be established: {original_error or last_error}. "
        "Do not delete uncertain uploads or blindly retry add_files. Inspect the saved run ID and all snapshots."
    ) from last_error


def _load_one(
    backend: Any,
    name: str,
    data: TableData,
    row: dict,
    run_id: str,
    generator: dict,
    namespace: list[str],
    buffer_size: int,
    checkpoint: Callable[[dict, str | None], None],
    emit: Callable[[str], None],
    uploader: Callable,
) -> None:
    if row["status"] == "skipped":
        return
    started = time.monotonic()
    row.update(
        status="creating", table_identifier=[*namespace, name], table_creation_state="unknown_or_unvalidated"
    )
    checkpoint(row, None)
    try:
        emit(f"Create {name}: {data.rows:,} rows, {len(data.parts)} file(s), {format_bytes(data.size_bytes)}")
        table = backend.create(data, run_id, generator)
        if table is None:
            row.update(
                status="skipped",
                table_creation_state="existing_skipped",
                reason="table appeared concurrently; --on-exists skip left it unchanged",
            )
            checkpoint(row, None)
            return
        row.update(
            table_uuid=table_uuid(table), table_created=True, table_creation_state="created_and_validated"
        )
        expected: dict[str, tuple[int, int]] = {}
        for index, (part, item) in enumerate(zip(data.parts, row["files"])):
            uri = data_location(table, f"crowdb-tpc-{run_id}-{index:06d}.parquet")
            if uri in expected:
                raise LoadError("Catalog location provider returned duplicate data file URIs")
            expected[uri] = (part.rows, part.size_bytes)
            item.update(remote_uri=uri, state="upload_started")
            row["status"] = "uploading"
            checkpoint(row, None)  # Persist URI before touching remote storage.
            emit(f"Upload {name}: part {index + 1}/{len(data.parts)}")
            uploaded_at = time.monotonic()
            checksum = uploader(table, part, uri, buffer_size, emit)
            from .transfers import UploadResult

            if isinstance(checksum, UploadResult):
                item["md5"] = checksum.md5
                item["md5_duration_seconds"] = checksum.md5_seconds
                item["transfer_duration_seconds"] = checksum.transfer_seconds
            else:
                item["md5"] = checksum
            item["upload_duration_seconds"] = round(time.monotonic() - uploaded_at, 3)
            item["state"] = "uploaded_unregistered"
        row["status"] = "committing"
        for item in row["files"]:
            item["state"] = "commit_unknown"
        checkpoint(row, None)  # Persist uncertain state before add_files.
        emit(f"Register {name}: {len(expected)} remote Parquet file(s)")
        error = None
        try:
            backend.register(table, list(expected), run_id)
        except Exception as exc:
            error = exc
        state = reconcile(backend, table, expected, row, checkpoint, error, emit)
        row.update(
            status="succeeded",
            snapshot_id=state.snapshot_id,
            duration_seconds=round(time.monotonic() - started, 3),
        )
        checkpoint(row, None)
        emit(f"Committed {name}: snapshot {state.snapshot_id}; {data.rows:,} rows verified")
    except Exception as exc:
        if row["status"] not in {"uncertain", "failed"}:
            row["status"] = "failed"
        row["error"] = str(exc)
        row["duration_seconds"] = round(time.monotonic() - started, 3)
        checkpoint(row, None)
        if isinstance(exc, LoadError):
            raise
        raise LoadError(f"Table {name} failed: {exc}") from exc


def load_tables(backend, tables, report, buffer_size, emit, uploader=None, upload_workers=8):
    if uploader is not None:
        return _load_tables(backend, tables, report, buffer_size, emit, uploader, upload_workers)
    from .transfers import upload_part
    from .upload_client import UploadClient

    # Table workers bound both concurrent checksum passes and live S3 connections.
    workers = min(upload_workers, 8)
    timeout = getattr(getattr(backend, "options", None), "timeout", 60)
    transport = UploadClient(timeout=timeout, connections=workers)
    try:

        def upload(*args):
            return upload_part(*args, transport=transport)

        return _load_tables(backend, tables, report, buffer_size, emit, upload, workers)
    finally:
        transport.close()


def _load_tables(
    backend: Any,
    tables: dict[str, TableData],
    report: RunReport,
    buffer_size: int,
    emit: Callable[[str], None],
    uploader: Callable | None = None,
    upload_workers: int = 8,
) -> None:
    active = [(name, data) for name, data in tables.items() if report.table(name)["status"] != "skipped"]
    run_id = report.data["run_id"]
    generator = report.data["generator"]
    namespace = report.data["namespace"]

    def save(
        name: str,
        row: dict,
        warning: str | None = None,
        secrets: set[str] | None = None,
        persist: bool = True,
    ) -> None:
        if secrets:
            report.redactor.secrets.update(secrets)
        report.data["tables"][name] = deepcopy(row)
        if warning:
            report.data["warnings"].append(warning)
        if persist:
            report.save()

    if upload_workers == 1 or len(active) <= 1:
        for name, data in active:
            row = deepcopy(report.table(name))
            _load_one(
                backend,
                name,
                data,
                row,
                run_id,
                generator,
                namespace,
                buffer_size,
                lambda row, warning, name=name: save(name, row, warning),
                emit,
                uploader,
            )
        return

    # Workers own one table each. The caller owns every report mutation and fsync;
    # acknowledgement keeps each remote side effect behind its durable journal entry.
    messages: Queue = Queue()

    def work(name: str, data: TableData) -> None:
        row = deepcopy(report.table(name))
        worker_backend = backend

        def checkpoint(row: dict, warning: str | None) -> None:
            ready = Event()
            result: list[Exception] = []
            secrets = set(worker_backend.redactor.secrets) if hasattr(worker_backend, "redactor") else None
            messages.put((name, deepcopy(row), warning, secrets, ready, result))
            ready.wait()
            if result:
                raise result[0]

        try:
            if callable(getattr(backend, "fork", None)):
                worker_backend = backend.fork()
            _load_one(
                worker_backend,
                name,
                data,
                row,
                run_id,
                generator,
                namespace,
                buffer_size,
                checkpoint,
                emit,
                uploader,
            )
        except Exception as exc:
            if row["status"] == "generated":
                row.update(status="failed", error=str(exc))
                checkpoint(row, None)
            raise
        finally:
            if worker_backend is not backend and hasattr(worker_backend, "catalog"):
                worker_backend.catalog.close()

    errors = []
    with ThreadPoolExecutor(max_workers=upload_workers) as pool:
        pending = {pool.submit(work, name, data): name for name, data in active}
        while pending:
            try:
                batch = [messages.get(timeout=0.1)]
                while len(batch) < upload_workers:
                    try:
                        batch.append(messages.get_nowait())
                    except Empty:
                        break
                error = None
                try:
                    for name, row, warning, secrets, _, _ in batch:
                        save(name, row, warning, secrets, persist=False)
                    report.save()
                except Exception as exc:
                    error = exc
                finally:
                    for _, _, _, _, ready, result in batch:
                        if error is not None:
                            result.append(error)
                        ready.set()
            except Empty:
                pass
            for future in tuple(pending):
                if future.done():
                    name = pending.pop(future)
                    try:
                        future.result()
                    except Exception as exc:
                        errors.append((name, exc))
    if errors:
        name, exc = errors[0]
        if isinstance(exc, LoadError):
            raise exc
        raise LoadError(f"Table {name} failed: {exc}") from exc
