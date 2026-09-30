"""Filesystem ownership, bounded streaming and resource preflight."""
from __future__ import annotations

import hashlib
import importlib.metadata
import os
import shutil
import tempfile
import time
from contextlib import contextmanager
from decimal import Decimal
from pathlib import Path
from threading import Event, Thread
from typing import BinaryIO, Callable, Iterator
from urllib.parse import urlsplit

from .errors import ArgumentError, CompatibilityError, ResourceError

MIB = 1024 ** 2
GIB = 1024 ** 3


def format_bytes(size: int) -> str:
    try:
        number = float(size)
    except OverflowError:
        number = Decimal(size)
    for unit in ("B", "KiB", "MiB", "GiB", "TiB", "PiB"):
        if number < 1024 or unit == "PiB":
            return f"{number:.1f} {unit}"
        number /= 1024
    raise AssertionError("unreachable")


def versions() -> dict[str, str]:
    import platform
    from . import __version__

    result = {"crowdb-tpc-loader": __version__, "python": platform.python_version()}
    for name in ("pyarrow", "pyiceberg", "duckdb", "tpchgen-cli", "packaging"):
        try:
            result[name] = importlib.metadata.version(name)
        except importlib.metadata.PackageNotFoundError:
            result[name] = "not installed"
    return result


def writable_directory(path: Path) -> None:
    try:
        path.mkdir(parents=True, exist_ok=True)
        with tempfile.NamedTemporaryFile(prefix=".crowdb-write-test-", dir=path) as stream:
            stream.write(b"ok")
            stream.flush()
            os.fsync(stream.fileno())
    except OSError as exc:
        raise ResourceError(f"Staging directory is not writable: {path}: {exc}") from exc


def estimated_disk_bytes(benchmark: str, sf: Decimal) -> int:
    """Conservative staging estimate, NOT a guarantee of generator/backend disk use.

    Includes uncompressed row buffers, DuckDB database + Parquet + spill for TPC-DS.
    Fixed-size TPC-DS dimensions matter even at fractional scale factors.
    """
    per_sf = Decimal(3 if benchmark == "tpch" else 8) * GIB
    fixed = 256 * MIB if benchmark == "tpch" else 2 * GIB
    return int(sf * per_sf) + fixed


def ensure_space(path: Path, required: int) -> int:
    try:
        free = shutil.disk_usage(path).free
    except OSError as exc:
        raise ResourceError(f"Cannot inspect free staging space at {path}: {exc}") from exc
    if free < required:
        raise ResourceError(
            f"Insufficient local space at {path}: {format_bytes(free)} free; "
            f"estimated requirement {format_bytes(required)}. Choose a larger --work-dir/--output-dir "
            "or a smaller --sf. The estimate includes staging overhead."
        )
    return free


def prepare_generate_directory(path: Path) -> Path:
    path = path.expanduser().absolute()
    if path.is_symlink():
        raise ArgumentError("--output-dir must not be a symbolic link")
    if path.exists() and (not path.is_dir() or any(path.iterdir())):
        raise ArgumentError(f"--output-dir must be absent or empty; refusing to overwrite: {path}")
    writable_directory(path)
    return path.resolve()


def create_work_directory(parent: Path | None, run_id: str) -> Path:
    if parent is not None:
        parent = parent.expanduser().absolute()
        writable_directory(parent)
    try:
        # --work-dir is a staging PARENT. We only ever remove our isolated child.
        return Path(tempfile.mkdtemp(prefix=f"crowdb-tpc-{run_id}-", dir=parent)).resolve()
    except OSError as exc:
        raise ResourceError(f"Cannot create isolated staging directory: {exc}") from exc


def mark_owned(path: Path, run_id: str) -> None:
    marker = path / ".crowdb-tpc-owned"
    with marker.open("x", encoding="utf-8") as stream:
        stream.write(run_id)


def cleanup_owned(path: Path, run_id: str) -> None:
    """Deletion is local-only and restricted to the exact directory owned by this run."""
    if path.is_symlink() or not path.is_dir():
        raise ResourceError("Refusing staging cleanup: path is not an owned regular directory")
    marker = path / ".crowdb-tpc-owned"
    try:
        valid = not marker.is_symlink() and marker.read_text(encoding="utf-8") == run_id
    except OSError:
        valid = False
    if not valid:
        raise ResourceError("Refusing staging cleanup: ownership marker is missing or changed")
    shutil.rmtree(path)


@contextmanager
def directory_lock(directory: Path) -> Iterator[None]:
    lock = directory / ".crowdb-tpc.lock"
    try:
        fd = os.open(lock, os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o600)
    except FileExistsError as exc:
        raise ResourceError(f"Another run owns {directory}; lock exists: {lock}") from exc
    try:
        os.write(fd, str(os.getpid()).encode())
        os.close(fd)
        yield
    finally:
        lock.unlink(missing_ok=True)


def remote_uri(uri: str) -> str:
    """Reject paths that would expire or refer to the loader's local filesystem."""
    try:
        parsed = urlsplit(uri)
        _ = parsed.port  # Validate malformed ports too.
    except ValueError as exc:
        raise CompatibilityError("Catalog returned an invalid data location") from exc
    if not parsed.scheme or parsed.scheme.lower() in {"file", "local", "memory"}:
        raise CompatibilityError(
            "Catalog returned a local/non-durable data location. Refusing to put local staging paths "
            "in Iceberg manifests. Configure a remotely writable FileIO and durable table location."
        )
    if not parsed.netloc or not parsed.path or parsed.path == "/":
        raise CompatibilityError("Catalog must return a fully qualified, durable remote data URI")
    if parsed.query or parsed.fragment or parsed.username or parsed.password:
        raise CompatibilityError(
            "Data locations must be stable URIs without credentials, signed query parameters or fragments. "
            "Configure authentication in FileIO, not in persistent manifest paths."
        )
    return uri


def copy_stream(
    source: BinaryIO,
    target: BinaryIO,
    buffer_size: int,
    progress: Callable[[int], None] | None = None,
) -> tuple[int, str]:
    if buffer_size <= 0:
        raise ValueError("buffer_size must be positive")
    total = 0
    digest = hashlib.sha256()
    while chunk := source.read(buffer_size):
        digest.update(chunk)
        view = memoryview(chunk)
        while view:
            written = target.write(view)
            # Buffered native FileIO streams may report None for a successful full write.
            if written is None:
                written = len(view)
            if not 0 < written <= len(view):
                raise OSError("FileIO made no progress or returned an invalid write length")
            view = view[written:]
        total += len(chunk)
        if progress:
            progress(total)
    return total, digest.hexdigest()


def hash_stream(source: BinaryIO, buffer_size: int) -> tuple[int, str]:
    if buffer_size <= 0:
        raise ValueError("buffer_size must be positive")
    digest = hashlib.sha256()
    total = 0
    while chunk := source.read(buffer_size):
        digest.update(chunk)
        total += len(chunk)
    return total, digest.hexdigest()


@contextmanager
def heartbeat(emit: Callable[[str], None], label: str, interval: float = 10.0) -> Iterator[None]:
    """Keep long native generator calls observable without materializing query results."""
    done = Event()
    started = time.monotonic()

    def pulse() -> None:
        while not done.wait(interval):
            emit(f"{label}: still running ({time.monotonic() - started:.0f}s elapsed)")

    worker = Thread(target=pulse, daemon=True)
    worker.start()
    try:
        yield
    finally:
        done.set()
        worker.join()
