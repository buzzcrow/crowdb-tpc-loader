"""Install ONLY a compatible, hash-verified published tpchgen binary wheel.

No cargo, Rust build, package installer, shell, global environment mutation or
source-distribution fallback. The native binary is cached in a user-owned folder.
"""
from __future__ import annotations

import hashlib
import json
import os
import platform
import shutil
import stat
import tempfile
import urllib.request
import zipfile
from pathlib import Path
from typing import Any, Callable
from urllib.parse import urlsplit

from ..errors import GenerationError

PINNED_TPCHGEN = "3.0.0"
MAX_WHEEL_BYTES = 100 * 1024 * 1024
MAX_BINARY_BYTES = 150 * 1024 * 1024


def cache_root() -> Path:
    if configured := os.getenv("CROWDB_TPC_CACHE"):
        return Path(configured).expanduser()
    if os.name == "nt":
        return Path(os.getenv("LOCALAPPDATA", str(Path.home() / "AppData/Local"))) / "crowdb-tpc-loader"
    return Path(os.getenv("XDG_CACHE_HOME", str(Path.home() / ".cache"))) / "crowdb-tpc-loader"


def choose_wheel(files: list[dict[str, Any]], compatible_tags=None) -> dict[str, Any]:
    try:
        from packaging.tags import sys_tags
        from packaging.utils import parse_wheel_filename
    except ImportError as exc:
        raise GenerationError("The packaging dependency is required to select a binary wheel") from exc
    priority = {tag: index for index, tag in enumerate(compatible_tags or sys_tags())}
    candidates = []
    for item in files:
        if item.get("packagetype") != "bdist_wheel" or item.get("yanked", False):
            continue
        try:
            distribution, version, _, tags = parse_wheel_filename(item["filename"])
        except (ValueError, KeyError):
            continue
        if str(distribution).replace("_", "-") != "tpchgen-cli" or str(version) != PINNED_TPCHGEN:
            continue
        ranks = [priority[tag] for tag in tags if tag in priority]
        if ranks:
            candidates.append((min(ranks), item["filename"], item))
    if not candidates:
        raise GenerationError(
            f"No published tpchgen-cli {PINNED_TPCHGEN} wheel supports {platform.system()} "
            f"{platform.machine()}. No source build will be attempted. Use a supported platform or "
            "provide a compatible prebuilt executable with --tpchgen."
        )
    return min(candidates, key=lambda candidate: (candidate[0], candidate[1]))[2]


def _request(url: str, host: str, timeout: float):
    parsed = urlsplit(url)
    if parsed.scheme != "https" or parsed.hostname != host or parsed.username or parsed.password:
        raise GenerationError("Binary distribution metadata contains an untrusted download URL")
    request = urllib.request.Request(url, headers={"User-Agent": "crowdb-tpc-loader/0.1"})
    response = urllib.request.urlopen(request, timeout=timeout)
    final = urlsplit(response.geturl())
    if final.scheme != "https" or final.hostname != host:
        response.close()
        raise GenerationError("Binary download redirected to an unexpected host")
    return response


def extract_binary(wheel: Path, destination: Path) -> None:
    filename = "tpchgen-cli.exe" if os.name == "nt" else "tpchgen-cli"
    with zipfile.ZipFile(wheel) as archive:
        matches = [info for info in archive.infolist()
                   if info.filename.endswith(".data/scripts/" + filename)
                   or (".data/scripts/" in info.filename and Path(info.filename).name == filename)]
        if len(matches) != 1:
            raise GenerationError("Published wheel does not contain exactly one expected tpchgen executable")
        info = matches[0]
        if info.file_size > MAX_BINARY_BYTES or stat.S_ISLNK(info.external_attr >> 16):
            raise GenerationError("Invalid native executable in binary wheel")
        # Never extract arbitrary archive paths or execute a wheel's Python code.
        with archive.open(info) as source, destination.open("xb") as output:
            shutil.copyfileobj(source, output, length=1024 * 1024)
    destination.chmod(0o700)


def get_binary(no_download: bool, timeout: float, emit: Callable[[str], None]) -> tuple[Path, dict[str, Any]]:
    root = cache_root() / "tpchgen-cli" / PINNED_TPCHGEN / f"{platform.system()}-{platform.machine()}"
    binary = root / ("tpchgen-cli.exe" if os.name == "nt" else "tpchgen-cli")
    receipt = root / "receipt.json"
    if binary.is_file() and receipt.is_file() and not binary.is_symlink():
        try:
            data = json.loads(receipt.read_text(encoding="utf-8"))
            with binary.open("rb") as stream:
                actual = hashlib.file_digest(stream, "sha256").hexdigest() if hasattr(hashlib, "file_digest") else _digest(stream)
            if data["binary_sha256"] == actual and data["version"] == PINNED_TPCHGEN:
                return binary, data
        except (OSError, ValueError, KeyError):
            pass
        raise GenerationError(f"Cached tpchgen binary failed integrity validation; remove this cache directory: {root}")
    if no_download:
        raise GenerationError("tpchgen-cli is missing and --no-download was specified; install a prebuilt binary or use --tpchgen")
    emit(f"Preparing tpchgen-cli {PINNED_TPCHGEN}: downloading a compatible binary wheel (no Rust build)")
    try:
        with _request(f"https://pypi.org/pypi/tpchgen-cli/{PINNED_TPCHGEN}/json", "pypi.org", timeout) as response:
            raw = response.read(8 * 1024 * 1024 + 1)
            if len(raw) > 8 * 1024 * 1024:
                raise GenerationError("Unexpectedly large package metadata response")
            metadata = json.loads(raw)
        item = choose_wheel(metadata["urls"])
        if item.get("size", 0) > MAX_WHEEL_BYTES:
            raise GenerationError("Binary wheel is unexpectedly large")
        expected_digest = item["digests"]["sha256"]
        root.parent.mkdir(parents=True, exist_ok=True)
        with tempfile.TemporaryDirectory(prefix=".download-", dir=root.parent) as temp:
            temporary = Path(temp)
            wheel = temporary / "distribution.whl"
            digest = hashlib.sha256()
            size = 0
            with _request(item["url"], "files.pythonhosted.org", timeout) as response, wheel.open("xb") as output:
                while chunk := response.read(1024 * 1024):
                    size += len(chunk)
                    if size > MAX_WHEEL_BYTES:
                        raise GenerationError("Binary wheel exceeded the download size limit")
                    digest.update(chunk)
                    output.write(chunk)
            if digest.hexdigest() != expected_digest or (item.get("size") and size != item["size"]):
                raise GenerationError("tpchgen binary wheel failed the published SHA-256/size check")
            executable = temporary / binary.name
            extract_binary(wheel, executable)
            with executable.open("rb") as stream:
                binary_sha = _digest(stream)
            info = {"version": PINNED_TPCHGEN, "wheel": item["filename"], "wheel_sha256": expected_digest,
                    "binary_sha256": binary_sha, "source": "PyPI binary wheel"}
            ready = temporary / "ready"
            ready.mkdir()
            os.replace(executable, ready / binary.name)
            (ready / "receipt.json").write_text(json.dumps(info, indent=2), encoding="utf-8")
            try:
                os.rename(ready, root)
            except OSError:
                if not (binary.is_file() and receipt.is_file()):
                    raise
                # Another process may have atomically populated the same cache. Verify it.
                return get_binary(True, timeout, emit)
            return binary, info
    except GenerationError:
        raise
    except Exception as exc:
        raise GenerationError(
            f"Unable to obtain the published tpchgen-cli binary wheel: {exc}. "
            f"On a networked machine use: python -m pip install --only-binary=:all: tpchgen-cli=={PINNED_TPCHGEN}; "
            "or supply --tpchgen /path/to/a/prebuilt/tpchgen-cli. No source build was attempted."
        ) from exc


def _digest(stream) -> str:
    digest = hashlib.sha256()
    while chunk := stream.read(1024 * 1024):
        digest.update(chunk)
    return digest.hexdigest()
