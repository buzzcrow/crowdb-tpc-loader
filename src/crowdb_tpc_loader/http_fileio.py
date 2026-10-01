"""Optional standard HTTP FileIO (HEAD, ranged GET, conditional PUT, DELETE).

Opt-in only: --py-io-impl crowdb_tpc_loader.http_fileio.HttpFileIO.
This is NOT an assumed CrowDB-specific wire protocol. A deployment must support
these HTTP operations. Preflight verifies compatibility before generation.
Authentication is restricted to the catalog origin / explicit allowlisted origins;
redirects are refused, and TLS certificate verification remains enabled.
"""

from __future__ import annotations

import io
import os
import tempfile
import urllib.error
import urllib.request
from pathlib import Path
from typing import Any
from urllib.parse import urlsplit

from pyiceberg.io import FileIO, InputFile, OutputFile
from pyiceberg.io.pyarrow import PyArrowFileIO

from .errors import CompatibilityError
from .security import safe_uri

BLOCK = 1024 * 1024


def origin(uri: str) -> str:
    p = urlsplit(uri)
    if p.scheme not in {"http", "https"} or not p.hostname or p.username or p.password:
        raise CompatibilityError("HTTP FileIO requires an HTTP(S) location without userinfo")
    port = p.port or (443 if p.scheme == "https" else 80)
    return f"{p.scheme}://{p.hostname.lower()}:{port}"


class _NoRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, req, fp, code, msg, headers, newurl):
        return None


class _Transport:
    def __init__(self, properties: dict[str, Any]):
        self.timeout = float(properties.get("http.timeout", properties.get("rest.timeout", 60)))
        if not 0 < self.timeout < float("inf"):
            raise CompatibilityError("HTTP FileIO timeout must be positive and finite")
        self.token = properties.get("http.token", properties.get("token"))
        configured = properties.get("http.auth-origins", properties.get("uri", ""))
        self.auth_origins = {origin(value.strip()) for value in str(configured).split(",") if value.strip()}
        self.spool_directory = properties.get("http.spool-directory")
        if self.spool_directory:
            Path(self.spool_directory).mkdir(parents=True, exist_ok=True)
        self.opener = urllib.request.build_opener(_NoRedirect())

    def request(self, method: str, uri: str, headers: dict | None = None, data=None):
        requested_origin = origin(uri)
        values = {"Accept-Encoding": "identity", "User-Agent": "crowdb-tpc-loader-http/0.1"}
        values.update(headers or {})
        if self.token and requested_origin in self.auth_origins:
            values["Authorization"] = "Bearer " + str(self.token)
        request = urllib.request.Request(uri, method=method, headers=values, data=data)
        try:
            return self.opener.open(request, timeout=self.timeout)
        except urllib.error.HTTPError as exc:
            code = exc.code
            exc.close()  # Never log an upstream response body or a signed Location header.
            text = f"HTTP FileIO {method} returned {code} at {safe_uri(uri)}"
            if code == 404:
                raise FileNotFoundError(text) from None
            if code in {401, 403}:
                raise PermissionError(text + "; check FileIO credentials and http.auth-origins") from None
            if code in {409, 412}:
                raise FileExistsError(text) from None
            raise CompatibilityError(
                text + "; endpoint must support HEAD, ranged GET, conditional PUT and DELETE"
            ) from None
        except urllib.error.URLError:
            raise OSError(f"HTTP FileIO {method} could not reach {safe_uri(uri)}") from None

    def size(self, uri: str) -> int:
        with self.request("HEAD", uri) as response:
            try:
                size = int(response.headers["Content-Length"])
                if size < 0:
                    raise ValueError()
            except (KeyError, TypeError, ValueError):
                raise CompatibilityError("HTTP FileIO requires a valid Content-Length on HEAD") from None
            return size


class _RangeReader(io.RawIOBase):
    def __init__(self, transport: _Transport, uri: str, size: int):
        super().__init__()
        self.transport, self.uri, self.size = transport, uri, size
        self.position = 0
        self.cache_start = 0
        self.cache = b""

    def readable(self) -> bool:
        return True

    def seekable(self) -> bool:
        return True

    def tell(self) -> int:
        self._checkClosed()
        return self.position

    def seek(self, offset: int, whence: int = os.SEEK_SET) -> int:
        self._checkClosed()
        if whence == os.SEEK_SET:
            position = offset
        elif whence == os.SEEK_CUR:
            position = self.position + offset
        elif whence == os.SEEK_END:
            position = self.size + offset
        else:
            raise ValueError("invalid whence")
        if position < 0:
            raise ValueError("negative seek position")
        self.position = position
        return position

    def _fill(self) -> None:
        start = self.position
        end = min(start + BLOCK, self.size) - 1
        if end < start:
            self.cache = b""
            self.cache_start = start
            return
        with self.transport.request("GET", self.uri, {"Range": f"bytes={start}-{end}"}) as response:
            if response.status == 206:
                if response.headers.get("Content-Range") != f"bytes {start}-{end}/{self.size}":
                    raise CompatibilityError("HTTP FileIO received a mismatched Content-Range")
            elif not (response.status == 200 and start == 0 and end == self.size - 1):
                raise CompatibilityError(
                    "HTTP endpoint ignored the Range request; refusing a full-file memory fallback"
                )
            body = response.read(end - start + 2)
            if len(body) != end - start + 1:
                raise OSError("HTTP FileIO returned a truncated/oversized byte range")
        self.cache_start, self.cache = start, body

    def read(self, size: int = -1) -> bytes:
        self._checkClosed()
        remaining = max(0, self.size - self.position)
        wanted = remaining if size is None or size < 0 else min(size, remaining)
        chunks = []
        while wanted:
            if not self.cache_start <= self.position < self.cache_start + len(self.cache):
                self._fill()
            offset = self.position - self.cache_start
            take = min(wanted, len(self.cache) - offset)
            if take <= 0:
                raise OSError("HTTP reader made no progress")
            chunks.append(self.cache[offset : offset + take])
            self.position += take
            wanted -= take
        return b"".join(chunks)

    def readinto(self, buffer) -> int:
        data = self.read(len(buffer))
        buffer[: len(data)] = data
        return len(data)


class _UploadStream(io.BufferedIOBase):
    """Disk spool yields a known Content-Length without retaining whole files in RAM."""

    def __init__(self, transport: _Transport, uri: str, overwrite: bool):
        super().__init__()
        self.transport, self.uri, self.overwrite = transport, uri, overwrite
        self.spool = tempfile.TemporaryFile(mode="w+b", dir=transport.spool_directory)
        self.failed = False

    def writable(self) -> bool:
        return True

    def write(self, data) -> int:
        self._checkClosed()
        try:
            return self.spool.write(data)
        except BaseException:
            self.failed = True
            raise

    def tell(self) -> int:
        self._checkClosed()
        return self.spool.tell()

    def flush(self) -> None:
        if not self.spool.closed:
            self.spool.flush()

    def close(self) -> None:
        if self.closed:
            return
        try:
            if not self.failed:
                self.spool.flush()
                size = self.spool.tell()
                self.spool.seek(0)
                headers = {"Content-Type": "application/octet-stream", "Content-Length": str(size)}
                if not self.overwrite:
                    headers["If-None-Match"] = "*"
                with self.transport.request("PUT", self.uri, headers, self.spool) as response:
                    if response.status not in {200, 201, 204}:
                        raise OSError(f"Unexpected HTTP upload response: {response.status}")
        finally:
            self.spool.close()
            super().close()

    def __exit__(self, exc_type, exc_value, traceback):
        if exc_type is not None:
            self.failed = True
        self.close()
        return False


class _HttpInput(InputFile):
    def __init__(self, location: str, transport: _Transport):
        super().__init__(location)
        self.transport = transport

    def __len__(self) -> int:
        return self.transport.size(self.location)

    def exists(self) -> bool:
        try:
            len(self)
            return True
        except FileNotFoundError:
            return False

    def open(self, seekable: bool = True):
        return _RangeReader(self.transport, self.location, len(self))


class _HttpOutput(OutputFile):
    def __init__(self, location: str, transport: _Transport):
        super().__init__(location)
        self.transport = transport

    def __len__(self) -> int:
        return self.transport.size(self.location)

    def exists(self) -> bool:
        return self.to_input_file().exists()

    def create(self, overwrite: bool = False):
        if not overwrite and self.exists():
            raise FileExistsError(safe_uri(self.location))
        return _UploadStream(self.transport, self.location, overwrite)

    def to_input_file(self):
        return _HttpInput(self.location, self.transport)


class HttpFileIO(PyArrowFileIO):
    """Standard HTTP object FileIO; non-HTTP schemes delegate to PyIceberg's native I/O."""

    def __init__(self, properties=None):
        super().__init__(properties or {})
        self.transport = _Transport(self.properties)
        self._delegates: dict[str, FileIO] = {}

    def _initialize_fs(self, scheme: str, netloc: str | None = None):
        if scheme in {"http", "https"}:
            if not netloc:
                raise CompatibilityError("HTTP Arrow filesystem requires a host")
            from .arrow_http_bridge import make_filesystem

            return make_filesystem(self, scheme, netloc)
        return super()._initialize_fs(scheme, netloc)

    def _delegate(self, location: str):
        from pyiceberg.io import load_file_io

        scheme = urlsplit(location).scheme
        if scheme not in self._delegates:
            properties = {k: v for k, v in self.properties.items() if k != "py-io-impl"}
            self._delegates[scheme] = load_file_io(properties, location=location)
        return self._delegates[scheme]

    def new_input(self, location: str):
        if urlsplit(location).scheme in {"http", "https"}:
            return _HttpInput(location, self.transport)
        return self._delegate(location).new_input(location)

    def new_output(self, location: str):
        if urlsplit(location).scheme in {"http", "https"}:
            return _HttpOutput(location, self.transport)
        return self._delegate(location).new_output(location)

    def delete(self, location) -> None:
        uri = location if isinstance(location, str) else location.location
        if urlsplit(uri).scheme not in {"http", "https"}:
            self._delegate(uri).delete(uri)
            return
        with self.transport.request("DELETE", uri) as response:
            if response.status not in {200, 202, 204}:
                raise OSError(f"Unexpected HTTP delete response: {response.status}")
