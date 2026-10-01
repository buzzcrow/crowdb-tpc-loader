"""Real loopback HTTP tests. Optional PyIceberg base-class double is explicitly isolated."""

from __future__ import annotations

import importlib.util
import io
import os
import re
import sys
import threading
import types
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

import pytest

from crowdb_tpc_loader.errors import CompatibilityError


@pytest.fixture
def http_module(monkeypatch):
    try:
        import pyiceberg.io  # noqa: F401
    except ImportError:
        base = types.ModuleType("pyiceberg")
        module = types.ModuleType("pyiceberg.io")

        class FileIO:
            def __init__(self, properties):
                self.properties = properties

        class InputFile:
            def __init__(self, location):
                self.location = location

        class OutputFile:
            def __init__(self, location):
                self.location = location

        module.FileIO, module.InputFile, module.OutputFile = FileIO, InputFile, OutputFile
        arrow = types.ModuleType("pyiceberg.io.pyarrow")

        class PyArrowFileIO(FileIO):
            def _initialize_fs(self, scheme, netloc=None):
                raise NotImplementedError("unit-test Arrow filesystem base")

        arrow.PyArrowFileIO = PyArrowFileIO
        module.__path__ = []
        monkeypatch.setitem(sys.modules, "pyiceberg.io.pyarrow", arrow)
        base.io = module
        monkeypatch.setitem(sys.modules, "pyiceberg", base)
        monkeypatch.setitem(sys.modules, "pyiceberg.io", module)
    # Separate alias: never leave a dependency-stubbed production module in sys.modules.
    path = Path(__file__).resolve().parents[2] / "src/crowdb_tpc_loader/http_fileio.py"
    spec = importlib.util.spec_from_file_location("crowdb_tpc_loader._http_under_test", path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


@pytest.fixture
def server(monkeypatch):
    monkeypatch.setenv("NO_PROXY", "127.0.0.1,localhost")
    monkeypatch.setenv("no_proxy", "127.0.0.1,localhost")
    state = types.SimpleNamespace(
        objects={}, requests=[], ignore_range=False, wrong_range=False, required_token=None, redirect=False
    )

    class Handler(BaseHTTPRequestHandler):
        def log_message(self, *args):
            pass

        def begin(self):
            state.requests.append((self.command, self.path, dict(self.headers)))
            if state.required_token and self.headers.get("Authorization") != "Bearer " + state.required_token:
                self.send_error(403)
                return False
            if state.redirect:
                self.send_response(307)
                self.send_header("Location", "http://127.0.0.1:9/secret-destination")
                self.send_header("Content-Length", "0")
                self.end_headers()
                return False
            return True

        def do_HEAD(self):
            if not self.begin():
                return
            if self.path not in state.objects:
                self.send_error(404)
                return
            self.send_response(200)
            self.send_header("Content-Length", str(len(state.objects[self.path])))
            self.end_headers()

        def do_PUT(self):
            if not self.begin():
                return
            if self.headers.get("If-None-Match") == "*" and self.path in state.objects:
                self.send_error(412)
                return
            content = self.rfile.read(int(self.headers["Content-Length"]))
            state.objects[self.path] = content
            self.send_response(201)
            self.send_header("Content-Length", "0")
            self.end_headers()

        def do_GET(self):
            if not self.begin():
                return
            if self.path not in state.objects:
                self.send_error(404)
                return
            body = state.objects[self.path]
            match = re.fullmatch(r"bytes=(\d+)-(\d+)", self.headers.get("Range", ""))
            if match and not state.ignore_range:
                start, end = map(int, match.groups())
                total = len(body)
                body = body[start : end + 1]
                self.send_response(206)
                self.send_header("Content-Range", f"bytes {start}-{end}/{total + int(state.wrong_range)}")
            else:
                self.send_response(200)
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            try:
                self.wfile.write(body)
            except (BrokenPipeError, ConnectionResetError):
                pass

        def do_DELETE(self):
            if not self.begin():
                return
            if self.path not in state.objects:
                self.send_error(404)
                return
            del state.objects[self.path]
            self.send_response(204)
            self.send_header("Content-Length", "0")
            self.end_headers()

    httpd = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    httpd.daemon_threads = True
    worker = threading.Thread(target=lambda: httpd.serve_forever(poll_interval=0.01), daemon=True)
    worker.start()
    state.url = f"http://127.0.0.1:{httpd.server_address[1]}"
    yield state
    httpd.shutdown()
    httpd.server_close()
    worker.join(timeout=2)


def make_io(module, server, **properties):
    return module.HttpFileIO({"uri": server.url, "token": "test-secret", **properties})


def test_real_http_streaming_roundtrip_and_delete(http_module, server, tmp_path):
    payload = bytes(range(256)) * 12289
    fileio = make_io(http_module, server, **{"http.spool-directory": str(tmp_path)})
    server.required_token = "test-secret"
    uri = server.url + "/table/data/part.parquet"
    output = fileio.new_output(uri)
    assert not output.exists()
    with output.create() as stream:
        for i in range(0, len(payload), 8192):
            stream.write(payload[i : i + 8192])
        assert not isinstance(stream.spool, io.BytesIO)
        assert stream.tell() == len(payload)
    remote = fileio.new_input(uri)
    assert remote.exists() and len(remote) == len(payload)
    with remote.open() as stream:
        assert stream.seek(-7, os.SEEK_END) == len(payload) - 7
        assert stream.read() == payload[-7:]
        stream.seek(0)
        result = bytearray()
        while block := stream.read(32768):
            result.extend(block)
        assert bytes(result) == payload
        assert len(stream.cache) <= http_module.BLOCK
    gets = [h for method, p, h in server.requests if method == "GET"]
    assert gets and all("Range" in h for h in gets)
    assert all(h.get("Authorization") == "Bearer test-secret" for method, p, h in server.requests)
    fileio.delete(uri)
    assert not remote.exists()
    assert list(tmp_path.iterdir()) == []


def test_auth_not_sent_cross_origin(http_module, server):
    server.objects["/x"] = b"data"
    fileio = make_io(http_module, server, **{"http.auth-origins": "https://trusted.other.example"})
    assert len(fileio.new_input(server.url + "/x")) == 4
    assert all("Authorization" not in h for method, p, h in server.requests)


def test_allowlisted_origin_receives_auth(http_module, server):
    server.objects["/x"] = b"data"
    fileio = http_module.HttpFileIO(
        {"uri": "https://catalog.other", "token": "allowlisted", "http.auth-origins": server.url}
    )
    assert len(fileio.new_input(server.url + "/x")) == 4
    assert server.requests[0][2]["Authorization"] == "Bearer allowlisted"


def test_redirect_is_not_followed(http_module, server):
    server.redirect = True
    with pytest.raises(CompatibilityError, match="307"):
        make_io(http_module, server).new_input(server.url + "/x").exists()
    assert len(server.requests) == 1


def test_failed_context_does_not_upload(http_module, server):
    output = make_io(http_module, server).new_output(server.url + "/x")
    with pytest.raises(RuntimeError):
        with output.create() as stream:
            stream.write(b"partial")
            raise RuntimeError("source interrupted")
    assert "/x" not in server.objects and all(method != "PUT" for method, p, h in server.requests)


def test_no_overwrite_existing(http_module, server):
    server.objects["/x"] = b"original"
    output = make_io(http_module, server).new_output(server.url + "/x")
    with pytest.raises(FileExistsError):
        output.create()
    assert server.objects["/x"] == b"original"
    with output.create(overwrite=True) as stream:
        stream.write(b"replacement")
    assert server.objects["/x"] == b"replacement"


def test_conditional_put_guards_race(http_module, server):
    output = make_io(http_module, server).new_output(server.url + "/x")
    stream = output.create()
    stream.write(b"new")
    server.objects["/x"] = b"concurrent-original"
    with pytest.raises(FileExistsError):
        stream.close()
    assert server.objects["/x"] == b"concurrent-original"
    assert stream.closed and stream.spool.closed


def test_ignoring_large_range_refused(http_module, server):
    server.objects["/x"] = b"a" * (http_module.BLOCK + 2)
    server.ignore_range = True
    with make_io(http_module, server).new_input(server.url + "/x").open() as stream:
        with pytest.raises(CompatibilityError, match="ignored the Range"):
            stream.read(4)


def test_wrong_content_range_refused(http_module, server):
    server.objects["/x"] = b"content"
    server.wrong_range = True
    with make_io(http_module, server).new_input(server.url + "/x").open() as stream:
        with pytest.raises(CompatibilityError, match="Content-Range"):
            stream.read()


def test_seek_readinto_and_closed_semantics(http_module, server):
    server.objects["/x"] = b"abcdef"
    stream = make_io(http_module, server).new_input(server.url + "/x").open()
    target = bytearray(3)
    assert stream.readinto(target) == 3 and target == b"abc"
    assert stream.seek(-1, os.SEEK_CUR) == 2
    assert stream.read(1) == b"c"
    assert stream.seek(99) == 99 and stream.read(4) == b""
    with pytest.raises(ValueError):
        stream.seek(-1)
    stream.close()
    with pytest.raises(ValueError):
        stream.read(1)


def test_permission_error_is_actionable(http_module, server):
    server.required_token = "wrong"
    with pytest.raises(PermissionError, match="http.auth-origins") as exc:
        make_io(http_module, server).new_input(server.url + "/x").exists()
    assert "test-secret" not in str(exc.value)
