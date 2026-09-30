"""Real byte streaming with explicit in-memory object storage."""
from types import SimpleNamespace
import hashlib

import pytest

from crowdb_tpc_loader.errors import LoadError
from crowdb_tpc_loader.transfers import upload_part


@pytest.fixture
def byte_store(tmp_path):
    root = tmp_path / "object-storage"
    root.mkdir()
    class File:
        def __init__(self, path): self.path = path
        def exists(self): return self.path.exists()
        def __len__(self): return self.path.stat().st_size
        def create(self, overwrite=False):
            self.path.parent.mkdir(parents=True, exist_ok=True)
            return self.path.open("wb" if overwrite else "xb")
        def open(self): return self.path.open("rb")
    class Store:
        def new_output(self, uri):
            return File(root / uri.rsplit("/",1)[1])
        def new_input(self, uri):
            return File(root / uri.rsplit("/",1)[1])
    return Store()


def test_exact_remote_bytes(make_data, byte_store):
    part = make_data()["region"].parts[0]
    table = SimpleNamespace(io=byte_store)
    uri = "s3://remote-bucket/unique.parquet"
    digest = upload_part(table, part, uri, 7, lambda m:None)
    assert digest == hashlib.sha256(part.path.read_bytes()).hexdigest()
    with byte_store.new_input(uri).open() as stream:
        assert stream.read() == part.path.read_bytes()


def test_upload_does_not_read_remote_object(make_data, byte_store):
    part = make_data()["region"].parts[0]
    def unexpected_read(_uri):
        raise AssertionError("upload must not read the accepted remote object")
    byte_store.new_input = unexpected_read
    upload_part(SimpleNamespace(io=byte_store), part, "s3://b/x.parquet", 7, lambda m:None)


def test_failed_upload_close_is_not_success(make_data, byte_store):
    part = make_data()["region"].parts[0]
    class FailingOutput:
        def exists(self): return False
        def create(self, overwrite=False):
            class FailedClose:
                def __enter__(self): return self
                def write(self, value): return len(value)
                def __exit__(self, *_): raise OSError("server rejected upload")
            return FailedClose()
    byte_store.new_output = lambda _uri: FailingOutput()
    with pytest.raises(OSError, match="server rejected upload"):
        upload_part(SimpleNamespace(io=byte_store), part, "s3://b/x.parquet", 7, lambda m:None)


def test_local_growth_detected(make_data, byte_store):
    part = make_data()["region"].parts[0]
    part.path.write_bytes(part.path.read_bytes() + b"growth")
    with pytest.raises(LoadError, match="changed size"):
        upload_part(SimpleNamespace(io=byte_store), part, "s3://b/x.parquet", 7, lambda m:None)


def test_existing_remote_never_overwritten(make_data, byte_store):
    part = make_data()["region"].parts[0]
    uri = "s3://b/x.parquet"
    with byte_store.new_output(uri).create() as stream:
        stream.write(b"original")
    with pytest.raises(LoadError, match="refusing to overwrite"):
        upload_part(SimpleNamespace(io=byte_store), part, uri, 7, lambda m:None)
    with byte_store.new_input(uri).open() as stream:
        assert stream.read() == b"original"
