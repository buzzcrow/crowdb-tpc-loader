"""Real byte streaming, with a labelled footer-parser double (NOT a Parquet acceptance test)."""
from dataclasses import replace
from pathlib import Path
from types import SimpleNamespace, ModuleType
import hashlib
import sys

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
        mutate = None
        def new_output(self, uri):
            return File(root / uri.rsplit("/",1)[1])
        def new_input(self, uri):
            path = root / uri.rsplit("/",1)[1]
            if self.mutate:
                self.mutate(path)
                self.mutate = None
            return File(path)
    return Store()


@pytest.fixture
def footer_double(monkeypatch):
    pa = ModuleType("pyarrow")
    pq = ModuleType("pyarrow.parquet")
    pa.parquet = pq
    monkeypatch.setitem(sys.modules, "pyarrow", pa)
    monkeypatch.setitem(sys.modules, "pyarrow.parquet", pq)
    return pq


def init_footer(pq, part, rows=None, schema=None):
    pq.read_metadata = lambda stream: SimpleNamespace(num_rows=part.rows if rows is None else rows,
        schema=SimpleNamespace(to_arrow_schema=lambda: schema or part.schema))


def test_exact_remote_bytes(make_data, byte_store, footer_double):
    part = make_data()["region"].parts[0]
    init_footer(footer_double, part)
    table = SimpleNamespace(io=byte_store)
    uri = "s3://remote-bucket/unique.parquet"
    digest = upload_part(table, part, uri, 7, lambda m:None)
    assert digest == hashlib.sha256(part.path.read_bytes()).hexdigest()
    with byte_store.new_input(uri).open() as stream:
        assert stream.read() == part.path.read_bytes()


def test_remote_corruption_detected(make_data, byte_store, footer_double):
    part = make_data()["region"].parts[0]
    init_footer(footer_double, part)
    byte_store.mutate = lambda path: path.write_bytes(b"X" + path.read_bytes()[1:])
    with pytest.raises(LoadError, match="checksum"):
        upload_part(SimpleNamespace(io=byte_store), part, "s3://b/x.parquet", 7, lambda m:None)


def test_remote_truncation_detected(make_data, byte_store, footer_double):
    part = make_data()["region"].parts[0]
    init_footer(footer_double, part)
    byte_store.mutate = lambda path: path.write_bytes(path.read_bytes()[:-1])
    with pytest.raises(LoadError, match="size mismatch"):
        upload_part(SimpleNamespace(io=byte_store), part, "s3://b/x.parquet", 7, lambda m:None)


def test_local_growth_detected(make_data, byte_store, footer_double):
    part = make_data()["region"].parts[0]
    init_footer(footer_double, part)
    part.path.write_bytes(part.path.read_bytes() + b"growth")
    with pytest.raises(LoadError, match="changed size"):
        upload_part(SimpleNamespace(io=byte_store), part, "s3://b/x.parquet", 7, lambda m:None)


def test_footer_difference_detected(make_data, byte_store, footer_double):
    part = make_data()["region"].parts[0]
    init_footer(footer_double, part, rows=999)
    with pytest.raises(LoadError, match="row count/schema"):
        upload_part(SimpleNamespace(io=byte_store), part, "s3://b/x.parquet", 7, lambda m:None)


def test_existing_remote_never_overwritten(make_data, byte_store, footer_double):
    part = make_data()["region"].parts[0]
    init_footer(footer_double, part)
    uri = "s3://b/x.parquet"
    with byte_store.new_output(uri).create() as stream:
        stream.write(b"original")
    with pytest.raises(LoadError, match="refusing to overwrite"):
        upload_part(SimpleNamespace(io=byte_store), part, uri, 7, lambda m:None)
    with byte_store.new_input(uri).open() as stream:
        assert stream.read() == b"original"
