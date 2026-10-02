"""Offline doubles are explicit; they never masquerade as real Parquet/Iceberg tests."""

from __future__ import annotations

from decimal import Decimal
from types import SimpleNamespace

import pytest

from crowdb_tpc_loader.models import Generated, Options, ParquetPart, TableData
from crowdb_tpc_loader.report import RunReport
from crowdb_tpc_loader.schemas import inventory
from crowdb_tpc_loader.security import Redactor


class FakeSchema(list):
    def __init__(self):
        super().__init__([SimpleNamespace(name="id", type="int64", nullable=True, metadata=None)])
        self.names = ["id"]

    def equals(self, other, check_metadata=False):
        return self.names == other.names


@pytest.fixture
def options(tmp_path):
    return Options(
        "load",
        "tpch",
        Decimal("1"),
        work_dir=tmp_path / "staging",
        catalog_uri="https://catalog.example",
        token="top-secret-credential",
        namespace=("test",),
        report_file=tmp_path / "report.json",
    )


@pytest.fixture
def redactor():
    return Redactor(["top-secret-credential"])


@pytest.fixture
def report(options, redactor, tmp_path):
    report = RunReport(options, "unit-test-run", redactor)
    report.data["generator"] = {"implementation": "explicit-test-double", "version": "0"}
    report.paths = [tmp_path / "checkpoint.json"]
    return report


@pytest.fixture
def make_data(tmp_path):
    def create(names=("region",), rows=5, root=None):
        root = root or tmp_path
        schema = FakeSchema()
        result = {}
        for name in names:
            path = root / name / "part-00000.parquet"
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_bytes(b"THIS IS TEST BYTES, NOT PARQUET:" + name.encode())
            result[name] = TableData(name, (ParquetPart(path, rows, path.stat().st_size, schema),), schema)
        return result

    return create


@pytest.fixture
def fake_generator_factory(make_data):
    class Generator:
        prepared = False
        generated = False
        closed = False

        def __init__(self, options, emit, redactor):
            self.options = options

        def prepare(self, scratch):
            self.prepared = True
            return Generated("explicit-test-double", "0")

        def generate(self, output, scratch):
            self.generated = True
            make_data(inventory(self.options.benchmark), root=output)
            return Generated("explicit-test-double", "0")

        def close(self):
            self.closed = True

    return Generator


@pytest.fixture
def fake_backend():
    from crowdb_tpc_loader.backend import Inventory, RemotePart

    class Backend:
        def __init__(self, options=None, redactor=None):
            self.options = options
            self.calls = []
            self.tables = {}
            self.data = {}
            self.states = {}
            self.existing_names = set()
            self.fail_upload_table = None
            self.commit_error = None
            self.commit_visible = True
            self.read_error = None
            self.concurrent_skip = set()

        def connect(self):
            self.calls.append("connect")

        def existing(self, names):
            self.calls.append("existing")
            return set(names) & self.existing_names

        def ensure_namespace(self):
            self.calls.append("namespace")

        def set_staging(self, scratch):
            self.calls.append("set_staging")

        def create(self, data, run_id, generator):
            self.calls.append("create:" + data.name)
            if data.name in self.concurrent_skip:
                return None
            name = data.name
            table = SimpleNamespace(
                metadata=SimpleNamespace(table_uuid=f"uuid-{name}", properties={}),
                location_provider=lambda: SimpleNamespace(
                    new_data_location=lambda filename: f"s3://unit-test/{name}/data/{filename}"
                ),
                name=lambda: ("test", name),
            )
            self.tables[name] = table
            self.data[name] = data
            self.states[name] = Inventory(None, {}, f"uuid-{name}")
            return table

        def validate_import(self, table, uris):
            self.calls.append("validate:" + table.name()[-1])
            assert all(uri.startswith("s3://") for uri in uris)

        def register(self, table, uris, run_id):
            name = table.name()[-1]
            self.calls.append("register:" + name)
            if self.commit_visible:
                self.states[name] = Inventory(
                    123,
                    {
                        uri: RemotePart(uri, part.rows, part.size_bytes)
                        for uri, part in zip(uris, self.data[name].parts)
                    },
                    f"uuid-{name}",
                )
            if self.commit_error:
                raise self.commit_error

        def reload_inventory(self, table):
            self.calls.append("reload:" + table.name()[-1])
            if self.read_error:
                raise self.read_error
            return table, self.states[table.name()[-1]]

        def definite_rejection(self, error):
            return isinstance(error, PermissionError)

    return Backend()
