"""Real PyIceberg + real Parquet + real HTTP durable read after local deletion.

Uses a temporary SQLite catalog, NOT CrowDB. The independent catalog reload and
scan exercise actual PyIceberg APIs. Runtime dependencies are mandatory.
"""
from dataclasses import replace
from pathlib import Path
import shutil

import pytest

pa = pytest.importorskip("pyarrow")
pq = pytest.importorskip("pyarrow.parquet")
pytest.importorskip("pyiceberg")
pytest.importorskip("sqlalchemy")

from pyiceberg.catalog.sql import SqlCatalog
from crowdb_tpc_loader.backend import IcebergBackend, inspect_inventory
from crowdb_tpc_loader.loader import load_tables
from crowdb_tpc_loader.models import ParquetPart, TableData
from crowdb_tpc_loader.schemas import arrow_schema
from crowdb_tpc_loader.transfers import probe_fileio
from tests.unit.test_http_fileio import server  # noqa: F401 -- shared real HTTP fixture

pytestmark = pytest.mark.integration


def test_real_registration_and_independent_http_scan(options, report, redactor, tmp_path, server):
    properties = {"uri": f"sqlite:///{tmp_path / 'catalog.sqlite'}", "warehouse": server.url + "/warehouse",
                  "py-io-impl": "crowdb_tpc_loader.http_fileio.HttpFileIO",
                  "http.auth-origins":server.url, "http.token":"integration-token",
                  "http.spool-directory": str(tmp_path / "spool")}
    server.required_token = "integration-token"
    catalog = SqlCatalog("http_test", **properties)
    backend = IcebergBackend(options, redactor)
    backend.catalog = catalog
    backend.ensure_namespace()
    scratch = tmp_path / "scratch"
    scratch.mkdir()
    probe_fileio(backend, scratch, report, 65536, lambda m:None)
    assert not catalog.list_tables(options.namespace), "preflight must not publish a table"
    schema = arrow_schema("tpch", "region")
    source = pa.Table.from_pydict({"r_regionkey":[0,1,2,3,4], "r_name":["A","B","C","D","E"],
                                  "r_comment":["test"]*5}, schema=schema)
    local = tmp_path / "local"
    local.mkdir()
    path = local / "region.parquet"
    pq.write_table(source, path)
    data = TableData("region", (ParquetPart(path,5,path.stat().st_size,schema),), schema)
    report.generated_table(data, tmp_path)
    load_tables(backend, {"region":data}, report, 65536, lambda m:None)
    assert report.table("region")["status"] == "succeeded"
    shutil.rmtree(local)
    independent = SqlCatalog("independent_reader", **properties)
    table = independent.load_table((*options.namespace,"region"))
    state = inspect_inventory(table)
    assert sum(item.rows for item in state.files.values()) == 5
    assert all(uri.startswith(server.url + "/") for uri in state.files)
    actual = table.scan().to_arrow()
    assert actual.num_rows == 5
    assert actual["r_regionkey"].to_pylist() == [0,1,2,3,4]
