from decimal import Decimal
from types import SimpleNamespace
from pathlib import Path
import hashlib
import io
import json
import os

import pytest

from crowdb_tpc_loader import util
from crowdb_tpc_loader.errors import ArgumentError, CompatibilityError, ResourceError, ValidationError
from crowdb_tpc_loader.report import atomic_json
from crowdb_tpc_loader.schemas import TPCH, TPCDS, inventory
from crowdb_tpc_loader.validation import discover_parts, identify_table, validate_tpch_counts


@pytest.mark.parametrize("benchmark,counts", [
    ("tpch", {"region":3,"nation":4,"supplier":7,"customer":8,"part":9,"partsupp":5,"orders":9,"lineitem":16}),
    ("tpcds", {"call_center":31,"catalog_page":9,"catalog_returns":27,"catalog_sales":34,"customer":18,
     "customer_address":13,"customer_demographics":9,"date_dim":28,"household_demographics":5,
     "income_band":3,"inventory":4,"item":22,"promotion":19,"reason":3,"ship_mode":6,"store":29,
     "store_returns":20,"store_sales":23,"time_dim":10,"warehouse":14,"web_page":14,
     "web_returns":24,"web_sales":34,"web_site":26})])
def test_explicit_inventory_column_counts(benchmark, counts):
    actual = inventory(benchmark)
    assert {key: len(value) for key, value in actual.items()} == counts
    for columns in actual.values():
        assert len({c.name for c in columns}) == len(columns)
        assert {c.kind for c in columns} <= {"I","L","S","D","D15_2","D7_2","D5_2"}
    assert "s_tax_precentage" in [c.name for c in TPCDS["store"]]


def test_unknown_benchmark():
    with pytest.raises(ValueError):
        inventory("invented")


@pytest.mark.parametrize("uri", ["/tmp/a.parquet", "file:///tmp/a", "memory://x/a", "local://x/a", "s3://x",
                                 "https://u:p@x/a", "https://x/a?secret=z", "s3://x/a#f", "https://x:bad/a"])
def test_reject_non_durable_uris(uri):
    with pytest.raises(CompatibilityError):
        util.remote_uri(uri)


@pytest.mark.parametrize("uri", ["s3://bucket/table/data/a.parquet", "https://host/a.parquet", "abfs://x/a", "gs://b/a"])
def test_remote_uris(uri):
    assert util.remote_uri(uri) == uri


def test_nonempty_output_never_modified(tmp_path):
    marker = tmp_path / "mine.txt"
    marker.write_text("must remain")
    with pytest.raises(ArgumentError):
        util.prepare_generate_directory(tmp_path)
    assert marker.read_text() == "must remain"


def test_new_output_and_owned_cleanup(tmp_path):
    parent = tmp_path / "parent"
    parent.mkdir()
    marker = parent / "user.txt"
    marker.write_text("keep")
    work = util.create_work_directory(parent, "abc")
    util.mark_owned(work, "abc")
    (work / "data").write_text("temporary")
    with pytest.raises(ResourceError):
        util.cleanup_owned(work, "wrong")
    util.cleanup_owned(work, "abc")
    assert not work.exists() and marker.read_text() == "keep"


def test_no_cleanup_without_marker(tmp_path):
    with pytest.raises(ResourceError):
        util.cleanup_owned(tmp_path, "abc")
    assert tmp_path.exists()


def test_lock_exclusion(tmp_path):
    with util.directory_lock(tmp_path):
        with pytest.raises(ResourceError):
            with util.directory_lock(tmp_path):
                pytest.fail("second writer entered")
    assert not (tmp_path / ".crowdb-tpc.lock").exists()


def test_symlink_output_refused(tmp_path):
    target = tmp_path / "target"
    target.mkdir()
    link = tmp_path / "link"
    try:
        link.symlink_to(target, target_is_directory=True)
    except OSError:
        pytest.skip("symlinks unavailable")
    with pytest.raises(ArgumentError):
        util.prepare_generate_directory(link)


def test_disk_preflight(monkeypatch, tmp_path):
    monkeypatch.setattr(util.shutil, "disk_usage", lambda p: SimpleNamespace(free=100))
    with pytest.raises(ResourceError, match="Insufficient"):
        util.ensure_space(tmp_path, 101)
    assert util.ensure_space(tmp_path, 100) == 100
    assert util.estimated_disk_bytes("tpcds", Decimal(".01")) > 2 * util.GIB
    assert util.estimated_disk_bytes("tpch", Decimal(2)) > util.estimated_disk_bytes("tpch", Decimal(1))


@pytest.mark.parametrize("partial", [None, 1, 7, 100])
def test_bounded_stream_with_partial_writes(partial):
    value = bytes(range(256)) * 100
    class Reader(io.BytesIO):
        def read(self, size=-1):
            assert 0 < size <= 31
            return super().read(size)
    class Writer(io.BytesIO):
        def write(self, data):
            if partial is None:
                super().write(data)
                return None
            return super().write(data[:partial])
    target = Writer()
    progress = []
    count, digest = util.copy_stream(Reader(value), target, 31, progress.append)
    assert count == len(value) and target.getvalue() == value
    assert digest == hashlib.sha256(value).hexdigest()
    assert progress[-1] == len(value) and sorted(progress) == progress
    assert util.hash_stream(Reader(value), 31) == (count, digest)


@pytest.mark.parametrize("count", [0, -1, 999])
def test_invalid_write_length(count):
    with pytest.raises(OSError):
        util.copy_stream(io.BytesIO(b"abc"), SimpleNamespace(write=lambda b: count), 3)


def test_atomic_report_and_secrets(report, tmp_path):
    report.data["errors"].append("raw top-secret-credential https://u:p@h/a?signature=123")
    report.finish("failed")
    raw = report.paths[0].read_text()
    assert "top-secret-credential" not in raw and "?signature=" not in raw and "u:p@" not in raw
    assert json.loads(raw)["status"] == "failed"
    if os.name != "nt":
        assert report.paths[0].stat().st_mode & 0o077 == 0
    assert not list(tmp_path.glob(".checkpoint.json.*"))


def test_summary_distinguishes_uncertain(report):
    report.table("region").update(status="uncertain", files=[{"state":"commit_unknown","remote_uri":"s3://b/a"}])
    report.table("nation").update(status="failed", files=[{"state":"upload_started","remote_uri":"s3://b/b"}])
    summary = report.summary()
    assert summary["uncertain"] == ["region"] and summary["failed"] == ["nation"]
    assert summary["uncertain_uploads"][0]["uri"] == "s3://b/a"
    assert summary["unregistered_uploads"][0]["uri"] == "s3://b/b"


@pytest.mark.parametrize("name", ["lineitem.parquet", "lineitem.1.parquet", "lineitem_0002.parquet", "lineitem-4.parquet",
                                 "lineitem/part-005.parquet"])
def test_multipart_filename_assignment(tmp_path, name):
    assert identify_table(tmp_path / name, tmp_path, tuple(TPCH)) == "lineitem"


def test_ambiguous_part_rejected(tmp_path):
    with pytest.raises(ValidationError):
        identify_table(tmp_path / "nation/region.parquet", tmp_path, tuple(TPCH))


def test_discovery_requires_all_tables(tmp_path):
    (tmp_path / "region.parquet").write_bytes(b"not-footer-test")
    with pytest.raises(ValidationError, match="Incomplete"):
        discover_parts(tmp_path, "tpch")


def test_discovery_full_multipart(tmp_path):
    for name in TPCH:
        (tmp_path / f"{name}.parquet").write_bytes(b"not-footer-test")
    (tmp_path / "lineitem.2.parquet").write_bytes(b"another")
    assert len(discover_parts(tmp_path, "tpch")["lineitem"]) == 2


def test_hardlink_duplicate_rejected(tmp_path):
    for name in TPCH:
        (tmp_path / f"{name}.parquet").write_bytes(b"not-footer-test")
    try:
        os.link(tmp_path / "lineitem.parquet", tmp_path / "lineitem.2.parquet")
    except OSError:
        pytest.skip("hard links unavailable")
    with pytest.raises(ValidationError, match="hard-linked"):
        discover_parts(tmp_path, "tpch")


def test_tpch_sf1_counts():
    values = dict(region=5,nation=25,supplier=10000,customer=150000,part=200000,partsupp=800000,
                  orders=1500000,lineitem=6001215)
    tables = {k: SimpleNamespace(rows=v) for k,v in values.items()}
    validate_tpch_counts(tables, Decimal(1))
    tables["lineitem"].rows -= 1
    with pytest.raises(ValidationError, match="lineitem"):
        validate_tpch_counts(tables, Decimal(1))
