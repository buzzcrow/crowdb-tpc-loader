from types import SimpleNamespace
from threading import Barrier

import pytest

from crowdb_tpc_loader import loader
from crowdb_tpc_loader.backend import IcebergBackend, Inventory, RemotePart, inspect_inventory
from crowdb_tpc_loader.errors import CommitUncertainError, LoadError


@pytest.fixture(autouse=True)
def no_reconciliation_delay(monkeypatch):
    original = loader.reconcile

    def fast(*args, **kwargs):
        return original(*args, **kwargs, sleep=lambda seconds: None)

    monkeypatch.setattr(loader, "reconcile", fast)


def seed(report, data, tmp_path):
    for item in data.values():
        report.generated_table(item, tmp_path)


def run(report, data, backend, uploader=None):
    loader.load_tables(
        backend,
        data,
        report,
        1024,
        lambda message: None,
        uploader=uploader or (lambda *args: "verified-test-digest"),
        upload_workers=1,
    )


def test_success_journal_and_remote_uris(report, make_data, fake_backend, tmp_path):
    data = make_data(("region", "nation"))
    seed(report, data, tmp_path)

    def upload(table, part, uri, buffer, emit):
        item = report.table(table.name()[-1])["files"][0]
        assert item["remote_uri"] == uri and item["state"] == "upload_started"
        assert uri.startswith("s3://unit-test/")
        assert "unit-test-run" in uri
        assert "upload_started" in report.paths[0].read_text()
        return "verified-test-digest"

    run(report, data, fake_backend, upload)
    for name in data:
        row = report.table(name)
        assert row["status"] == "succeeded" and row["snapshot_id"] == 123
        assert row["files"][0]["state"] == "registered"
        assert fake_backend.calls.count("register:" + name) == 1
    assert fake_backend.calls.index("register:region") < fake_backend.calls.index("create:nation")


def test_pyiceberg_definite_rejections_match_installed_exception_types():
    from pyiceberg.exceptions import BadRequestError, CommitFailedException, ForbiddenError, UnauthorizedError

    for error in (
        BadRequestError("bad"),
        CommitFailedException("conflict"),
        ForbiddenError("forbidden"),
        UnauthorizedError("unauthorized"),
    ):
        assert IcebergBackend.definite_rejection(error)
    assert not IcebergBackend.definite_rejection(TimeoutError("unknown"))


def test_timeout_after_commit_reconciles_no_retry(report, make_data, fake_backend, tmp_path):
    data = make_data()
    seed(report, data, tmp_path)
    fake_backend.commit_error = TimeoutError("lost commit response")
    run(report, data, fake_backend)
    assert report.table("region")["status"] == "succeeded"
    assert fake_backend.calls.count("register:region") == 1
    assert any("registration was verified" in item for item in report.data["warnings"])


def test_timeout_not_visible_is_uncertain(report, make_data, fake_backend, tmp_path):
    data = make_data(("region", "nation"))
    seed(report, data, tmp_path)
    fake_backend.commit_visible = False
    fake_backend.commit_error = TimeoutError("ambiguous")
    with pytest.raises(CommitUncertainError):
        run(report, data, fake_backend)
    assert fake_backend.calls.count("register:region") == 1
    assert fake_backend.calls.count("reload:region") == 3
    assert "create:nation" not in fake_backend.calls
    assert report.table("region")["status"] == "uncertain"
    assert report.table("region")["files"][0]["state"] == "commit_unknown"
    assert report.summary()["uncertain_uploads"]


def test_definite_rejection_unregistered(report, make_data, fake_backend, tmp_path):
    data = make_data()
    seed(report, data, tmp_path)
    fake_backend.commit_visible = False
    fake_backend.commit_error = PermissionError("denied")
    with pytest.raises(LoadError, match="rejected"):
        run(report, data, fake_backend)
    assert report.table("region")["status"] == "failed"
    assert report.table("region")["files"][0]["state"] == "uploaded_unregistered"
    assert not report.summary()["uncertain_uploads"]


def test_unreachable_reconciliation_stays_uncertain(report, make_data, fake_backend, tmp_path):
    data = make_data()
    seed(report, data, tmp_path)
    fake_backend.read_error = OSError("catalog down")
    with pytest.raises(CommitUncertainError):
        run(report, data, fake_backend)
    assert report.table("region")["files"][0]["state"] == "commit_unknown"
    assert fake_backend.calls.count("register:region") == 1


def test_upload_failure_stops_later_tables_and_journals_partial(report, make_data, fake_backend, tmp_path):
    data = make_data(("region", "nation", "supplier"))
    seed(report, data, tmp_path)

    def upload(table, *args):
        if table.name()[-1] == "nation":
            raise OSError("disk full upstream")
        return "digest"

    with pytest.raises(LoadError, match="nation"):
        run(report, data, fake_backend, upload)
    assert report.table("region")["status"] == "succeeded"
    assert report.table("nation")["files"][0]["state"] == "upload_started"
    assert "register:nation" not in fake_backend.calls and "create:supplier" not in fake_backend.calls
    assert report.summary()["unregistered_uploads"][0]["table"] == "nation"


def test_skip_never_uploads(report, make_data, fake_backend, tmp_path):
    data = make_data(("region", "nation"))
    seed(report, data, tmp_path)
    report.table("region")["status"] = "skipped"
    fake_backend.concurrent_skip.add("nation")
    run(report, data, fake_backend, lambda *args: pytest.fail("upload called for skipped table"))
    assert "create:region" not in fake_backend.calls
    assert report.table("nation")["status"] == "skipped"


def test_parallel_tables_commit_concurrently(report, make_data, fake_backend, tmp_path):
    data = make_data(("region", "nation"))
    seed(report, data, tmp_path)
    overlap = Barrier(2)
    original_register = fake_backend.register

    def register(table, uris, run_id):
        overlap.wait(timeout=5)
        original_register(table, uris, run_id)

    fake_backend.register = register
    loader.load_tables(
        fake_backend,
        data,
        report,
        1024,
        lambda message: None,
        uploader=lambda *args: "digest",
        upload_workers=2,
    )
    assert all(report.table(name)["status"] == "succeeded" for name in data)
    assert all(fake_backend.calls.count("register:" + name) == 1 for name in data)
    assert all(report.table(name)["files"][0]["state"] == "registered" for name in data)


def test_parallel_failure_keeps_other_table_result(report, make_data, fake_backend, tmp_path):
    data = make_data(("region", "nation"))
    seed(report, data, tmp_path)

    def upload(table, part, uri, buffer, emit):
        if table.name()[-1] == "nation":
            raise OSError("upload rejected")
        return "digest"

    with pytest.raises(LoadError, match="nation"):
        loader.load_tables(
            fake_backend, data, report, 1024, lambda message: None, uploader=upload, upload_workers=2
        )
    assert report.table("region")["status"] == "succeeded"
    assert report.table("nation")["status"] == "failed"
    assert report.table("nation")["files"][0]["state"] == "upload_started"
    assert fake_backend.calls.count("register:region") == 1
    assert "register:nation" not in fake_backend.calls


@pytest.mark.parametrize(
    "snapshot,files",
    [
        (None, {}),
        (1, {}),
        (1, {"s3://b/a": RemotePart("s3://b/a", 4, 100)}),
        (1, {"s3://b/a": RemotePart("s3://b/a", 5, 99)}),
        (1, {"s3://b/a": RemotePart("s3://b/a", 5, 100), "s3://b/extra": RemotePart("s3://b/extra", 1, 10)}),
    ],
)
def test_exact_postcommit_verification(snapshot, files):
    with pytest.raises(LoadError):
        loader.verify_inventory(Inventory(snapshot, files, "uuid"), {"s3://b/a": (5, 100)})


def test_zero_row_file_counts_in_manifest():
    item = SimpleNamespace(
        content=0, file_path="s3://b/empty.parquet", record_count=0, file_size_in_bytes=120
    )
    manifest = SimpleNamespace(
        fetch_manifest_entry=lambda io, discard_deleted: [SimpleNamespace(data_file=item)]
    )
    snapshot = SimpleNamespace(snapshot_id=7, manifests=lambda io: [manifest])
    table = SimpleNamespace(
        io=object(), current_snapshot=lambda: snapshot, metadata=SimpleNamespace(table_uuid="u")
    )
    result = inspect_inventory(table)
    assert result.files["s3://b/empty.parquet"].rows == 0
    loader.verify_inventory(result, {"s3://b/empty.parquet": (0, 120)})


def test_duplicate_manifest_path_is_rejected():
    item = SimpleNamespace(content=0, file_path="s3://b/a", record_count=1, file_size_in_bytes=120)
    manifest = SimpleNamespace(
        fetch_manifest_entry=lambda io, discard_deleted: [SimpleNamespace(data_file=item)] * 2
    )
    snapshot = SimpleNamespace(snapshot_id=7, manifests=lambda io: [manifest])
    table = SimpleNamespace(
        io=object(), current_snapshot=lambda: snapshot, metadata=SimpleNamespace(table_uuid="u")
    )
    with pytest.raises(LoadError, match="Duplicate"):
        inspect_inventory(table)


def test_create_retries_transient_catalog_busy(options, redactor, make_data, monkeypatch):
    from pyiceberg.exceptions import ServiceUnavailableError, TableAlreadyExistsError
    from crowdb_tpc_loader import backend as backend_module

    data = make_data()["region"]
    created = SimpleNamespace(metadata=SimpleNamespace(properties={"crowdb-tpc-loader.run-id": "run"}))
    attempts = []

    class Catalog:
        def create_table(self, *args, **kwargs):
            attempts.append("create")
            if len(attempts) == 1:
                raise ServiceUnavailableError("busy")
            raise TableAlreadyExistsError("created by first request")

        def load_table(self, *args):
            attempts.append("load")
            return created

    client = IcebergBackend(options, redactor)
    client.catalog = Catalog()
    monkeypatch.setattr(backend_module.time, "sleep", lambda _: None)
    monkeypatch.setattr(backend_module, "schema_compatible", lambda table, part: None)
    monkeypatch.setattr(client, "_learn", lambda table: None)
    assert client.create(data, "run", {"implementation": "test", "version": "0"}) is created
    assert attempts == ["create", "create", "load"]
