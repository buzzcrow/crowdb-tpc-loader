from dataclasses import replace
import json

import pytest

from crowdb_tpc_loader import loader
from crowdb_tpc_loader.errors import CompatibilityError, ValidationError, ResourceError
from crowdb_tpc_loader.runner import Runner
from crowdb_tpc_loader.schemas import inventory


def make_runner(options, redactor, fake_backend, fake_generator_factory, make_data, **kwargs):
    def validator(output, benchmark, sf, generated, emit):
        fake_backend.calls.append("validate_dataset")
        return make_data(inventory(benchmark), root=output)

    def probe(backend, *args):
        backend.calls.append("probe")

    def table_loader(backend, tables, report, buffer, emit, upload_workers=1):
        return loader.load_tables(
            backend,
            tables,
            report,
            buffer,
            emit,
            uploader=lambda *args: "digest",
            upload_workers=upload_workers,
        )

    defaults = dict(
        generator_factory=fake_generator_factory,
        backend_factory=lambda *args: fake_backend,
        validator=validator,
        prober=probe,
        table_loader=table_loader,
    )
    defaults.update(kwargs)
    return Runner(options, lambda message: None, redactor, **defaults)


def test_load_success_cleanup_parent_safe(options, redactor, fake_backend, fake_generator_factory, make_data):
    options.work_dir.mkdir()
    marker = options.work_dir / "my-file.txt"
    marker.write_text("keep")
    runner = make_runner(options, redactor, fake_backend, fake_generator_factory, make_data)
    result = runner.run()
    assert result.exit_code == 0, result.report["errors"]
    assert result.report["work_directory_removed"] is True
    assert not runner.work.exists() and marker.read_text() == "keep"
    assert result.report_path.exists()
    assert len(result.report["summary"]["succeeded"]) == 8
    assert fake_backend.calls.index("probe") < fake_backend.calls.index("validate_dataset")
    assert fake_backend.calls.index("validate_dataset") < fake_backend.calls.index("create:region")
    assert "top-secret-credential" not in result.report_path.read_text()


def test_native_fileio_does_not_leave_undeletable_probe(
    options, redactor, fake_backend, fake_generator_factory, make_data
):
    fake_backend.probe_cleanup_supported = False
    runner = make_runner(options, redactor, fake_backend, fake_generator_factory, make_data)
    result = runner.run()
    assert result.exit_code == 0, result.report["errors"]
    assert "probe" not in fake_backend.calls
    assert fake_backend.calls.index("validate_dataset") < fake_backend.calls.index("create:region")


def test_keep_files(options, redactor, fake_backend, fake_generator_factory, make_data):
    runner = make_runner(
        replace(options, keep_files=True), redactor, fake_backend, fake_generator_factory, make_data
    )
    result = runner.run()
    assert result.exit_code == 0 and runner.work.exists()
    assert (runner.work / "data/lineitem/part-00000.parquet").exists()
    assert not (runner.work / ".scratch").exists()
    assert (runner.work / "run-summary.json").exists()


def test_generate_persistent_output(
    options, redactor, fake_backend, fake_generator_factory, make_data, tmp_path
):
    options = replace(options, command="generate", output_dir=tmp_path / "export", report_file=None)
    runner = make_runner(options, redactor, fake_backend, fake_generator_factory, make_data)
    result = runner.run()
    assert result.exit_code == 0
    assert runner.work == options.output_dir
    assert (options.output_dir / "data/region/part-00000.parquet").exists()
    assert result.report_path == options.output_dir / "run-summary.json"
    assert fake_backend.calls == ["validate_dataset"]


def test_existing_error_before_generator_or_upload(
    options, redactor, fake_backend, fake_generator_factory, make_data
):
    fake_backend.existing_names.add("region")
    runner = make_runner(
        options,
        redactor,
        fake_backend,
        fake_generator_factory,
        make_data,
        generator_factory=lambda *args: pytest.fail("generator must not start"),
    )
    result = runner.run()
    assert result.exit_code == 4 and runner.work is None
    assert fake_backend.calls == ["connect", "existing"]
    assert result.report_path.exists() and not options.work_dir.exists()


def test_all_skipped_shortcircuit(options, redactor, fake_backend, fake_generator_factory, make_data):
    fake_backend.existing_names = set(inventory("tpch"))
    runner = make_runner(
        replace(options, on_exists="skip"),
        redactor,
        fake_backend,
        fake_generator_factory,
        make_data,
        generator_factory=lambda *args: pytest.fail("generator must not start"),
    )
    result = runner.run()
    assert result.exit_code == 0 and len(result.report["summary"]["skipped"]) == 8
    assert fake_backend.calls == ["connect", "existing"] and runner.work is None


def test_partial_skip_still_validates_complete_dataset(
    options, redactor, fake_backend, fake_generator_factory, make_data
):
    fake_backend.existing_names.add("region")
    runner = make_runner(
        replace(options, on_exists="skip"), redactor, fake_backend, fake_generator_factory, make_data
    )
    result = runner.run()
    assert result.exit_code == 0 and result.report["summary"]["skipped"] == ["region"]
    assert "create:region" not in fake_backend.calls
    assert len(result.report["summary"]["succeeded"]) == 7


@pytest.mark.parametrize(
    "failure,code",
    [(ValidationError("bad decimal"), 3), (ResourceError("disk full"), 5), (KeyboardInterrupt(), 130)],
)
def test_validation_failure_never_creates_tables_retains_work(
    options, redactor, fake_backend, fake_generator_factory, make_data, failure, code
):
    def reject(*args):
        raise failure

    runner = make_runner(options, redactor, fake_backend, fake_generator_factory, make_data, validator=reject)
    result = runner.run()
    assert result.exit_code == code and runner.work.exists()
    assert not any(c.startswith("create:") for c in fake_backend.calls)
    assert not (runner.work / ".crowdb-tpc.lock").exists()
    assert json.loads(result.report_path.read_text())["status"] in {"failed", "interrupted"}


def test_probe_failure_before_generation(options, redactor, fake_backend, fake_generator_factory, make_data):
    def probe(*args):
        raise CompatibilityError("FileIO refused")

    runner = make_runner(options, redactor, fake_backend, fake_generator_factory, make_data, prober=probe)
    result = runner.run()
    assert result.exit_code == 4 and runner.work.exists()
    assert not (runner.work / "data").exists() and "validate_dataset" not in fake_backend.calls


def test_explicit_report_no_overwrite(options, redactor, fake_backend, fake_generator_factory, make_data):
    options.report_file.write_text("existing user report")
    runner = make_runner(options, redactor, fake_backend, fake_generator_factory, make_data)
    result = runner.run()
    assert result.exit_code == 2 and options.report_file.read_text() == "existing user report"
    assert not fake_backend.calls


def test_nonempty_generate_refused(
    options, redactor, fake_backend, fake_generator_factory, make_data, tmp_path
):
    output = tmp_path / "output"
    output.mkdir()
    (output / "user.txt").write_text("untouched")
    runner = make_runner(
        replace(options, command="generate", output_dir=output),
        redactor,
        fake_backend,
        fake_generator_factory,
        make_data,
    )
    result = runner.run()
    assert result.exit_code == 2 and (output / "user.txt").read_text() == "untouched"
    assert len(list(output.iterdir())) == 1


def test_generate_directory_race_does_not_overwrite_other_run(
    options, redactor, fake_backend, fake_generator_factory, make_data, monkeypatch, tmp_path
):
    from crowdb_tpc_loader import runner as runner_module

    original = runner_module.prepare_generate_directory
    output = tmp_path / "race-output"

    def raced_prepare(path):
        result = original(path)
        (result / "run-summary.json").write_text("OTHER RUN REPORT")
        (result / "data.parquet").write_bytes(b"OTHER RUN DATA")
        return result

    monkeypatch.setattr(runner_module, "prepare_generate_directory", raced_prepare)
    runner = make_runner(
        replace(options, command="generate", output_dir=output),
        redactor,
        fake_backend,
        fake_generator_factory,
        make_data,
    )
    result = runner.run()
    assert result.exit_code == 2
    assert (output / "run-summary.json").read_text() == "OTHER RUN REPORT"
    assert (output / "data.parquet").read_bytes() == b"OTHER RUN DATA"
    assert not (output / ".crowdb-tpc.lock").exists()
