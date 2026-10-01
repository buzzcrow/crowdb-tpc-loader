"""Run orchestration with injected boundaries for deterministic failure-path tests."""

from __future__ import annotations

import shutil
import uuid
from contextlib import ExitStack
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any, Callable

from .backend import IcebergBackend
from .errors import ArgumentError, ExistingTablesError, LoaderError
from .generators import make_generator
from .loader import load_tables
from .models import Options
from .report import RunReport
from .schemas import inventory
from .security import Redactor
from .util import (
    cleanup_owned,
    create_work_directory,
    directory_lock,
    ensure_space,
    estimated_disk_bytes,
    format_bytes,
    mark_owned,
    prepare_generate_directory,
)
from .validation import validate_dataset


@dataclass(frozen=True)
class RunResult:
    exit_code: int
    report: dict[str, Any]
    report_path: Path | None


class Runner:
    def __init__(
        self,
        options: Options,
        emit: Callable[[str], None],
        redactor: Redactor,
        generator_factory: Callable = make_generator,
        backend_factory: Callable = IcebergBackend,
        validator: Callable = validate_dataset,
        prober: Callable | None = None,
        table_loader: Callable = load_tables,
    ):
        self.options, self.emit, self.redactor = options, emit, redactor
        self.generator_factory, self.backend_factory = generator_factory, backend_factory
        if prober is None:
            from .transfers import probe_fileio

            prober = probe_fileio
        self.validator, self.prober, self.table_loader = validator, prober, table_loader
        self.report = RunReport(options, uuid.uuid4().hex, redactor)
        self.work: Path | None = None
        self.external_report: Path | None = None

    def _report_location(self, default: Path) -> None:
        path = (self.options.report_file or default).expanduser().absolute()
        if path.exists() or path.is_symlink():
            raise ArgumentError(f"Report file already exists; refusing to overwrite: {path}")
        self.external_report = path
        self.report.paths.append(path)
        self.report.data["report_file"] = str(path)
        self.report.save()

    def _result(self, code: int) -> RunResult:
        return RunResult(code, self.redactor.data(self.report.data), self.external_report)

    def run(self) -> RunResult:
        options = self.options
        report = self.report
        run_id = report.data["run_id"]
        generator = None
        self.emit(f"Run {run_id}: {options.command} {options.benchmark} SF={options.sf}")
        try:
            if options.report_file and (options.report_file.exists() or options.report_file.is_symlink()):
                raise ArgumentError("--report-file already exists; choose a new file")
            backend = None
            if options.command == "load":
                backend = self.backend_factory(options, self.redactor)
                backend.connect()
                existing = backend.existing(tuple(inventory(options.benchmark)))
                if existing and options.on_exists == "error":
                    raise ExistingTablesError(
                        "Target tables already exist: "
                        + ", ".join(sorted(existing))
                        + ". No generation or upload was started. Use a fresh namespace or --on-exists skip."
                    )
                if existing:
                    report.data["warnings"].append(
                        "Existing skipped tables are left unchanged and are NOT validated. "
                        "--on-exists skip is not resume/repair and does not prove their scale factor or completeness."
                    )
                for name in existing:
                    report.table(name).update(
                        {"status": "skipped", "reason": "existing table left unchanged"}
                    )
                if len(existing) == len(inventory(options.benchmark)):
                    self._report_location(Path.cwd() / f"crowdb-tpc-{run_id}.json")
                    report.data["phase"] = "complete"
                    report.finish("succeeded")
                    self.emit(
                        "All target tables exist: skipped without generation, namespace changes or upload"
                    )
                    return self._result(0)
                self.work = create_work_directory(options.work_dir, run_id)
                mark_owned(self.work, run_id)
                self._report_location(self.work.parent / f"crowdb-tpc-{run_id}.json")
                report.paths.insert(0, self.work / "run-summary.json")
            else:
                assert options.output_dir is not None
                self.work = prepare_generate_directory(options.output_dir)
            with ExitStack() as stack:
                stack.enter_context(directory_lock(self.work))
                if options.command == "generate":
                    # Recheck UNDER the exclusive lock: another process may have
                    # populated the directory after our initial emptiness check.
                    if any(path.name != ".crowdb-tpc.lock" for path in self.work.iterdir()):
                        raise ArgumentError("--output-dir became nonempty; refusing to overwrite another run")
                    # Do not publish any report into this user-owned directory
                    # until both the lock and the second emptiness check succeed.
                    self._report_location(self.work / "run-summary.json")
                    if self.external_report != self.work / "run-summary.json":
                        report.paths.insert(0, self.work / "run-summary.json")
                report.data["work_directory"] = str(self.work)
                report.save()
                need = estimated_disk_bytes(options.benchmark, options.sf)
                free = ensure_space(self.work, need)
                report.data["staging_space"] = {
                    "free_bytes": free,
                    "estimated_required_bytes": need,
                    "estimate_is_guarantee": False,
                }
                self.emit(
                    f"Staging: {self.work}; free {format_bytes(free)}, estimated need {format_bytes(need)}"
                )
                scratch = self.work / ".scratch"
                scratch.mkdir()
                generator = self.generator_factory(options, self.emit, self.redactor)
                prepared = generator.prepare(scratch)
                report.data["generator"] = asdict(prepared)
                report.save()
                if backend is not None:
                    if callable(getattr(backend, "set_staging", None)):
                        backend.set_staging(scratch)
                    backend.ensure_namespace()
                    if getattr(backend, "probe_cleanup_supported", True):
                        self.prober(backend, scratch, report, options.upload_buffer_mib * 1024**2, self.emit)
                    else:
                        self.emit("CrowDB FileIO: uploads start with the first table")
                report.phase("generating")
                output = self.work / "data"
                generated = generator.generate(output, scratch)
                report.data["generator"] = asdict(generated)
                report.phase("validating")
                tables = self.validator(output, options.benchmark, options.sf, generated, self.emit)
                # No real benchmark table is created before the COMPLETE dataset passes validation.
                for table in tables.values():
                    report.generated_table(table, self.work)
                if backend is None:
                    for row in report.data["tables"].values():
                        row["status"] = "succeeded"
                else:
                    report.phase("loading")
                    self.table_loader(
                        backend,
                        tables,
                        report,
                        options.upload_buffer_mib * 1024**2,
                        self.emit,
                        upload_workers=options.upload_workers,
                    )
                generator.close()
                generator = None
                if not scratch.is_symlink():
                    shutil.rmtree(scratch)
                report.data["phase"] = "complete"
                report.finish("succeeded")
            if options.command == "load" and not options.keep_files:
                try:
                    cleanup_owned(self.work, run_id)
                    report.paths = [path for path in report.paths if not path.is_relative_to(self.work)]
                    report.data["work_directory_removed"] = True
                    report.save()
                    self.emit("Local staging removed. Registered remote data was not touched.")
                except Exception as exc:
                    # A cleanup failure does not undo verified, successfully committed tables.
                    report.paths = [path for path in report.paths if path.parent.exists()]
                    report.data["warnings"].append(f"Local staging cleanup was incomplete: {exc}")
                    report.data["work_directory_removed"] = False
                    self._safe_finish("succeeded")
            return self._result(0)
        except (KeyboardInterrupt, SystemExit):
            for row in report.data["tables"].values():
                if row["status"] == "committing":
                    row["status"] = "uncertain"
                elif row["status"] in {"creating", "uploading"}:
                    row["status"] = "failed"
                    row["error"] = "Run interrupted"
            self._safe_finish(
                "interrupted", "Run interrupted; staging and any uncertain remote files were retained"
            )
            return self._result(130)
        except Exception as exc:
            code = exc.exit_code if isinstance(exc, LoaderError) else 1
            self._safe_finish("failed", str(exc))
            return self._result(code)
        finally:
            if generator is not None:
                try:
                    generator.close()
                except Exception as exc:
                    self.emit(self.redactor.text(f"Generator close warning: {exc}"))

    def _safe_finish(self, status: str, error: str | None = None) -> None:
        try:
            if not self.report.paths:
                fallback = (
                    self.options.report_file or Path.cwd() / f"crowdb-tpc-{self.report.data['run_id']}.json"
                )
                if not fallback.exists() and not fallback.is_symlink():
                    self.external_report = fallback.absolute()
                    self.report.paths.append(self.external_report)
                    self.report.data["report_file"] = str(self.external_report)
            self.report.finish(status, error)
        except Exception as exc:
            # Disk full can make checkpointing impossible; still emit the original report on stdout.
            self.report.data["warnings"].append(f"Could not persist the final report: {exc}")
        if self.work:
            self.emit(f"Working directory retained: {self.work}")
