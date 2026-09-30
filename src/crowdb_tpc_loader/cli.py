"""Stable CLI; --help and --version do not import generator / Iceberg dependencies."""
from __future__ import annotations

import argparse
import functools
import json
import math
import os
import re
import signal
import sys
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Sequence
from urllib.parse import urlsplit

from . import __version__
from .errors import ArgumentError
from .models import Options
from .runner import Runner
from .security import Redactor, configure_logging
from .util import format_bytes


class SafeParser(argparse.ArgumentParser):
    def __init__(self, *args, redactor: Redactor | None = None, **kwargs):
        self.redactor = redactor or Redactor()
        super().__init__(*args, **kwargs)

    def _print_message(self, message, file=None):
        if message:
            super()._print_message(self.redactor.text(message), file)


def positive_sf(value: str) -> Decimal:
    try:
        if len(value) > 64:
            raise ValueError()
        sf = Decimal(value)
        floating = float(sf)
        if not sf.is_finite() or sf <= 0 or not math.isfinite(floating) or floating == 0:
            raise ValueError()
    except (InvalidOperation, ValueError, OverflowError):
        raise argparse.ArgumentTypeError("--sf must be a positive finite scale factor representable as a double") from None
    return sf


def positive_int(value: str) -> int:
    try:
        parsed = int(value)
        if parsed <= 0:
            raise ValueError()
        return parsed
    except ValueError:
        raise argparse.ArgumentTypeError("must be a positive integer") from None


def positive_timeout(value: str) -> float:
    try:
        parsed = float(value)
        if not math.isfinite(parsed) or parsed <= 0:
            raise ValueError()
        return parsed
    except ValueError:
        raise argparse.ArgumentTypeError("timeout must be positive and finite") from None


def build_parser(redactor: Redactor) -> SafeParser:
    parser = SafeParser(prog="crowdb-tpc-loader", redactor=redactor,
                        description="Generate TPC-H/TPC-DS Parquet and safely load durable Iceberg tables.")
    parser.add_argument("--version", action="version", version=f"%(prog)s {__version__}")
    commands = parser.add_subparsers(dest="command", required=True,
                                    parser_class=functools.partial(SafeParser, redactor=redactor))
    for name, help_text in (("generate", "Generate and validate a complete persistent Parquet dataset"),
                            ("load", "Generate, upload, verify and register a complete dataset")):
        command = commands.add_parser(name, help=help_text)
        command.add_argument("--benchmark", required=True, choices=("tpch", "tpcds"))
        command.add_argument("--sf", type=positive_sf, default=Decimal(1), help="positive scale factor (default: 1)")
        command.add_argument("--report-file", type=Path, help="also save the JSON report at a new path")
        command.add_argument("--tpchgen", type=Path, help="prebuilt tpchgen-cli 3.x executable (otherwise PATH/cache/PyPI wheel)")
        command.add_argument("--no-download", action="store_true", help="forbid generator binary/extension downloads")
        command.add_argument("--memory-limit", default="1GB", help="DuckDB native memory limit, e.g. 1GB or 512MB")
        command.add_argument("--threads", type=positive_int, default=2, help="native generator threads (default: 2)")
        command.add_argument("--timeout", type=positive_timeout, default=60.0,
                             help="network/preflight timeout in seconds, NOT total generation time")
        command.add_argument("--quiet", action="store_true", help="suppress progress (summary/errors remain)")
        command.add_argument("--json", action="store_true", help="print the final run report as JSON on stdout")
        if name == "generate":
            command.add_argument("--output-dir", type=Path, required=True, help="new or empty persistent directory")
        else:
            command.add_argument("--catalog-uri", help="REST Catalog base URI; overrides ICEBERG_URI")
            command.add_argument("--token", help="Catalog authentication token; prefer ICEBERG_TOKEN to avoid shell/process exposure")
            command.add_argument("--namespace", help="namespace (default: benchmark); dots separate namespace levels")
            command.add_argument("--on-exists", choices=("error", "skip"), default="error")
            command.add_argument("--work-dir", type=Path, help="staging PARENT; a unique child is created, never erase this parent")
            command.add_argument("--keep-files", action="store_true", help="retain local Parquet after a successful load")
            command.add_argument("--upload-buffer-mib", type=positive_int, default=8,
                                 help="single-upload copy/checksum buffer, 1–64 MiB (default: 8)")
            command.add_argument("--upload-workers", type=positive_int, default=24,
                                 help="maximum concurrent file uploads (default: 24)")
            command.add_argument("--catalog-property", action="append", default=[], metavar="KEY=VALUE",
                                 help="advanced PyIceberg FileIO/catalog property; repeatable")
            command.add_argument("--py-io-impl", help="deployment-specific Python FileIO implementation class")
    return parser


def to_options(args: argparse.Namespace, redactor: Redactor) -> Options:
    if not re.fullmatch(r"[1-9][0-9]*(?:\.[0-9]+)?(?:KB|MB|GB|TB|KiB|MiB|GiB|TiB)", args.memory_limit, re.I):
        raise ArgumentError("--memory-limit must be a positive size such as 512MB or 1GB")
    common = dict(command=args.command, benchmark=args.benchmark, sf=args.sf, report_file=args.report_file,
                  tpchgen=args.tpchgen, no_download=args.no_download, memory_limit=args.memory_limit,
                  threads=args.threads, timeout=args.timeout, quiet=args.quiet)
    if args.command == "generate":
        return Options(**common, output_dir=args.output_dir)
    uri = args.catalog_uri if args.catalog_uri is not None else os.getenv("ICEBERG_URI")
    token = args.token if args.token is not None else os.getenv("ICEBERG_TOKEN")
    redactor.add(token)
    if not uri:
        raise ArgumentError("load requires --catalog-uri or ICEBERG_URI")
    try:
        parsed = urlsplit(uri)
        _ = parsed.port
        valid = (parsed.scheme in {"http", "https"} and parsed.netloc and not parsed.username
                 and not parsed.password and not parsed.query and not parsed.fragment)
    except ValueError:
        valid = False
    if not valid:
        raise ArgumentError("Catalog URI must be an HTTP(S) URL without userinfo, query parameters or fragment")
    if token is not None and ("\n" in token or "\r" in token):
        raise ArgumentError("Token must not contain newline characters")
    namespace = tuple((args.namespace if args.namespace is not None else args.benchmark).split("."))
    if any(not re.fullmatch(r"[A-Za-z0-9_-]{1,255}", name) for name in namespace):
        raise ArgumentError("Namespace levels must contain 1–255 letters, numbers, underscores or hyphens; separate levels with dots")
    if args.upload_buffer_mib > 64:
        raise ArgumentError("--upload-buffer-mib must be between 1 and 64")
    if args.upload_workers > 24:
        raise ArgumentError("--upload-workers must be between 1 and 24")
    properties = {}
    for entry in args.catalog_property:
        if "=" not in entry:
            raise ArgumentError("--catalog-property requires KEY=VALUE")
        key, value = entry.split("=", 1)
        if not re.fullmatch(r"[A-Za-z][A-Za-z0-9_.-]*", key):
            raise ArgumentError("Invalid catalog property name")
        if key in {"uri", "token", "type"}:
            raise ArgumentError("Use --catalog-uri/--token rather than overriding reserved catalog properties")
        properties[key] = value
    if args.py_io_impl:
        properties["py-io-impl"] = args.py_io_impl
    redactor.learn(properties)
    return Options(**common, catalog_uri=uri.rstrip("/"), token=token, namespace=namespace,
                   on_exists=args.on_exists, work_dir=args.work_dir, keep_files=args.keep_files,
                   upload_buffer_mib=args.upload_buffer_mib, upload_workers=args.upload_workers,
                   catalog_properties=properties)


def initial_redactor(argv: list[str]) -> Redactor:
    redactor = Redactor([os.getenv("ICEBERG_TOKEN", "")])
    for index, value in enumerate(argv):
        if value == "--token" and index + 1 < len(argv):
            redactor.add(argv[index + 1])
        elif value.startswith("--token="):
            redactor.add(value.split("=", 1)[1])
        # Protect sensitive --catalog-property values even when argument parsing itself fails.
        if value.startswith("--catalog-property="):
            value = value.split("=", 1)[1]
        if "=" in value:
            key, text = value.split("=", 1)
            redactor.learn({key: text})
    return redactor


def print_summary(report: dict, redactor: Redactor) -> None:
    lines = [f"\n{report['command']} {report['benchmark']} SF={report['scale_factor']}: {report['status']}",
             f"Run ID: {report['run_id']}",
             f"{'TABLE':26} {'STATUS':12} {'ROWS':>16} {'FILES':>7} {'SIZE':>12}"]
    for name, row in report["tables"].items():
        rows = f"{row['row_count']:,}" if "row_count" in row else "-"
        size = format_bytes(row["size_bytes"]) if "size_bytes" in row else "-"
        lines.append(f"{name:26} {row['status']:12} {rows:>16} {row.get('file_count', '-'):>7} {size:>12}")
    for error in report.get("errors", []):
        lines.append(f"ERROR: {error}")
    for warning in report.get("warnings", []):
        lines.append(f"WARNING: {warning}")
    summary = report.get("summary", {})
    for key, label in (("unregistered_uploads", "Unregistered/partial upload candidates"),
                       ("uncertain_uploads", "UNCERTAIN uploads — do not delete"),
                       ("probe_cleanup_candidates", "Uncommitted probe cleanup candidates")):
        items = summary.get(key, [])
        if items:
            lines.append(f"{label}: {len(items)}; inspect the JSON report before manual cleanup.")
            for item in items:
                lines.append(f"  {item['uri']}")
    if report.get("work_directory") and not report.get("work_directory_removed"):
        lines.append(f"Local files retained: {report['work_directory']}")
    if report.get("report_file"):
        lines.append(f"Run report: {report['report_file']}")
    print(redactor.text("\n".join(lines)))


def main(argv: Sequence[str] | None = None) -> int:
    arguments = list(sys.argv[1:] if argv is None else argv)
    redactor = initial_redactor(arguments)
    parser = build_parser(redactor)
    try:
        args = parser.parse_args(arguments)
        options = to_options(args, redactor)
    except SystemExit as exc:
        return int(exc.code or 0)
    except ArgumentError as exc:
        print(redactor.text(f"ERROR: {exc}"), file=sys.stderr)
        return exc.exit_code
    configure_logging(redactor)

    def emit(message: str) -> None:
        if not options.quiet:
            print(redactor.text(message), file=sys.stderr, flush=True)

    previous_handler = None
    try:
        previous_handler = signal.getsignal(signal.SIGTERM)
        signal.signal(signal.SIGTERM, lambda signum, frame: (_ for _ in ()).throw(KeyboardInterrupt()))
    except (ValueError, AttributeError):
        pass  # Embedded invocation in a non-main thread.
    try:
        result = Runner(options, emit, redactor).run()
        if args.json:
            print(json.dumps(result.report, indent=2, ensure_ascii=False, allow_nan=False))
        else:
            print_summary(result.report, redactor)
        return result.exit_code
    finally:
        if previous_handler is not None:
            try:
                signal.signal(signal.SIGTERM, previous_handler)
            except ValueError:
                pass
