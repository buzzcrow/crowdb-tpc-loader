#!/usr/bin/env python3
"""Read-only verification in a NEW process/catalog client, without local Parquet.

Examples:
  python scripts/verify_crowdb.py load-report.json --iceberg-scan
  python scripts/verify_crowdb.py load-report.json --full-read --require-complete

Credentials come from ICEBERG_TOKEN unless explicitly supplied. No remote table
or object is ever created, modified or deleted by this script.
"""

from __future__ import annotations

import argparse
import json
import os
from pathlib import Path
import sys

from crowdb_tpc_loader.backend import CROWDB_FILE_IO, inspect_inventory
from crowdb_tpc_loader.cli import SafeParser, initial_redactor, positive_timeout
from crowdb_tpc_loader.loader import verify_inventory
from crowdb_tpc_loader.rest_catalog import create_catalog
from crowdb_tpc_loader.schemas import inventory
from crowdb_tpc_loader.security import configure_logging
from crowdb_tpc_loader.util import hash_stream, remote_uri


def main(argv=None) -> int:
    argv = list(sys.argv[1:] if argv is None else argv)
    redactor = initial_redactor(argv)
    parser = SafeParser(
        description=__doc__, redactor=redactor, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument("report", type=Path, help="JSON run report saved by load")
    parser.add_argument("--catalog-uri", default=os.getenv("ICEBERG_URI"))
    parser.add_argument("--token", default=os.getenv("ICEBERG_TOKEN"))
    parser.add_argument("--py-io-impl")
    parser.add_argument("--catalog-property", action="append", default=[], metavar="KEY=VALUE")
    parser.add_argument("--timeout", type=positive_timeout, default=60.0)
    parser.add_argument(
        "--full-read", action="store_true", help="read and decode every remote Parquet row in bounded batches"
    )
    parser.add_argument(
        "--checksum", action="store_true", help="also verify whole-object SHA-256 from the saved report"
    )
    parser.add_argument(
        "--iceberg-scan",
        action="store_true",
        help="also run an independent PyIceberg scan for up to 10 rows/table",
    )
    parser.add_argument(
        "--require-complete",
        action="store_true",
        help="require this run to have loaded all 8/24 tables, with none skipped",
    )
    args = parser.parse_args(argv)
    redactor.add(args.token)
    configure_logging(redactor)
    try:
        import pyarrow.parquet as pq

        report = json.loads(args.report.read_text(encoding="utf-8"))
        if report.get("report_format_version") != 1 or report.get("command") != "load":
            raise ValueError("Expected a report-format-v1 load report")
        uri = args.catalog_uri or report.get("catalog_uri")
        if not uri:
            raise ValueError("Set ICEBERG_URI or --catalog-uri")
        remote_uri(str(uri).rstrip("/") + "/v1/config")
        namespace = tuple(report["namespace"])
        definitions = inventory(report["benchmark"])
        selected = {name: row for name, row in report["tables"].items() if row["status"] == "succeeded"}
        if not selected:
            raise ValueError("This report contains no successfully loaded tables to verify")
        if args.require_complete and set(selected) != set(definitions):
            raise ValueError(
                "The report does not contain a complete new 8/24-table load; skipped/failed tables are not equivalent"
            )
        properties = {"uri": uri, "http.timeout": str(args.timeout), "py-io-impl": CROWDB_FILE_IO}
        if args.token:
            properties["token"] = args.token
        for entry in args.catalog_property:
            if "=" not in entry:
                raise ValueError("--catalog-property requires KEY=VALUE")
            key, value = entry.split("=", 1)
            if key in {"uri", "token", "type"}:
                raise ValueError("Use --catalog-uri / --token, not reserved catalog properties")
            properties[key] = value
        if args.py_io_impl:
            properties["py-io-impl"] = args.py_io_impl
        redactor.learn(properties)
        catalog = create_catalog("crowdb_tpc_independent_verifier", args.timeout, properties)
        redactor.learn(catalog.properties)
        total_rows = 0
        for name, row in selected.items():
            table = catalog.load_table((*namespace, name))
            redactor.learn(getattr(table.io, "properties", {}))
            if str(table.metadata.table_uuid) != row.get("table_uuid"):
                raise ValueError(f"{name}: table was replaced since this load report")
            expected = {}
            parts = {}
            for part in row["files"]:
                location = remote_uri(part["remote_uri"])
                if location in expected or part.get("state") != "registered":
                    raise ValueError(f"{name}: duplicate or unverified file in report")
                expected[location] = (part["row_count"], part["size_bytes"])
                parts[location] = part
            state = inspect_inventory(table)
            verify_inventory(state, expected)
            rows = 0
            for location, part in parts.items():
                remote = table.io.new_input(location)
                if len(remote) != part["size_bytes"]:
                    raise ValueError(f"{name}: stored object size changed")
                with remote.open() as stream:
                    footer = pq.read_metadata(stream)
                if footer.num_rows != part["row_count"]:
                    raise ValueError(f"{name}: remote footer row count changed")
                if args.full_read:
                    with remote.open() as stream:
                        decoded = sum(
                            batch.num_rows for batch in pq.ParquetFile(stream).iter_batches(batch_size=65536)
                        )
                    if decoded != part["row_count"]:
                        raise ValueError(f"{name}: decoded row count differs from the report")
                if args.checksum:
                    algorithm = "md5" if part.get("md5") else "sha256"
                    if not part.get(algorithm):
                        raise ValueError(f"{name}: this report has no upload checksum")
                    with remote.open() as stream:
                        count, digest = hash_stream(stream, 8 * 1024**2, algorithm)
                    if count != part["size_bytes"] or digest != part[algorithm]:
                        raise ValueError(f"{name}: remote checksum differs from uploaded bytes")
                rows += part["row_count"]
            if args.iceberg_scan:
                sample = table.scan(limit=10).to_arrow()
                if sample.num_rows != min(10, rows):
                    raise ValueError(f"{name}: independent Iceberg sample row count differs")
            total_rows += rows
            print(
                redactor.text(
                    f"PASS {'.'.join((*namespace, name))}: snapshot={state.snapshot_id}, files={len(parts)}, rows={rows:,}"
                )
            )
        print(f"Verified {len(selected)} successfully loaded tables and {total_rows:,} manifest/footer rows.")
        print("Local Parquet was not used. All operations were read-only.")
        if not args.full_read:
            print(
                "This was footer/sample verification, not a full-row data scan; use --full-read to decode all rows."
            )
        return 0
    except Exception as exc:
        print(redactor.text(f"VERIFY FAILED: {type(exc).__name__}: {exc}"), file=sys.stderr)
        return 4


if __name__ == "__main__":
    raise SystemExit(main())
