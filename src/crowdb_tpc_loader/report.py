"""Atomic, secret-free checkpoints and final reports."""

from __future__ import annotations

import json
import os
import tempfile
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from .models import Options, TableData
from .schemas import inventory
from .security import Redactor
from .util import versions


def timestamp() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def atomic_json(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, temporary = tempfile.mkstemp(prefix=f".{path.name}.", dir=path.parent)
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as stream:
            json.dump(payload, stream, ensure_ascii=False, indent=2, allow_nan=False)
            stream.write("\n")
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(temporary, path)
    finally:
        Path(temporary).unlink(missing_ok=True)


class RunReport:
    def __init__(self, options: Options, run_id: str, redactor: Redactor):
        self.redactor = redactor
        self.paths: list[Path] = []
        self.data: dict[str, Any] = {
            "report_format_version": 1,
            "run_id": run_id,
            "command": options.command,
            "benchmark": options.benchmark,
            "scale_factor": str(options.sf),
            "namespace": list(options.namespace),
            "catalog_uri": options.catalog_uri,
            "on_exists": options.on_exists,
            "status": "running",
            "phase": "preflight",
            "started_at": timestamp(),
            "dependencies": versions(),
            "generator": None,
            "work_directory": None,
            "tables": {name: {"status": "pending", "files": []} for name in inventory(options.benchmark)},
            "probes": [],
            "warnings": [],
            "errors": [],
        }

    def save(self) -> None:
        self.data["updated_at"] = timestamp()
        safe = self.redactor.data(self.data)
        for path in self.paths:
            atomic_json(path, safe)

    def phase(self, value: str) -> None:
        self.data["phase"] = value
        self.save()

    def table(self, name: str) -> dict[str, Any]:
        return self.data["tables"][name]

    def generated_table(self, table: TableData, root: Path) -> None:
        row = self.table(table.name)
        if row["status"] != "skipped":
            row["status"] = "generated"
        row.update(
            {
                "row_count": table.rows,
                "size_bytes": table.size_bytes,
                "file_count": len(table.parts),
                "schema": [
                    {"name": field.name, "type": str(field.type), "nullable": field.nullable}
                    for field in table.schema
                ],
                "files": [
                    {
                        "local_path": str(part.path.relative_to(root)),
                        "row_count": part.rows,
                        "size_bytes": part.size_bytes,
                        "remote_uri": None,
                        "state": "local",
                    }
                    for part in table.parts
                ],
            }
        )
        self.save()

    def finish(self, status: str, error: str | None = None) -> None:
        self.data["status"] = status
        self.data["finished_at"] = timestamp()
        if error:
            self.data["errors"].append(error)
        self.data["summary"] = self.summary()
        self.save()

    def summary(self) -> dict[str, Any]:
        tables = self.data["tables"]
        result: dict[str, Any] = {}
        for status in ("succeeded", "skipped", "failed", "uncertain"):
            result[status] = [name for name, row in tables.items() if row["status"] == status]
        result["not_loaded"] = [
            name
            for name, row in tables.items()
            if row["status"] not in {"succeeded", "skipped", "failed", "uncertain"}
        ]
        result["tables_requiring_inspection"] = [
            {
                "table": name,
                "identifier": row.get("table_identifier"),
                "creation_state": row.get("table_creation_state"),
                "status": row["status"],
            }
            for name, row in tables.items()
            if row.get("table_creation_state") and row["status"] not in {"succeeded", "skipped"}
        ]
        result["generated_rows"] = sum(row.get("row_count", 0) for row in tables.values())
        result["generated_bytes"] = sum(row.get("size_bytes", 0) for row in tables.values())
        result["unregistered_uploads"] = []
        result["uncertain_uploads"] = []
        for name, row in tables.items():
            for part in row["files"]:
                uri = part.get("remote_uri")
                if uri and part["state"] not in {"registered", "local"}:
                    key = "uncertain_uploads" if part["state"] == "commit_unknown" else "unregistered_uploads"
                    result[key].append({"table": name, "uri": uri, "state": part["state"]})
        result["probe_cleanup_candidates"] = [p for p in self.data["probes"] if p["state"] != "deleted"]
        return result
