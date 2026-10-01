"""Opt-in, potentially sizeable writes to a NEW CrowDB namespace. No remote cleanup."""

from decimal import Decimal
from pathlib import Path
import os
import subprocess
import sys
import uuid

import pytest

from crowdb_tpc_loader.models import Options
from crowdb_tpc_loader.runner import Runner
from crowdb_tpc_loader.security import Redactor

pytestmark = [pytest.mark.integration, pytest.mark.generator, pytest.mark.crowdb]


@pytest.mark.parametrize("benchmark", ["tpch", "tpcds"])
def test_sf1_load_and_independent_read(benchmark, tmp_path):
    if os.getenv("CROWDB_TPC_CROWDB_TESTS") != "1" or os.getenv("CROWDB_TPC_SF1_TESTS") != "1":
        pytest.skip(
            "requires CROWDB_TPC_CROWDB_TESTS=1 and CROWDB_TPC_SF1_TESTS=1; creates fresh remote tables"
        )
    pytest.importorskip("pyarrow")
    pytest.importorskip("pyiceberg")
    uri = os.environ.get("ICEBERG_URI")
    assert uri, "ICEBERG_URI must identify the test CrowDB deployment"
    namespace = "loader_acceptance_" + benchmark + "_" + uuid.uuid4().hex[:12]
    properties = {}
    if os.getenv("CROWDB_TPC_FILEIO"):
        properties["py-io-impl"] = os.environ["CROWDB_TPC_FILEIO"]
    options = Options(
        "load",
        benchmark,
        Decimal(1),
        work_dir=tmp_path / "work",
        report_file=tmp_path / "load-report.json",
        catalog_uri=uri,
        token=os.getenv("ICEBERG_TOKEN"),
        namespace=(namespace,),
        catalog_properties=properties,
    )
    result = Runner(options, print, Redactor([options.token or ""])).run()
    assert result.exit_code == 0, result.report
    assert result.report.get("work_directory_removed") is True
    assert len(result.report["summary"]["succeeded"]) == (8 if benchmark == "tpch" else 24)
    script = Path(__file__).resolve().parents[2] / "scripts/verify_crowdb.py"
    command = [
        sys.executable,
        str(script),
        str(result.report_path),
        "--iceberg-scan",
        "--full-read",
        "--require-complete",
    ]
    if properties.get("py-io-impl"):
        command += ["--py-io-impl", properties["py-io-impl"]]
    completed = subprocess.run(command, text=True, capture_output=True, check=False)
    assert completed.returncode == 0, completed.stdout + completed.stderr
    # Tables are intentionally retained. The run report gives the exact namespace/files.
    print(f"Acceptance tables retained in namespace {namespace}")
