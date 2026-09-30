#!/usr/bin/env python3
"""Run a local generation smoke test, or an explicitly requested remote load.

No remote writes unless --load is present. Uses a unique output directory and,
for loads, a unique namespace. It never drops remote data.
"""
from __future__ import annotations

import argparse
from pathlib import Path
import subprocess
import sys
import uuid


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--benchmark",choices=("tpch","tpcds"),required=True)
    parser.add_argument("--sf",default="0.01")
    parser.add_argument("--output-root",type=Path,default=Path("smoke-results"))
    parser.add_argument("--load",action="store_true",help="explicitly enable writes to ICEBERG_URI using ICEBERG_TOKEN")
    parser.add_argument("--py-io-impl")
    parser.add_argument("--tpchgen",type=Path)
    parser.add_argument("--full-read",action="store_true")
    args = parser.parse_args()
    run = args.output_root / (args.benchmark + "-" + uuid.uuid4().hex[:12])
    run.mkdir(parents=True,exist_ok=False)
    command = [sys.executable,"-m","crowdb_tpc_loader","load" if args.load else "generate",
               "--benchmark",args.benchmark,"--sf",args.sf,"--report-file",str(run / "report.json")]
    if args.tpchgen:
        command += ["--tpchgen",str(args.tpchgen)]
    if args.load:
        namespace = "smoke_" + args.benchmark + "_" + uuid.uuid4().hex[:12]
        command += ["--namespace",namespace,"--work-dir",str(run / "staging")]
        if args.py_io_impl:
            command += ["--py-io-impl",args.py_io_impl]
    else:
        command += ["--output-dir",str(run / "generated")]
    print(f"Smoke output: {run.resolve()}",flush=True)
    result = subprocess.run(command,check=False)
    if result.returncode != 0 or not args.load:
        return result.returncode
    verify = [sys.executable,str(Path(__file__).with_name("verify_crowdb.py")),str(run / "report.json"),
              "--iceberg-scan","--require-complete"]
    if args.py_io_impl:
        verify += ["--py-io-impl",args.py_io_impl]
    if args.full_read:
        verify += ["--full-read"]
    return subprocess.run(verify,check=False).returncode


if __name__ == "__main__":
    raise SystemExit(main())
