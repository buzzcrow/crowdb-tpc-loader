# CrowDB TPC Loader

Generate standard TPC-H or TPC-DS tables as Parquet, then upload and register them through CROWDB's Iceberg REST Catalog. The tool has two commands: `generate` for local Parquet and `load` for a complete remote import. It does not run benchmark queries or claim TPC certification.

## Install

Use Python 3.10 or later. From this repository:

```sh
python3 -m venv .venv
. .venv/bin/activate
python -m pip install --only-binary=:all: -e .
```

The first TPC-H run obtains the `tpchgen-cli` 3.0.0 binary if none is installed. The first TPC-DS run may install DuckDB's official `tpcds` extension. Use `--no-download` and supply those dependencies ahead of time for an offline run. This package is not published to PyPI.

## Load a small dataset into CROWDB

Start a CROWDB Iceberg container and put the `ICEBERG_URI` and `ICEBERG_TOKEN` values from `crowdb-monitor credentials show --format env` in your shell. Treat the token as a secret. Use a fresh namespace for each import:

```sh
crowdb-tpc-loader load --benchmark tpch --sf 0.01 \
  --namespace tpch_demo --report-file ./tpch-demo.json
crowdb-tpc-loader load --benchmark tpcds --sf 0.01 \
  --namespace tpcds_demo --report-file ./tpcds-demo.json
```

Each command generates and validates the whole benchmark dataset, creates its 8 or 24 unpartitioned Iceberg tables, uploads one or more Parquet files per table, then registers them in one snapshot commit per table. Local Parquet footers are validated before upload. The server checks each upload's signed payload or supplied checksum before accepting it, and PyIceberg reads remote footers during registration. The loader does not download uploaded files for a second checksum pass. Success removes local staging by default. The JSON report records table row counts, remote file locations, snapshots, and any failure. An existing target table makes `load` stop before generation; `--on-exists skip` skips it without verifying or repairing it.

The bundled CrowDB FileIO handles exact-object metadata requests against the native Iceberg endpoint. It does not need S3 bucket listing. The native endpoint has no file DELETE, so `load` starts uploading with the first real table instead of writing an orphaned preflight object. For another storage service, select its FileIO with `--py-io-impl`. The command reads the storage locations and credentials returned by the catalog; it never registers local file paths.

With `--upload-workers 24` (the default), files from different tables can upload concurrently even when each table has only one Parquet file. Each table is committed once, in table order, after all uploads finish. `--upload-workers 1` retains sequential loading. If any upload fails, the concurrent batch is not committed; the report and local staging are kept for inspection. Copy buffers can use roughly `upload-workers × upload-buffer-mib` MiB.

## Read the imported tables

In a fresh Python process with the same environment variables:

```python
import os
from pyiceberg.catalog import load_catalog

catalog = load_catalog(
    "crowdb", type="rest",
    uri=os.environ["ICEBERG_URI"], token=os.environ["ICEBERG_TOKEN"],
    **{"py-io-impl": "crowdb_tpc_loader.crowdb_fileio.CrowdbFileIO"},
)

region = catalog.load_table("tpch_demo.region")
print(region.scan(row_filter="r_regionkey == 1",
                  selected_fields=("r_regionkey", "r_name")).to_arrow().to_pylist())

item = catalog.load_table("tpcds_demo.item")
print(item.scan(selected_fields=("i_item_sk", "i_item_id"), limit=5).to_arrow().to_pylist())
```

For a read-only check of every table, including remote footers and a sample Iceberg scan:

```sh
python scripts/verify_crowdb.py ./tpch-demo.json --require-complete --iceberg-scan
python scripts/verify_crowdb.py ./tpcds-demo.json --require-complete --iceberg-scan
```

## Generate Parquet without uploading

```sh
crowdb-tpc-loader generate --benchmark tpch --sf 0.01 --output-dir ./tpch-001
crowdb-tpc-loader generate --benchmark tpcds --sf 0.01 --output-dir ./tpcds-001
```

The output directory must be new or empty. Each run writes `data/` and `run-summary.json`. Defaults are SF 1, two generator threads, 1 GB DuckDB memory limit, an 8 MiB upload buffer, 24 upload workers for `load`, and a 60-second network timeout. These limits are configurable with `--sf`, `--threads`, `--memory-limit`, `--upload-buffer-mib`, `--upload-workers`, and `--timeout`; the timeout is not a whole-run deadline.

A load commits tables one at a time. A later commit failure leaves earlier committed tables intact and retains the local staging directory for investigation. See [recovery](docs/RECOVERY.md), [compatibility](docs/COMPATIBILITY.md), and the [test record](docs/TEST_REPORT.md) for detail. TPC-H and TPC-DS through SF 10 have been exercised against a local single-node CROWDB container, including spot DuckDB Iceberg queries; distributed deployments still need separate acceptance.

## Develop

```sh
python -m pip install --only-binary=:all: -e '.[dev]'
python -m pytest
ruff check src tests scripts
```

The package uses Apache-2.0; third-party generators and libraries retain their own licenses.
