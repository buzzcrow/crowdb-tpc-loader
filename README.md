# CrowDB TPC Loader

Generate TPC-H / TPC-DS Parquet data and import it into remote storage through an Iceberg REST Catalog and the target tables' FileIO.

**Current version: `0.1.0rc1`, a candidate implementation for local testing and modification, not yet published to PyPI.** The original requirements remain in [`design/design.md`](design/design.md). The code covers the initial two commands, 8/24 tables, type validation, upload, registration, duplicate-import protection, and failure reports. The current delivery environment could not install PyArrow, DuckDB, or PyIceberg, obtain a real generator, or access a CrowDB instance. Therefore, **the bundled unit-test results do not establish real SF1 or CrowDB acceptance**. See [`docs/TEST_REPORT.md`](docs/TEST_REPORT.md) for the actual execution record.

## Installation

Requires Python 3.10 or later and an environment that can install runtime dependencies. Initial TPC-H / TPC-DS generation may require network access.

```bash
python3 -m venv .venv
source .venv/bin/activate
# Windows PowerShell: .venv\Scripts\Activate.ps1

# Install from this repository's root; accept dependency wheels only to avoid local native builds.
python -m pip install --only-binary=:all: .
crowdb-tpc-loader --help
crowdb-tpc-loader --version
```

You can also install the wheel from the `dist/` download bundle:

```bash
python -m pip install --only-binary=:all: ./dist/crowdb_tpc_loader-0.1.0rc1-py3-none-any.whl
```

The download bundle **does not include offline dependencies**. Installation still needs runtime dependencies from a package index. Do not start with `pip install crowdb-tpc-loader`: this release is unpublished, and PyPI name availability was not checked for this delivery.

### TPC-H does not require local Rust

Search order: file specified by `--tpchgen` → `tpchgen-cli` on `PATH` → user cache → download a compatible binary wheel for `tpchgen-cli==3.0.0`. Downloads are checked against SHA-256 and platform tags. Only the native executable is extracted; Python code from the wheel is not installed, Cargo is not run, and there is no source-build fallback.

```bash
# Optional: install the binary distribution yourself first.
python -m pip install --only-binary=:all: tpchgen-cli==3.0.0

# Or use an existing 3.x executable compatible with this machine.
crowdb-tpc-loader generate --benchmark tpch --sf 0.01 \
  --tpchgen /absolute/path/to/tpchgen-cli --no-download --output-dir ./tpch-check
```

If no compatible wheel is available, the tool fails explicitly. Do not bypass this boundary with an automatic source build.

## Generate a small dataset first

```bash
crowdb-tpc-loader generate --benchmark tpch --sf 0.01 --output-dir ./tpch-001
crowdb-tpc-loader generate --benchmark tpcds --sf 0.01 --output-dir ./tpcds-001
```

`--output-dir` must be absent or empty. Output layout:

```text
output-directory/
  data/                  # Native generator output; supports multiple Parquet shards per table
  run-summary.json       # Actual versions, schemas, row counts, sizes, file list, and run status
```

The tool reads every Parquet footer. It validates the complete table list, column names and order, integer types, DECIMAL precision and scale, DATE types, schemas across shards, and row counts. TPC-DS also compares each DuckDB source table's `COUNT(*)`; TPC-H checks dimension and fixed-cardinality tables and lineitem cardinality. A table without a valid empty Parquet file is incomplete, even if it has zero rows. The tool does not silently fabricate data.

TPC-DS uses a separate on-disk DuckDB database. If the first `LOAD tpcds` fails, it tries to install the official extension; `--no-download` prevents that installation. Without the extension, generation fails rather than substituting another generator.

## Import into CrowDB

Use the actual address and port of your deployment. Pass the token through an environment variable to avoid putting it in shell history and process arguments.

```bash
export ICEBERG_URI='http://localhost'
export ICEBERG_TOKEN='<token from your CrowDB deployment>'

crowdb-tpc-loader load --benchmark tpch --sf 0.01 \
  --namespace tpch_try_001 --work-dir ./staging --report-file ./tpch-try-001.json
```

CLI `--catalog-uri` / `--token` take precedence over environment variables. The default namespace is `tpch` or `tpcds`. Dots in a namespace indicate levels; this release allows letters, digits, underscores, and hyphens in each level.

### Verify FileIO compatibility

By default, the tool honors table locations and FileIO configuration returned by the Catalog. For storage such as S3 that PyIceberg supports natively, it uses the corresponding FileIO. If the deployment provides a dedicated Python FileIO, specify it:

```bash
crowdb-tpc-loader load --benchmark tpch --sf 0.01 \
  --namespace tpch_io_test --py-io-impl your_package.YourFileIO
```

The package also includes an **optional standard HTTP object-storage adapter**:

```bash
crowdb-tpc-loader load --benchmark tpch --sf 0.01 \
  --namespace tpch_http_test \
  --py-io-impl crowdb_tpc_loader.http_fileio.HttpFileIO
```

This adapter requires storage to support `HEAD`, range `GET`, conditional `PUT`, and `DELETE`. CrowDB's name alone does not establish that it uses this protocol. **The current CrowDB server's actual FileIO protocol could not be verified in this delivery; compatibility depends on preflight against your deployment.** By default, the HTTP token is sent only to the Catalog's origin. Cross-origin storage requires an explicit trusted `http.auth-origins` setting. See [`docs/COMPATIBILITY.md`](docs/COMPATIBILITY.md).

This release also requires REST `stage-create` support. An uncommitted temporary table transaction provides a valid FileIO and remote location for preflight without publishing a probe table. If unsupported, the tool reports a compatibility error before data generation; it does not guess a path or create a permanent benchmark table for probing.

### What happens during a load

Check existing tables, local write access, and free space → prepare the generator → preflight FileIO with a small write/read/verification → generate and validate the **entire** dataset → create tables one at a time → upload the original Parquet bytes → verify remote size, SHA-256, and footer → call `add_files` → reread snapshot/manifest to check files and row counts.

**Only persistent remote URIs are passed to `add_files`; local paths are never registered.** Uploads are serial with an 8 MiB default buffer, and the tool does not convert entire tables into Python DataFrames. To verify uploads, it reads every remote object back in full to compute SHA-256, adding one full read of network traffic.

After every table succeeds, the tool deletes the local working subdirectory it created by default; `--keep-files` retains the Parquet files. On failure or interruption, it keeps the directory and prints its path. `--work-dir` is a **parent directory**; the tool does not clear it or remove user files in it.

Without `--report-file`, the external load report is kept in the working subdirectory's parent. That parent may default to a system temporary directory and remain subject to system cleanup. Specify a persistent report path for important runs.

## Existing tables and failure recovery

```bash
# Default: stop before generation or upload if any target table exists.
crowdb-tpc-loader load --benchmark tpch --namespace tpch_sf1

# Skip existing tables without modifying them.
crowdb-tpc-loader load --benchmark tpch --namespace tpch_sf1 --on-exists skip
```

**`skip` does not resume or repair a previous run.** An empty table left by a failed run still counts as existing and will be skipped. The existing table's row count, SF, and completeness are not thereby verified. Use a new namespace for initial acceptance.

Tables are committed independently. If one fails, later tables are not processed, and earlier successful tables remain. After a commit timeout, the tool rereads the remote manifest to check the result; it does not blindly call `add_files` again. An unconfirmed result is marked `uncertain`; do not directly delete its files. The tool does not automatically delete benchmark tables, snapshots, or data files. It cleans up only a unique preflight object known never to have been committed.

Report fields `summary.unregistered_uploads`, `uncertain_uploads`, and `tables_requiring_inspection` support manual investigation; they are not a bulk-deletion list. See [`docs/RECOVERY.md`](docs/RECOVERY.md).

## Verify remote data in an independent process

After a successful load and the default local cleanup, run:

```bash
python scripts/verify_crowdb.py ./tpch-try-001.json \
  --require-complete --iceberg-scan --full-read --checksum
```

The script reconnects to the Catalog, verifies table UUIDs, current manifest files, footers, and row counts, decodes remote Parquet in batches, and can perform PyIceberg reads and SHA-256 checks. It uses no local Parquet files and modifies no remote objects. If a custom FileIO was used for loading, pass the same `--py-io-impl` for verification and, if needed, the same non-sensitive Catalog properties.

Full SF1 acceptance:

```bash
crowdb-tpc-loader load --benchmark tpch --sf 1 --namespace tpch_sf1_test \
  --report-file ./tpch-sf1-test.json
crowdb-tpc-loader load --benchmark tpcds --sf 1 --namespace tpcds_sf1_test \
  --report-file ./tpcds-sf1-test.json

python scripts/verify_crowdb.py ./tpch-sf1-test.json --require-complete --iceberg-scan --full-read
python scripts/verify_crowdb.py ./tpcds-sf1-test.json --require-complete --iceberg-scan --full-read
```

## Arguments and resources

| Argument | Default / behavior |
|---|---|
| `--sf` | `1`; positive; original decimal value recorded; the generator may accept a narrower range |
| `--on-exists` | `error`; `skip` is also available |
| `--threads` | `2`; native generator thread limit |
| `--memory-limit` | `1GB`; DuckDB memory limit, not a hard RSS limit for the process |
| `--upload-buffer-mib` | `8`; load accepts 1–64 |
| `--timeout` | `60` seconds for network requests and generator probes, not a total generation time limit |
| `--no-download` | Do not download the TPC-H binary or install the TPC-DS extension; does not prepare offline dependencies automatically |
| `--report-file` | Path for a new JSON report; refuses to overwrite an existing file |
| `--catalog-property K=V` | Repeatable; passes deployment-specific PyIceberg properties; do not use it to override uri/token |
| `--json` / `--quiet` | Output final JSON on stdout / hide phase progress; errors and the summary remain visible |

Local space estimate: approximately `3 GiB × SF + 256 MiB` for TPC-H and `8 GiB × SF + 2 GiB` for TPC-DS. It accounts for the database, Parquet, spill, and possible HTTP disk spooling. This is a **heuristic preflight, not a capacity guarantee**. Data distribution, compression, generator version, and concurrency affect actual needs. No generic remote free-space API is available; upload failures are recorded.

Exit codes: `0` success/all skipped, `2` argument error, `3` generation or data-validation failure, `4` Catalog/FileIO/import failure, `5` local-resource preflight failure, `130` interruption, `1` unclassified internal error. If some tables succeed and a later one fails, the overall exit code remains nonzero.

## Development and testing

```bash
python -m pip install --only-binary=:all: -e '.[dev,sql-test]'
python -m pytest
python -m build
```

Default tests do not generate large datasets or access real CrowDB. With runtime dependencies installed, real Parquet and SQLite Catalog + HTTP FileIO integration tests run automatically. Real-generator and CrowDB SF1 tests must be explicitly enabled. See [`docs/TESTING.md`](docs/TESTING.md) for steps and network and disk requirements.

Main modules: `generators/` for generation, `schemas.py` for 32 table definitions, `validation.py` for footer checks, `backend.py` for PyIceberg adaptation, `transfers.py` for upload, `loader.py` for commit and verification, `runner.py` for orchestration, and `report.py` for atomic checkpoints. See [`docs/IMPLEMENTATION.md`](docs/IMPLEMENTATION.md) for the mapping to the design.

## Scope and license

The tool does not run benchmark queries, compare query performance, or claim official TPC certification. It does not include a Docker image, Web UI, TPC-C, or TPC-E. The project follows the uploaded repository's Apache-2.0 license. Third-party generators and libraries retain their own licenses; this package does not redistribute their binaries.
