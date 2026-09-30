# CrowDB TPC Loader: Requirements and Design

## 1. Purpose and Scope

The [CrowDB Iceberg container](https://hub.docker.com/repository/docker/crowdb/crowdb-iceberg/general) provides an Iceberg REST Catalog and storage. Users need a Python command-line tool, installable through pip, that generates standard TPC-H and TPC-DS datasets and loads them as readable Iceberg tables.

The first release of `crowdb-tpc-loader` supports all 8 TPC-H tables and all 24 TPC-DS tables. Users can generate Parquet files alone or generate and load a complete dataset into CrowDB. The tool does not execute benchmark queries or claim official TPC certification. Its own container image, a web UI, TPC-C/TPC-E, and query performance testing are outside this design.

## 2. Requirements

### Functional requirements

1. Select `tpch` or `tpcds` and a positive scale factor (SF), defaulting to 1. Record the generator implementation and version for every run.
2. `generate` exports every table as Parquet to a user-specified persistent directory. Preserve standard table and column names and appropriate types. Report file count, row count, and size for each table.
3. `load` generates data, connects to the CrowDB Iceberg REST Catalog, and loads every table into a namespace. The namespace defaults to the benchmark name and is created if absent.
4. Accept Catalog URI and token through CLI options or `ICEBERG_URI` and `ICEBERG_TOKEN` environment variables. CLI options take precedence. Never log a token.
5. Handle existing tables with `error` (default) or `skip`. `skip` leaves an existing table unchanged. The first release does not automatically drop or replace tables.
6. If one table fails, stop loading further tables. Report succeeded, skipped, and failed tables, plus any uploaded files that were not registered. A rerun must not register the same files twice.
7. Show progress and a final summary. Distinguish success, invalid arguments, and loading failures with exit codes.

### Packaging and operating constraints

- The intended installation command is `pip install crowdb-tpc-loader`; it installs a `crowdb-tpc-loader` executable. Confirm PyPI name availability before publication.
- Installing the Python package must not require a local Rust toolchain. TPC-H uses a published `tpchgen-cli` binary. If the platform has no compatible distribution, fail with a clear message.
- DuckDB may need network access to install the `tpcds` extension on first use. An unavailable extension must produce an explicit error, not an incomplete dataset.
- Before loading, check that the local staging directory is writable and has adequate free space, and that CrowDB FileIO is reachable and writable. If backend free space cannot be queried, report upload failures accurately. Large SF values must not require an entire table in Python memory.

## 3. CLI

```text
crowdb-tpc-loader generate --benchmark {tpch,tpcds} [--sf N] --output-dir DIR
crowdb-tpc-loader load     --benchmark {tpch,tpcds} [--sf N]
                           [--catalog-uri URI] [--token TOKEN]
                           [--namespace NAME] [--on-exists {error,skip}]
                           [--work-dir DIR] [--keep-files]
```

`generate` requires `--output-dir` and refuses to overwrite a nonempty directory. `load` creates an isolated temporary working directory by default. `--work-dir` selects the staging location; `--keep-files` retains generated local files after success. On failure, retain the working directory and print its path. Removing local staging files must never remove remote files referenced by Iceberg.

```bash
export ICEBERG_URI=http://localhost
export ICEBERG_TOKEN='<token>'

pip install crowdb-tpc-loader
crowdb-tpc-loader load --benchmark tpch --sf 1
crowdb-tpc-loader load --benchmark tpcds --sf 1 --namespace tpcds_sf1
crowdb-tpc-loader generate --benchmark tpch --sf 10 --output-dir ./tpch-sf10
```

Obtain credentials from the CrowDB container and use the deployment's actual address and port. The current single-node guide states that port 80 serves both the REST Catalog and Iceberg FileIO and that clients use `ICEBERG_URI` and `ICEBERG_TOKEN`. For remote deployments, clients must also reach the FileIO URLs returned by the Catalog. REST access alone is insufficient for loading data. [CrowDB single-node container guide](https://github.com/buzzcrow/crowdb/blob/main/doc/user-manual/docker-single-node-user-guide.md)

## 4. Architecture

```text
CLI
 ├─ Argument and connection preflight
 ├─ TPC-H generator (tpchgen-cli) / TPC-DS generator (DuckDB tpcds)
 │    └─ Local Parquet working directory
 ├─ Validation (table inventory, schema, row counts, sizes)
 └─ Iceberg loader (PyIceberg)
      ├─ REST Catalog: namespaces, tables, snapshot metadata
      └─ Table FileIO: upload durable Parquet, then commit with add_files
```

The REST Catalog manages table and snapshot metadata. Parquet data must remain readable by Iceberg clients after the loader exits. `add_files` registers **existing** Parquet files; it does not upload local files to CrowDB. Therefore `load` must stream generated Parquet bytes through the target table's FileIO into durable storage, verify each upload, and call `add_files` with the **remote file URIs**. Never commit local temporary paths to Iceberg manifests. [PyIceberg Add Files documentation](https://py.iceberg.apache.org/api/#adding-files)

Use table locations and FileIO settings returned by the CrowDB REST Catalog. Do not guess paths inside the container or require users to mount its data volume. If the selected CrowDB and PyIceberg versions cannot upload remote Parquet, read its footer, and register it through the available FileIO, `load` must report a compatibility error before committing a table. It must not fall back to registering local paths. `generate` remains usable.

### 4.1 Data generation

**TPC-H:** Invoke `tpchgen-cli parquet` as a subprocess. Build its scale-factor and output-directory arguments for the installed CLI version, check the exit status, and collect Parquet output for the expected 8 tables. Do not assume one file per table. The package's PyPI page includes a Parquet command example. [tpchgen-cli on PyPI](https://pypi.org/project/tpchgen-cli/)

**TPC-DS:** Load DuckDB's official `tpcds` extension in a separate DuckDB database, execute `CALL dsdgen(sf = ...);`, and export each of the 24 tables with `COPY ... TO ... (FORMAT PARQUET)`. Report extension installation and export errors. DuckDB documents a generator change toward TPC-DS v4 in version 2.0; record the DuckDB and extension versions and do not silently treat output from different versions as identical benchmark input. [DuckDB TPC-DS documentation](https://duckdb.org/docs/stable/core_extensions/tpcds)

Both generators return the benchmark, SF, generator version, and each table's file URIs, Parquet schema, footer row counts, and file sizes. Even an empty table needs a Parquet file with the correct schema. File names and splitting behavior are implementation details, not proof of completeness.

### 4.2 Schema and table creation

Generator modules maintain explicit table inventories and column definitions for both benchmarks. Before loading, validate every Parquet footer: all files for a table have compatible schemas, and column names, order, numeric precision, and date types match the relevant benchmark definition. Report differences and stop rather than silently converting incompatible columns.

Create each Iceberg table from the validated Arrow schema, preserving integer types, `DECIMAL` precision and scale, and `DATE` semantics. First-release tables are unpartitioned. Check Iceberg and Parquet schema compatibility before upload and registration.

### 4.3 Loading and commit behavior

1. Validate arguments, generator availability, REST access, and FileIO capabilities. Inspect existing target tables. In `error` mode, exit before generation or upload if any target table exists.
2. Generate and validate the complete dataset. Begin table creation and upload only after all 8 or 24 tables meet the expected requirements.
3. For each table: create it; stream the original Parquet bytes into its durable data location using a unique run ID and file names; verify uploaded size and readability; call `table.add_files(file_paths=remote_uris)`; inspect the resulting snapshot and file inventory; compare file and footer row counts; record success.
4. Keep `check_duplicate_files=True`, its default. Unique remote URIs, the existing-table policy, and pre-commit checks prevent duplicate registration. Disable duplicate checking only if a verified idempotency mechanism is added later. [PyIceberg Add Files documentation](https://py.iceberg.apache.org/api/#adding-files)
5. Commit tables independently. There is no atomic transaction across all 8 or 24 tables. On failure, do not automatically delete committed tables or snapshots. Report unregistered uploaded files as cleanup candidates. Delete an uncommitted file or failed empty table automatically only if no snapshot can reference it.

In `skip` mode, identify skipped tables before the run; do not upload files or modify those tables. If all tables exist, return a summary immediately. If a network timeout makes the commit outcome uncertain, reload the table and its file inventory, check whether the remote URIs were registered, and then report the outcome. Never blindly repeat `add_files`.

### 4.4 State and resource use

- Users own the `generate` output directory. The `load` working directory holds generated files and a run summary with the run ID, benchmark, SF, generator version, table inventory, remote URIs, and per-table status.
- Upload with bounded buffers and process files incrementally. Show separate generation, upload, and registration progress. Limit concurrent uploads to avoid exhausting memory or disk.
- Do not print tokens, signed URLs, or other credentials. Logs may include table names, row counts, sizes, durations, and file locations without sensitive query parameters.
- Declare tested dependency version ranges for PyIceberg, DuckDB, and `tpchgen-cli`; record installed versions in the run summary. Use `pyproject.toml` for package metadata and a `[project.scripts]` entry point. Publish a wheel and source distribution.

## 5. Acceptance Criteria

- `generate` produces all 8 TPC-H or 24 TPC-DS tables, each with readable Parquet schema and row-count statistics.
- Against a running CrowDB Iceberg container, `load --benchmark tpch --sf 1` and `load --benchmark tpcds --sf 1` create the corresponding number of tables. An independent Iceberg client can read their data after the loader exits.
- Removing the local working directory does not affect Iceberg reads; manifests reference durable remote file URIs.
- Existing tables, invalid Catalog credentials, unreachable FileIO, insufficient disk space, missing generators, and partial commits produce actionable errors. No failure path commits local temporary files as Iceberg data files.
- A pip-installed package provides `crowdb-tpc-loader --help` without requiring the source directory.

## 6. References

- [CrowDB Iceberg on Docker Hub](https://hub.docker.com/repository/docker/crowdb/crowdb-iceberg/general)
- [CrowDB single-node container guide](https://github.com/buzzcrow/crowdb/blob/main/doc/user-manual/docker-single-node-user-guide.md)
- [PyIceberg API: Add Files](https://py.iceberg.apache.org/api/#adding-files)
- [DuckDB TPC-DS extension](https://duckdb.org/docs/stable/core_extensions/tpcds)
- [tpchgen-cli on PyPI](https://pypi.org/project/tpchgen-cli/)
