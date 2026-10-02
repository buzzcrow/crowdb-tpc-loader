# CrowDB TPC Loader

Generate TPC-H or TPC-DS Parquet, upload it to CROWDB Iceberg, and register complete tables through the REST Catalog. The loader creates 8 TPC-H or 24 TPC-DS tables. It does not run benchmark SQL queries or claim TPC certification.

- Repository: [buzzcrow/crowdb-tpc-loader](https://github.com/buzzcrow/crowdb-tpc-loader)
- End-to-end guide: [CROWDB TPC loader documentation](https://crowdb.dev/docs/tpc-loader/)

## Install

Python 3.10–3.12 is required. Install the published package from PyPI:

```sh
python3 -m venv .venv
. .venv/bin/activate
python -m pip install crowdb-tpc-loader
```

For local development, install from a checkout with `python -m pip install --only-binary=:all: -e .`. The first TPC-H run may download `tpchgen-cli` 3.0.0; TPC-DS may download DuckDB's `tpcds` extension. Use `--no-download` and provide these components ahead of time for an offline run.

## Load

Start `crowdb/crowdb-iceberg:latest` and export the `ICEBERG_URI` and `ICEBERG_TOKEN` values printed by `docker exec <container> crowdb-monitor credentials show --format env`. Keep the token private. Use a new namespace for each run:

```sh
crowdb-tpc-loader load --benchmark tpch --sf 1 \
  --namespace tpch_demo --report-file ./tpch-demo.json
crowdb-tpc-loader load --benchmark tpcds --sf 1 \
  --namespace tpcds_demo --upload-workers 4 --report-file ./tpcds-demo.json
```

The loader validates the entire dataset before creating a table. TPC-H and TPC-DS use the same load flow. Different tables calculate MD5 and upload concurrently, with 8 workers by default. `--upload-workers N` selects 1–24 scheduled table writes; the S3 path caps active workers and connections at 8. One S3 client shares its connection pool, with each request signed using that table's catalog credentials. Files stream from disk without a whole-file buffer. S3 PUT and multipart parts send `Content-MD5`; file payload SHA256 is disabled while SigV4 authentication remains enabled. PUT uses `If-None-Match: *` without an existence HEAD.

Each table's files are registered in one snapshot. Required recovery records are durable before table creation, each upload and commit; concurrently ready records share one report save. Catalog configuration and namespace setup happen once per run. A successful commit reuses its returned table metadata, then verifies the snapshot's file inventory. Ambiguous commits are checked without repeating `add_files`. One table's failure does not roll back successful tables. See [recovery](docs/RECOVERY.md) before retrying. An existing table stops the default load; `--on-exists skip` leaves it unchanged without verifying it.

After all tables are committed and verified, local staging is removed unless `--keep-files` is set. Failed and uncertain runs retain local files. Reports contain `md5`, `md5_duration_seconds`, `transfer_duration_seconds`, and the combined `upload_duration_seconds`. For non-S3 FileIO the checksum is calculated inline while copying, and separate checksum time is unavailable. Performance checks use SF=1; record generation, transfer and commit time separately.

For local Parquet only, use `crowdb-tpc-loader generate --benchmark tpch --sf 1 --output-dir ./tpch-sf1`.

Normal load does not download uploaded files to recheck MD5, decode every row, or execute SQL. The server validates `Content-MD5` during upload. PyIceberg reads Parquet footers for registration and small manifest metadata for commit verification.

## Check the result

Run the read-only verifier from a checkout after loading:

```sh
python scripts/verify_crowdb.py ./tpch-demo.json --require-complete --iceberg-scan
```

With DuckDB's `iceberg` and `httpfs` extensions, attach the REST Catalog using its token and query `tpch_demo.region` or run TPC-H Q1 against `tpch_demo.lineitem`. The [website guide](https://crowdb.dev/docs/tpc-loader/) has the SQL. At SF 0.01, DuckDB 1.5.6 ran all 22 TPC-H and 99 TPC-DS queries against the published `latest` container; results matched DuckDB reading the same local Parquet. This is a development check, not a benchmark result. See [compatibility](docs/COMPATIBILITY.md) and the [test record](docs/TEST_REPORT.md).

## Develop and publish

```sh
python -m pip install --only-binary=:all: -e '.[dev,sql-test]'
ruff check src tests scripts
ruff format --check src tests scripts
python -m pytest
python -m build
python -m twine check dist/*
```

CI runs lint, format, tests, and package checks. Publishing is manual through [the PyPI workflow](.github/workflows/publish.yml) from a `release/v<version>` branch after configuring PyPI Trusted Publishing. See [testing](docs/TESTING.md) for optional real generator and CROWDB runs. The package is Apache-2.0; third-party generators and libraries keep their own licenses.

To publish a later version:

1. Keep the existing PyPI Trusted Publisher for project `crowdb-tpc-loader`: GitHub owner `buzzcrow`, repository `crowdb-tpc-loader`, workflow `publish.yml`, environment `pypi`. Keep the matching `pypi` environment in GitHub.
2. Bump the version in `pyproject.toml` and `src/crowdb_tpc_loader/__init__.py`, then run CI. Create and push `release/v<version>` from the commit to publish.
3. In GitHub Actions, open **Publish to PyPI**, click **Run workflow**, select that release branch, then run it. The workflow verifies the branch name against the package version, reruns checks, builds distributions, and publishes through OIDC. No PyPI API token is stored in GitHub.
4. Confirm the files on [PyPI](https://pypi.org/project/crowdb-tpc-loader/), then test `python -m pip install --no-cache-dir crowdb-tpc-loader==<version>` in a clean environment and run `crowdb-tpc-loader --version`.
