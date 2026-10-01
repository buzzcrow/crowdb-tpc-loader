# CrowDB TPC Loader

Generate TPC-H or TPC-DS Parquet, upload it to CROWDB Iceberg, and register complete tables through the REST Catalog. The loader creates 8 TPC-H or 24 TPC-DS tables. It does not run benchmark SQL queries or claim TPC certification.

- Repository: [buzzcrow/crowdb-tpc-loader](https://github.com/buzzcrow/crowdb-tpc-loader)
- End-to-end guide: [CROWDB TPC loader documentation](https://crowdb.dev/docs/tpc-loader/)

## Install

Python 3.10–3.12 is required. After the first PyPI release:

```sh
python3 -m venv .venv
. .venv/bin/activate
python -m pip install crowdb-tpc-loader
```

Until PyPI publication, install from a checkout with `python -m pip install --only-binary=:all: -e .`. The first TPC-H run may download `tpchgen-cli` 3.0.0; TPC-DS may download DuckDB's `tpcds` extension. Use `--no-download` and provide these components ahead of time for an offline run.

## Load

Start `crowdb/crowdb-iceberg:latest` and export the `ICEBERG_URI` and `ICEBERG_TOKEN` values printed by `docker exec <container> crowdb-monitor credentials show --format env`. Keep the token private. Use a new namespace for each run:

```sh
crowdb-tpc-loader load --benchmark tpch --sf 0.01 \
  --namespace tpch_demo --report-file ./tpch-demo.json
crowdb-tpc-loader load --benchmark tpcds --sf 0.01 \
  --namespace tpcds_demo --report-file ./tpcds-demo.json
```

The loader validates the entire generated dataset before creating a table. It writes different tables concurrently, with 8 workers by default. Use `--upload-workers N` to control concurrent Iceberg table writes (1–24). Each table's files are uploaded and registered in one snapshot, with a durable report checkpoint before each remote side effect. One table's failure does not roll back tables that already succeeded. The report identifies committed, unregistered, and uncertain files; see [recovery](docs/RECOVERY.md) before retrying. An existing table stops the default load; `--on-exists skip` leaves it unchanged without verifying it.

For local Parquet only, use `crowdb-tpc-loader generate --benchmark tpch --sf 0.01 --output-dir ./tpch-001`.

## Check the result

Run the read-only verifier from a checkout after loading:

```sh
python scripts/verify_crowdb.py ./tpch-demo.json --require-complete --iceberg-scan
```

With DuckDB's `iceberg` and `httpfs` extensions, attach the REST Catalog using its token and query `tpch_demo.region` or run TPC-H Q1 against `tpch_demo.lineitem`. The [website guide](https://crowdb.dev/docs/tpc-loader/) has the SQL. A published `latest` image and the local DuckDB 1.5.6 CLI passed an SF 0.01 TPC-H import, independent table verification, and Q1 read on October 1, 2026. This is an integration check, not a performance result. See [compatibility](docs/COMPATIBILITY.md) and the [test record](docs/TEST_REPORT.md).

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

To publish `0.1.0`:

1. In PyPI, create a pending Trusted Publisher for project `crowdb-tpc-loader`: GitHub owner `buzzcrow`, repository `crowdb-tpc-loader`, workflow `publish.yml`, environment `pypi`. Create the `pypi` environment in GitHub.
2. After CI is green, create and push branch `release/v0.1.0` from the commit to publish.
3. In GitHub Actions, open **Publish to PyPI**, click **Run workflow**, select branch `release/v0.1.0`, then run it. The workflow verifies the branch name against the package version, reruns checks, builds distributions, and publishes through OIDC. No PyPI API token is stored in GitHub.
4. Confirm the files on [PyPI](https://pypi.org/project/crowdb-tpc-loader/), then test `python -m pip install --no-cache-dir crowdb-tpc-loader==0.1.0` in a clean environment and run `crowdb-tpc-loader --version`.
