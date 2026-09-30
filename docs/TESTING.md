# Testing and acceptance

## 1. Default tests

```bash
python -m pip install --only-binary=:all: -e '.[dev,sql-test]'
python -m pytest -ra
python -m pytest --cov=crowdb_tpc_loader --cov-report=term-missing
```

`tests/unit/` covers arguments and redaction, column lists for 32 tables, type and footer validation decisions, directory ownership, disk space checks, bounded copying, binary selection and verification, per-table commits, commit timeouts, partial failures, reporting, and real loopback HTTP requests.

The tests clearly identify stand-ins for the generator, catalog, footer, and PyIceberg base class. These tests exercise decisions and failure paths; the stand-ins are not evidence of real generator or Parquet/Iceberg compatibility.

With the actual dependencies installed, the default suite also runs:

- `tests/integration/test_parquet.py`: real PyArrow tests for empty Parquet schemas across 32 tables, shards, precision and date errors, and corrupt footers.
- `tests/integration/test_iceberg_http.py`: real PyIceberg, PyArrow, SQLite Catalog, and a local HTTP object service. After a real `add_files` call, it removes the local Parquet files and performs an Iceberg scan with a new Catalog client. **This is not a CrowDB test.**

Tests explicitly skip when dependencies are missing; a skip is not a success. A module-level `pytest` skip counts an entire uncollected group as one skip. For example, a skipped 32-table parametrized group does not mean only one table was missed.

## 2. Reproduction with pinned versions

```bash
python -m pip install --only-binary=:all: -r requirements-integration.txt
python -m pip install --no-deps -e .
python -m pytest
```

This file is a candidate version matrix awaiting validation, not a claim that the versions have been tested. After a real successful run, save `pip freeze`, the generator version, platform, and report before adjusting the ranges or publishing a release.

## 3. Run the real generators

This requires network access, permission to download generators/extensions, and sufficient disk space. Some TPC-DS dimension tables do not shrink in proportion to a small SF, so `0.01` is not a free test.

```bash
CROWDB_TPC_GENERATOR_TESTS=1 python -m pytest tests/integration/test_generators.py -ra

# Includes both full SF1 datasets and requires more disk space.
CROWDB_TPC_GENERATOR_TESTS=1 CROWDB_TPC_SF1_TESTS=1 \
  python -m pytest tests/integration/test_generators.py -ra
```

If a tpchgen executable is available, set `TPC_H_GENERATOR=/absolute/path/to/tpchgen-cli`. Otherwise, the tool searches PATH, the cache, and compatible binary wheels.

Manual smoke commands:

```bash
python scripts/smoke_test.py --benchmark tpch --sf 0.01
python scripts/smoke_test.py --benchmark tpcds --sf 0.01
```

## 4. Real CrowDB SF1 acceptance (writes remote data)

Use a dedicated test deployment. The tests create a new namespace for each benchmark and do not delete remote tables or data. Confirm storage capacity, billing, and credential permissions before running. A failed run does not automatically clean up the server.

```bash
export ICEBERG_URI='http://your-test-crowdb'
export ICEBERG_TOKEN='<test-token>'
# Specify FileIO only if the deployment requires it:
# export CROWDB_TPC_FILEIO='crowdb_tpc_loader.http_fileio.HttpFileIO'

CROWDB_TPC_CROWDB_TESTS=1 CROWDB_TPC_SF1_TESTS=1 \
  python -m pytest tests/integration/test_crowdb.py -s -ra
```

These tests perform real loads, check that local working directories were cleaned up, and run the verifier in a new process with `--require-complete --iceberg-scan --full-read`. Confirm that all 8 TPC-H and 24 TPC-DS tables were loaded successfully by this run, rather than counting skipped tables.

Manual small remote smoke test (writes to the server only with `--load`):

```bash
python scripts/smoke_test.py --benchmark tpch --sf 0.01 --load --full-read
```

## 5. Package installation check

```bash
python -m build
python -m venv /tmp/tpc-wheel-test
/tmp/tpc-wheel-test/bin/python -m pip install --only-binary=:all: dist/*.whl
cd /tmp
/tmp/tpc-wheel-test/bin/crowdb-tpc-loader --help
```

In the current offline delivery environment, only a `--no-deps` installation of the project wheel was possible to check the entry point and help output. This cannot run generate/load and does not replace acceptance with all dependencies installed.

## 6. GitHub Actions

`.github/workflows/tests.yml` configures routine unit and local integration tests on Linux with Python 3.10/3.12/3.13, plus a build check. It does not read user tokens, run against real CrowDB, or publish to PyPI. `workflow_dispatch` can explicitly enable a native generator test at 0.01 SF. Full SF1 and real CrowDB tests belong in an environment with confirmed capacity and access.

The workflow file is provided here, but has not been committed to your repository, and GitHub Actions has not been claimed to pass.
