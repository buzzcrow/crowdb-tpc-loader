# Testing

CI runs `ruff check src tests scripts`, `ruff format --check src tests scripts`, and `python -m pytest` on Python 3.10, 3.11, and 3.12. It builds and checks the wheel and sdist on Python 3.12. The default tests include unit and local PyArrow/PyIceberg integration tests. Real generators and a CROWDB instance are opt-in.

## Local checks

```sh
python -m pip install --only-binary=:all: -e '.[dev,sql-test]'
ruff check src tests scripts
ruff format --check src tests scripts
python -m pytest -q
python -m build
python -m twine check dist/*
```

Check a built wheel in a fresh virtual environment before release. Confirm both `crowdb-tpc-loader --version` and `crowdb-tpc-loader load --help` work.

## Real generators

These may download generator binaries or extensions and need disk space:

```sh
CROWDB_TPC_GENERATOR_TESTS=1 python -m pytest tests/integration/test_generators.py -ra
```

Unit and quick functional checks may use SF=0.01 or 0.1. Performance comparisons and complete remote acceptance use SF=1; set `CROWDB_TPC_SF1_TESTS=1` to enable the full-size generator cases. Or run `python scripts/smoke_test.py --benchmark tpch --sf 1` for local generation only.

## Real CROWDB load

Use a dedicated disposable instance with `ICEBERG_URI` and `ICEBERG_TOKEN` exported. The tests create fresh remote namespaces and do not delete them:

```sh
CROWDB_TPC_CROWDB_TESTS=1 CROWDB_TPC_SF1_TESTS=1 \
  python -m pytest tests/integration/test_crowdb.py -s -ra
```

For an SF=1 remote run, use `python scripts/smoke_test.py --benchmark tpch --sf 1 --load`. Preserve the JSON report on failure. A skipped integration test is not evidence of remote acceptance.

## Upload acceptance versus normal load

Remote checksum reads, `--full-read` decoding and DuckDB SQL queries are independent opt-in acceptance checks. They are not part of normal load. Use controlled SF=1 datasets for these checks; do not automatically reread large user uploads. Normal S3 load relies on server validation of Content-MD5, reads Parquet footers for registration and verifies the committed manifest inventory.
