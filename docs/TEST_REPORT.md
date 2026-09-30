# Test record for this delivery

Version: **0.1.0rc1**. This document records only results actually obtained in the current environment. It does not represent end-to-end CrowDB acceptance.

## Executed

| Item | Result |
|---|---|
| Local pytest suite | **176 passed, 8 skipped** |
| Source statement coverage | **72% (1,869 statements, 520 missed)**; this does not cover every real dependency path |
| Python | 3.13.5, Linux x86_64 |
| Python 3.10 syntax check | All `src/` and `scripts/` files passed an AST grammar check; a 3.10 interpreter was not run |
| Real local HTTP tests | HEAD, range GET, conditional PUT, DELETE, authentication isolation, redirect rejection, upload failure, and overwrite protection passed |
| Failure paths | Partial commits, uncertain commit results, a single `add_files` call, directory ownership, concurrent output-directory protection, and report redaction passed; some Catalog/footer tests used explicitly identified stand-ins |
| Packaging and isolated installation | Wheel and sdist builds passed; primary entry point, both subcommand help screens, and module entry point passed in an isolated venv |

Raw output: [`test-results/pytest.txt`](test-results/pytest.txt), [`test-results/pytest.xml`](test-results/pytest.xml), [`test-results/coverage.json`](test-results/coverage.json), and [`test-results/environment.json`](test-results/environment.json).

Command:

```bash
python -m pytest -q --disable-warnings \
  --junitxml=docs/test-results/pytest.xml \
  --cov=crowdb_tpc_loader --cov-report=term-missing \
  --cov-report=json:docs/test-results/coverage.json
```

## Not executed; success must not be inferred

The current environment could not resolve external package sources or download dependencies. PyArrow, DuckDB, PyIceberg, and tpchgen-cli were not installed, and no CrowDB server address or credentials were available.

- `test_parquet.py` and `test_iceberg_http.py` each produced one **module-level skip**. An entire parametrized group covering 32 tables was not collected; this does not mean only two tables were untested.
- Four real-generator cases (two benchmarks × small SF/SF1) were not run. Actual generator output schemas, row counts, platform wheels, and extension installation remain unverified.
- Two real CrowDB SF1 cases were not run. CrowDB's `stage-create` support, current FileIO protocol, upload/registration, and independent Iceberg client reads remain unverified.
- Actual PyIceberg 0.10.x API adaptation, the HTTP Arrow filesystem bridge, and real `add_files` still need integration tests after dependencies are installed. Unit tests do not replace these checks.
- Ruff, GitHub Actions, Python 3.10/3.12 runtime tests, peak memory tests, and large-scale performance tests were not run.
- `requirements-integration.txt` is a candidate dependency matrix, not a set of versions confirmed in testing. The package was not published to PyPI, and name availability was not checked.

## Installation check record

After building with setuptools, the project wheel was installed with `pip install --no-deps` in an isolated venv outside the repository. `--version`, the main command and both subcommands' `--help`, and `python -m crowdb_tpc_loader --version` were checked. Results are recorded in [`test-results/package-smoke.txt`](test-results/package-smoke.txt).

`--no-deps` verifies packaging and entry points only. It does not mean generate/load work without runtime dependencies. See [`TESTING.md`](TESTING.md) for acceptance steps after installing all dependencies.

## Acceptance sequence

Install all dependencies and run default pytest so the real Parquet and local Iceberg/HTTP tests are no longer skipped. Then run both real generators at a small SF, test a small CrowDB load in a new namespace, and finally run both SF1 datasets with independent remote reads. If a compatibility error occurs, keep the report; do not bypass it by registering local file paths or disabling duplicate checks.
