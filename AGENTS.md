# CrowDB TPC Loader

- Keep CLI examples, package metadata, and the website guide consistent.
- Generate and validate the full dataset before creating benchmark tables.
- Uploads and commits run concurrently by table. A failed table does not roll back another table; preserve accurate per-table reports and uncertain commit states.
- Journal the remote URI before upload and the uncertain state before commit. Never retry an ambiguous `add_files` call.
- Run `ruff check src tests scripts`, `ruff format --check src tests scripts`, and `python -m pytest` before a release. Run a fresh container load and SQL read when changing remote behavior.
- Publish only through the manual PyPI workflow from a release/v<version> branch matching the package version. Never store PyPI credentials in the repository.
- Keep docs short; use README for usage, `docs/RECOVERY.md` for failure handling, and `docs/TESTING.md` for verification commands.
