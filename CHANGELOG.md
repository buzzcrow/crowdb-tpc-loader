# Changelog

## 0.1.0rc1 — 2026-09-30

Initial implementation against the uploaded `design/design.md`.

- Package metadata, `generate` / `load` CLI and documented exit codes.
- Explicit TPC-H (8) / TPC-DS (24) inventories and logical column schemas.
- Published-binary-only TPC-H adapter and disk-backed DuckDB TPC-DS adapter.
- Multipart Parquet validation, source/footer counts, strict decimal/date semantics.
- Catalog-derived durable locations, bounded uploads, SHA-256/footer checks, `add_files` and snapshot reconciliation.
- Existing-table policy, fail-stop per-table commits, uncertainty-aware reports, secret redaction and safe local cleanup.
- Optional standard HTTP FileIO and read-only Arrow filesystem bridge.
- Offline unit/failure-injection tests, explicit real integration tests, independent read-only verifier, smoke script, CI configuration, wheel/sdist.

This is a candidate implementation, not a declaration of CrowDB/SF1 acceptance. The delivery environment lacked runtime data dependencies, native generators and a CrowDB deployment. See `docs/TEST_REPORT.md` and `docs/COMPATIBILITY.md`.
