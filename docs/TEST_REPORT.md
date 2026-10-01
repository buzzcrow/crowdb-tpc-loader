# Test record

## Published image, October 1, 2026

- Image: `crowdb/crowdb-iceberg:latest`, digest `sha256:e4e7f80367094a35ab54c2bf1b0a9780811f9a668bc6037cfe7ced1b23373718`, Linux amd64 single-node.
- Loader: eight concurrent table writes (`--upload-workers 8`), TPC-H SF 0.01, fresh namespace. All 8 tables committed; independent verifier checked 86,805 rows in manifests and remote Parquet footers, plus sample Iceberg scans after local staging was removed.
- DuckDB CLI: locally built 1.5.6 with `iceberg` and `httpfs`. `region` returned `(1, AMERICA)`. Standard TPC-H Q1 on the imported `lineitem` returned four groups: `(A,F,14876)`, `(N,F,348)`, `(N,O,29181)`, `(R,F,14902)` for `(returnflag, linestatus, count_order)`. The CLI reported 0.075 seconds for that small warm-host run. No baseline or repeated trials were taken.
- Initial 8-worker attempt saw four transient Catalog HTTP 503 responses and partial table success. The loader now retries bounded 503 create responses; a fresh namespace completed. Both reports are local test artifacts, not included in the package.

This proves one integration path and one Q1 execution. It does not establish the full TPC-H suite, benchmark performance, distributed behavior, or TPC certification.
