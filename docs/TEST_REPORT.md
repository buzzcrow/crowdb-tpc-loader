# Test record

## Published image, October 1, 2026

- Image: `crowdb/crowdb-iceberg:latest`, digest `sha256:e4e7f80367094a35ab54c2bf1b0a9780811f9a668bc6037cfe7ced1b23373718`, Linux amd64 single-node.
- Published loader 0.1.0: TPC-H SF 0.01 loaded 8 tables and 86,805 rows with 8 workers. All 22 DuckDB 1.5.6 TPC-H queries against the container matched DuckDB reading byte-identical local Parquet files. Q1 returned `(A,F,14876)`, `(N,F,348)`, `(N,O,29181)`, `(R,F,14902)` for `(returnflag, linestatus, count_order)`.
- TPC-DS SF 0.01 loaded 24 tables and 277,976 rows with 4 workers. All 99 DuckDB TPC-DS queries against the container matched DuckDB reading byte-identical local Parquet files.

These are development checks of the container's Iceberg read path and query results. They are not timed benchmark results, distributed behavior tests, or TPC certification.
