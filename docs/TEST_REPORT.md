# Test record

This record separates the original package-only test run from the 2026-09-30 local CROWDB run. It does not claim SF 1, distributed deployment, or official benchmark-query acceptance.

## Local single-node run, 2026-09-30

- Built `crowdb-iceberg-single-node:dev` from CROWDB branch `task-single-access-server`, revision `1bb2099d`, version `0.2.0`, with `pixi run build-single-node-container`. The container was healthy on local ports 80 and 81.
- Host: Linux x86_64, Intel Core i9-7960X, 32 logical CPUs, 62 GiB RAM. Loader used its default two generator threads and 1 GB DuckDB limit. Dependencies: Python 3.12.3, PyArrow 23.0.1, PyIceberg 0.10.0, DuckDB 1.5.6, `tpchgen-cli` 3.0.0.
- TPC-H SF 0.01: 8 tables, 8 Parquet files, 86,805 rows, 3,233,124 uploaded bytes. A clean `load` using the bundled exact-object FileIO succeeded in 29.81 seconds wall time. Report: `/tmp/crowdb-tpc-tpch-load-clean.json` on the test host.
- TPC-DS SF 0.01: 24 tables, 24 Parquet files, 277,976 rows, 3,856,545 uploaded bytes. `load` succeeded in 75.83 seconds wall time. Report: `/tmp/crowdb-tpc-tpcds-load.json` on the test host.
- A new Python process loaded both tables and selected remote rows from `tpch_smoke_001.region` and `tpcds_smoke_001.item` through PyIceberg. The independent verifier also checked all 32 committed tables after local staging was deleted: manifest identity and counts, Parquet footer, full decoded row count, whole-object SHA-256, and an Iceberg scan. TPC-H verification took 2.28 seconds; TPC-DS took 6.69 seconds.
- For these tiny files, table registration dominates elapsed time: TPC-H table steps total 26.96 of 29.0 report seconds, median 3.33 seconds per table; TPC-DS table steps total 70.93 of 75.0 seconds, median 2.85 seconds per table. This is fixed per-table overhead in this single-node setup; these runs do not isolate which metadata round trip is slow or predict large-scale throughput.

Two bugs were reproduced and fixed locally: the TPC-DS `store` schema expected `s_tax_precentage` instead of `s_tax_percentage`; and PyArrow's default S3 `get_file_info` issued `ListObjectsV2` against the native Iceberg file endpoint during an exact-object existence check. The bundled FileIO now uses exact-object opens, and the loader avoids an undeletable preflight object on this native endpoint. Full native prefix listing is tracked separately in CROWDB R194.

## Earlier package-only run

The original delivery ran 176 passing tests with 8 skips, but had no PyArrow, DuckDB, PyIceberg, generator, or CROWDB container available. Its raw output remains under [`test-results/`](test-results/). Those records describe the earlier environment only, not the later local container run.

## Remaining acceptance

Run SF 1 and larger scales separately, then test distributed CROWDB and an independent SQL engine. The loader's successful generation and Iceberg reads do not establish standard TPC-H or TPC-DS query compliance or performance.
