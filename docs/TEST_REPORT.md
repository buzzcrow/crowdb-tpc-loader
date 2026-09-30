# Test record

This record separates the original package-only test run from local CROWDB single-node runs on 2026-09-30 and 2026-10-01. It does not claim distributed deployment or official benchmark-query acceptance.

## Local single-node run, 2026-09-30

- Built `crowdb-iceberg-single-node:dev` from CROWDB branch `task-single-access-server`, revision `1bb2099d`, version `0.2.0`, with `pixi run build-single-node-container`. The container was healthy on local ports 80 and 81.
- Host: Linux x86_64, Intel Core i9-7960X, 32 logical CPUs, 62 GiB RAM. Loader used its default two generator threads and 1 GB DuckDB limit. Dependencies: Python 3.12.3, PyArrow 23.0.1, PyIceberg 0.10.0, DuckDB 1.5.6, `tpchgen-cli` 3.0.0.
- TPC-H SF 0.01: 8 tables, 8 Parquet files, 86,805 rows, 3,233,124 uploaded bytes. A clean `load` using the bundled exact-object FileIO succeeded in 29.81 seconds wall time. Report: `/tmp/crowdb-tpc-tpch-load-clean.json` on the test host.
- TPC-DS SF 0.01: 24 tables, 24 Parquet files, 277,976 rows, 3,856,545 uploaded bytes. `load` succeeded in 75.83 seconds wall time. Report: `/tmp/crowdb-tpc-tpcds-load.json` on the test host.
- A new Python process loaded both tables and selected remote rows from `tpch_smoke_001.region` and `tpcds_smoke_001.item` through PyIceberg. The independent verifier also checked all 32 committed tables after local staging was deleted: manifest identity and counts, Parquet footer, full decoded row count, whole-object SHA-256, and an Iceberg scan. TPC-H verification took 2.28 seconds; TPC-DS took 6.69 seconds.
- For these tiny files, table registration dominates elapsed time: TPC-H table steps total 26.96 of 29.0 report seconds, median 3.33 seconds per table; TPC-DS table steps total 70.93 of 75.0 seconds, median 2.85 seconds per table. This is fixed per-table overhead in this single-node setup; these runs do not isolate which metadata round trip is slow or predict large-scale throughput.

Two bugs were reproduced and fixed locally: the TPC-DS `store` schema expected `s_tax_precentage` instead of `s_tax_percentage`; and PyArrow's default S3 `get_file_info` issued `ListObjectsV2` against the native Iceberg file endpoint during an exact-object existence check. The bundled FileIO now uses exact-object opens, and the loader avoids an undeletable preflight object on this native endpoint. Full native prefix listing is tracked separately in CROWDB R194.

## Concurrent upload and larger scales, 2026-10-01

- The CLI now uploads up to 24 table files concurrently by default, then commits each table once in a deterministic order. The server checks signed payloads or declared checksums. The loader no longer downloads each uploaded object for another digest pass; independent verification below remains a separate audit step.
- TPC-H SF 0.01: 8 tables and 86,805 rows loaded in 31 s. TPC-H SF 1: 8 tables and 8,661,245 rows loaded in 50 s. Parallelism did not improve these small single-node runs; the comparison with the sequential runs above was not controlled for cache or container state.
- TPC-H SF 10: 8 tables, 86,586,082 rows, and 3,635,609,132 compressed Parquet bytes loaded in 232 s. The upload phase before the first table commit took about 176 s, or 20.7 MB/s overall. The independent verifier checked all eight committed manifests, Parquet footers, row counts, object digests, and sample Iceberg scans. Report: `/tmp/crowdb-tpc-tpch-parallel-sf10-fixed.json`.
- An earlier SF 10 attempt had uploaded every file but failed to commit `lineitem`: its 466,271-byte footer with 229 row groups exceeded CROWDB's 100,000-value parser budget. CROWDB raised that budget to 500,000 while keeping the 1 MiB footer bound, and the same remote file then committed. The original report and staging were retained. This is an operational recovery record, not a successful loader run.
- TPC-DS SF 1: 24 tables, 19,557,579 rows, and 277,305,727 compressed Parquet bytes loaded in 157 s. The independent verifier passed all 24 tables. Report: `/tmp/crowdb-tpc-tpcds-parallel-sf1.json`.
- TPC-DS SF 10 first attempt generated 191,500,208 rows and 2,774,412,209 compressed Parquet bytes, but 24 simultaneous multipart initiations hit `SlowDown` on shared server admission. The loader committed no table in that batch and retained its report and staging for inspection. Report: `/tmp/crowdb-tpc-tpcds-parallel-sf10.json`.
- A server admission retry fix passed a 24-way concurrent multipart create regression test. A fresh TPC-DS SF 10 load succeeded: 24 tables, 191,500,208 rows, 2,774,412,209 compressed Parquet bytes, and 453 s wall time. Independent manifest, footer, and sample Iceberg scan verification passed all 24 tables without local Parquet staging. Report: `/tmp/crowdb-tpc-tpcds-parallel-sf10-retry.json`. The first failed report and local staging remain for audit.
- The locally built DuckDB 1.5.6 CLI with `iceberg` and `httpfs` extensions attached to the CROWDB REST Catalog and selected `AMERICA` from the remote TPC-H `region` table. It also fetched a row from the 2.2 GiB SF 10 `lineitem` Parquet file and selected `i_item_sk=1, i_item_id=AAAAAAAABAAAAAAA` from the new TPC-DS SF 10 `item` table. This proves an independent SQL reader can use the catalog and catalog-vended object credentials. It does not establish standard TPC query correctness or performance.
- A representative live KV metrics window reported 244 GET RPCs averaging 98.9 microseconds, p99 154 microseconds, while PUT RPCs averaged 3.95 ms and WAL fsync 3.27 ms. GET count alone does not explain multi-second operations. The 20.7 MB/s upload ceiling needs a separate write-path profile; this aggregate window does not isolate a specific upload.

## Earlier package-only run

The original delivery ran 176 passing tests with 8 skips, but had no PyArrow, DuckDB, PyIceberg, generator, or CROWDB container available. Its raw output remains under [`test-results/`](test-results/). Those records describe the earlier environment only, not the later local container run.

## Remaining acceptance

Profile the remaining upload throughput gap, then test distributed CROWDB and standard TPC-H/TPC-DS query suites. The successful loads and spot DuckDB queries do not establish benchmark-query compliance or performance.
