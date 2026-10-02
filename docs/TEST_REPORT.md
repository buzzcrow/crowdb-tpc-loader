# Test record

## Loader 0.1.1, October 2, 2026

- Fresh local image `crowdb-loader-011:validation`, image ID `b1b931994744`, server revision `19ba6530`. Real single-node disk writes, one 1 MiB mirror, GC disabled; io_uring unavailable, so DiskIO used its blocking fallback.
- TPC-H and TPC-DS SF=1 ran concurrently in separate processes. Each loader used one S3 data-upload client and eight workers/connections. Both versions used the same new image and new namespaces. The baseline ran after the optimized run; these are single-run development measurements, not stable benchmark claims.
- Both loaders committed all 32 tables. Server metrics after loading and verification recorded **0 unavailable (503/504) responses and 0 server errors** across all Iceberg routes.
- Optimized results passed remote MD5 comparison, full-row Parquet decoding, Iceberg sample scans and DuckDB SQL row-count queries for all 32 tables. The baseline passed SHA256 comparison and sample/footer verification. Local files were removed only after successful load verification.
- `0.1.1` wall time includes dataset generation, validation, upload and table commit, excluding subsequent independent reads: TPC-H **10.120 s**, TPC-DS **38.776 s**. They overlapped; do not add these wall times.
- File upload time includes local checksum work and the complete upload call, excluding report checkpoints and table commit. MD5 and transfer columns are separate measured components of the new upload time. Concurrent file durations must not be added to obtain wall time.

### TPC-H SF=1

- 8 files, 340,201,375 bytes. Median file upload: 2.4005 s → 0.1900 s (12.63×).

| File/table | Bytes       | 0.1.0 upload (s) | MD5 (s)  | Transfer (s) | 0.1.1 upload (s) |
| ---------- | ----------- | ---------------- | -------- | ------------ | ---------------- |
| region     | 1,227       | 1.880            | 0.000749 | 0.075732     | 0.077            |
| nation     | 2,670       | 1.779            | 0.009823 | 0.131048     | 0.141            |
| supplier   | 901,201     | 1.881            | 0.013099 | 0.121957     | 0.135            |
| customer   | 13,562,085  | 2.428            | 0.054500 | 0.144023     | 0.199            |
| part       | 6,682,273   | 2.373            | 0.024229 | 0.156283     | 0.181            |
| partsupp   | 43,479,766  | 3.285            | 0.136988 | 0.561957     | 0.706            |
| orders     | 59,244,162  | 2.871            | 0.240169 | 0.653701     | 0.894            |
| lineitem   | 216,327,991 | 4.472            | 0.531123 | 1.267352     | 1.799            |

### TPC-DS SF=1

- 24 files, 277,305,727 bytes. Median file upload: 3.0100 s → 0.1430 s (21.05×).

| File/table             | Bytes       | 0.1.0 upload (s) | MD5 (s)  | Transfer (s) | 0.1.1 upload (s) |
| ---------------------- | ----------- | ---------------- | -------- | ------------ | ---------------- |
| call_center            | 6,275       | 2.463            | 0.001186 | 0.064085     | 0.065            |
| catalog_page           | 369,912     | 2.297            | 0.009744 | 0.085421     | 0.095            |
| catalog_returns        | 7,882,698   | 3.438            | 0.035030 | 0.132752     | 0.168            |
| catalog_sales          | 78,918,866  | 5.528            | 0.301264 | 0.650193     | 0.983            |
| customer               | 3,703,776   | 3.254            | 0.029279 | 0.117051     | 0.146            |
| customer_address       | 706,113     | 2.827            | 0.015630 | 0.125819     | 0.142            |
| customer_demographics  | 2,024,739   | 2.343            | 0.020922 | 0.122526     | 0.144            |
| date_dim               | 639,274     | 2.954            | 0.009873 | 0.134443     | 0.144            |
| household_demographics | 14,922      | 2.883            | 0.036637 | 0.057644     | 0.094            |
| income_band            | 764         | 3.066            | 0.023087 | 0.094967     | 0.118            |
| inventory              | 19,700,814  | 4.401            | 0.114090 | 0.368926     | 0.483            |
| item                   | 1,030,900   | 3.457            | 0.036089 | 0.077720     | 0.114            |
| promotion              | 13,560      | 2.721            | 0.008388 | 0.082514     | 0.091            |
| reason                 | 1,122       | 3.552            | 0.032385 | 0.090683     | 0.123            |
| ship_mode              | 1,740       | 3.228            | 0.009723 | 0.059822     | 0.070            |
| store                  | 6,783       | 2.823            | 0.010821 | 0.128460     | 0.139            |
| store_returns          | 12,122,814  | 2.234            | 0.092311 | 0.197127     | 0.290            |
| store_sales            | 107,147,541 | 7.072            | 0.325360 | 0.627556     | 0.953            |
| time_dim               | 316,030     | 2.624            | 0.079046 | 0.071982     | 0.151            |
| warehouse              | 2,979       | 4.027            | 0.031305 | 0.094877     | 0.126            |
| web_page               | 3,908       | 2.460            | 0.031746 | 0.094342     | 0.126            |
| web_returns            | 3,876,964   | 3.521            | 0.039901 | 0.222050     | 0.262            |
| web_sales              | 38,804,696  | 4.135            | 0.186994 | 0.384673     | 0.572            |
| web_site               | 8,537       | 2.679            | 0.024765 | 0.148047     | 0.173            |

### Remaining costs

- PyIceberg still reads remote Parquet footers to construct DataFile statistics, writes manifests and reads the committed file inventory. Normal commit verification reuses returned metadata; uncertain commits still reload the table.
- PyArrow metadata FileIO uses table-scoped credentials and its own transport. Sharing this with the data-upload pool would require a FileIO adapter and should be justified by measured remaining cost.
- Generation/export/validation dominate SF=1 wall time after these upload improvements. DuckDB export does not expose a streaming checksum hook; MD5 remains a concurrent read after file generation.
- Multipart parts within one file upload sequentially. Multiple files overlap; part-level parallelism is a separate option for larger workloads, subject to the same connection limit.
- Durable JSON report saves still fsync both report locations. Required states remain; intermediate saves were removed and ready worker records batch without an added delay.

## Published image, October 1, 2026

- Image: `crowdb/crowdb-iceberg:latest`, digest `sha256:e4e7f80367094a35ab54c2bf1b0a9780811f9a668bc6037cfe7ced1b23373718`, Linux amd64 single-node.
- Published loader 0.1.0: TPC-H SF 0.01 loaded 8 tables and 86,805 rows with 8 workers. All 22 DuckDB 1.5.6 TPC-H queries against the container matched DuckDB reading byte-identical local Parquet files. Q1 returned `(A,F,14876)`, `(N,F,348)`, `(N,O,29181)`, `(R,F,14902)` for `(returnflag, linestatus, count_order)`.
- TPC-DS SF 0.01 loaded 24 tables and 277,976 rows with 4 workers. All 99 DuckDB TPC-DS queries against the container matched DuckDB reading byte-identical local Parquet files.

These are development checks of the container's Iceberg read path and query results. They are not timed benchmark results, distributed behavior tests, or TPC certification.
