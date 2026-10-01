# Compatibility

The package requires Python 3.10+, PyArrow `>=18,<24`, PyIceberg `>=0.10,<0.11`, and DuckDB `>=1.4,<1.6`. TPC-H uses `tpchgen-cli` 3.x; automatic download pins 3.0.0. These ranges are package constraints, not a claim that every version combination passed. The [test record](TEST_REPORT.md) names the observed local combination.

`load` requires an Iceberg REST Catalog and working remote FileIO. It reads table locations and credentials from the catalog; it never registers a local path. The default `crowdb_tpc_loader.crowdb_fileio.CrowdbFileIO` uses exact-object opens on CROWDB's native Iceberg endpoint, which does not require S3 prefix listing. Other deployments can provide a trusted FileIO class with `--py-io-impl`.

Before generation, the loader checks catalog access, existing tables, namespace creation, and a staged FileIO write probe where cleanup is supported. CROWDB's native FileIO has no remote file delete, so the first real table upload acts as its write test. The entire generated dataset passes schema and footer checks before any benchmark table is created.

The catalog can return transient HTTP 503 when its bounded operation capacity is busy. Table creation retries up to six attempts with short backoff. If a retry finds a table created by an earlier attempt in the same run, it verifies the run ID before continuing. Registration (`add_files`) is never blindly retried after an ambiguous response; the loader reads the current snapshot to reconcile the outcome. Keep the JSON report for [failure recovery](RECOVERY.md).

Each worker owns a separate Catalog client and one table. Eight workers run by default; `--upload-workers` accepts 1–24. Files within a table upload in sequence, then that table commits once. Other tables can upload or commit at the same time. This can expose server backpressure; choose fewer workers for a small deployment if bounded retries are still exhausted. A failure in one table does not roll back another table's successful snapshot.

TPC-DS generation uses DuckDB's `tpcds` extension and a disk database. `--memory-limit` controls DuckDB memory, not total process memory. TPC-H defaults to `ceil(SF / 10)` parts. Reports record the actual dependency and generator versions for each run. Distributed deployments and the full TPC query suites need separate acceptance.
