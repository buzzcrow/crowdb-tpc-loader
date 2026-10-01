# Changelog

## 0.1.0

- Load TPC-H and TPC-DS datasets into Iceberg tables, with validation and recoverable reports.
- Write separate tables concurrently, with 8 workers by default and a configurable 1–24 range.
- Retry transient Catalog 503 responses during table creation without retrying ambiguous table commits.
- Verify the published CROWDB Iceberg image with a small TPC-H load and DuckDB Q1 read.
- Add CI checks and a manually triggered PyPI release workflow.
