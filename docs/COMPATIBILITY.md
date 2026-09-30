# Compatibility boundaries and deployment configuration

## Version ranges and observed local run

| Component | Constraint or choice | Local verification on 2026-09-30 |
|---|---|---|
| Python | `>=3.10` | Ran on 3.12.3 |
| PyArrow | `>=18,<24` | 23.0.1, local TPC load/read passed |
| PyIceberg | `>=0.10,<0.11` | 0.10.0, local TPC load/read passed |
| DuckDB | `>=1.4,<1.6` | 1.5.6, SF 0.01 TPC-DS generated |
| tpchgen-cli | Built-in downloader pins `3.0.0`; explicitly supplied executables may be `3.x` | 3.0.0, SF 0.01 TPC-H generated |
| CROWDB | Requires REST Catalog and readable/writable remote FileIO | Local version 0.2.0 single-node image passed SF 0.01 load/read |

`requirements-integration.txt` provides pinned versions for reproduction; the versions actually used are in each run report. Reports record dependency versions for each run. An automatically downloaded tpchgen binary may not be an installed Python distribution, so `dependencies.tpchgen-cli` may say `not installed` while `generator.version` still records the actual binary version and source.

DuckDB is constrained to 1.4/1.5 to avoid mixing TPC-DS generator changes from 2.x into the same data baseline. The extension version and source and the DuckDB build are also recorded. Pinned versions do not replace data validation: any change to column types is an error; DECIMAL precision is not relaxed and date types are not changed automatically.

## Catalog and preflight

PyIceberg adaptation is concentrated in `backend.py` and `rest_catalog.py`. Two extension points require particular regression attention:

- `CreateTableTransaction._table`: obtains a valid location and FileIO for an uncommitted table. This object never enters the transaction commit context.
- `RestCatalog._create_session`: sets a bounded request timeout on the Catalog's own session, preserves authentication/signing adapters, and disables transport retries and HTTP redirects. It does not modify global requests or assume an unsupported `rest.timeout` option takes effect.

At runtime, load explicitly rejects PyIceberg versions outside 0.10.x. Custom FileIO paths still use staged preflight and require `stage-create`. The native CrowDB FileIO skips the write probe because the native endpoint has no file DELETE; the first real table performs upload verification.

REST preflight may create a missing namespace. It does not publish benchmark tables before generating data. Whether the server retains uncommitted metadata from `stage-create` depends on the server implementation; this tool does not guess or delete internal server directories.

The table property `commit.retry.num-retries=0` is set, and the application calls `add_files` only once. After an exception, it performs read-only verification without another append. Retries by the server or other clients are outside this tool's control.

## FileIO selection

The default loader FileIO is `crowdb_tpc_loader.crowdb_fileio.CrowdbFileIO`, which uses exact-object opens for existence and length and otherwise uses PyArrow streaming S3 reads and writes. It works with the local native CrowDB Iceberg endpoint without `ListObjectsV2`. Other deployments may select their own FileIO with `--py-io-impl`. Honor catalog-provided storage URI and vended credentials; do not treat paths inside a Docker container as client-writable paths.

Example of advanced parameters:

```bash
crowdb-tpc-loader load --benchmark tpch --namespace tpch_io_test \
  --catalog-property s3.endpoint=https://your-object-store.example \
  --catalog-property s3.region=your-region
```

Prefer authentication information supplied by the deployment or Catalog. If storage keys must be configured manually, use a secure method supported by the deployment. CLI arguments may still appear in process lists and shell history even when the tool redacts them.

`--py-io-impl` is the fully qualified name of an installed Python class; use only trusted implementations. A custom FileIO returned by REST should likewise come from a trusted deployment. The tool does not dynamically download Python FileIO code from an arbitrary URL.

## Optional HTTP FileIO

Class: `crowdb_tpc_loader.http_fileio.HttpFileIO`. It supports a standard HTTP object interface, not an unverified CrowDB-specific protocol:

| Operation | Required behavior |
|---|---|
| `HEAD <object>` | Return the correct nonnegative Content-Length for an existing object and 404 for an absent one |
| `GET <object>` + Range | Return 206 and an exact Content-Range; large objects cannot fall back to a full response that ignores Range |
| `PUT <object>` | Accept a binary body and known Content-Length; `If-None-Match: *` must protect existing objects |
| `DELETE <object>` | Delete the explicitly specified uncommitted probe; directory listing and bulk deletion are not required |

Writes spool to a local disk TemporaryFile before upload, avoiding a whole-object Python memory buffer. Reads support seeking by range with 1 MiB cache blocks. Storage must allow the required operations on server-generated metadata, manifest, and data URIs.

HTTP FileIO extends PyArrowFileIO and includes a read-only Arrow filesystem bridge so PyIceberg can perform an actual Iceberg scan in an independent process. The bridge does not implement object-directory listing, recursive deletion, append, or move. Its real PyArrow/PyIceberg integration tests were not run in the current environment; the standard HTTP request layer was tested against a real local HTTP service.

Supported properties:

```text
http.timeout         Network request timeout in seconds; load defaults to --timeout
http.token           Separate storage token; defaults to the Catalog token when omitted
http.auth-origins    Comma-separated origins allowed to receive the token, for example https://storage.example:443
http.spool-directory Temporary disk directory for HTTP writes; load defaults to this run's .scratch
```

By default, the token is sent only to object addresses with the same origin (scheme, host, and port) as the Catalog URI. HTTP redirects are not followed. HTTPS uses system certificate validation; there is no option to ignore TLS verification. Other origins must be explicitly allowed, rather than automatically authorizing every storage host.

```bash
crowdb-tpc-loader load --benchmark tpch --namespace tpch_http_test \
  --py-io-impl crowdb_tpc_loader.http_fileio.HttpFileIO \
  --catalog-property http.auth-origins=https://storage.example:443
```

This optional HTTP adapter is separate from the native CrowDB adapter. The local CrowDB Iceberg endpoint uses S3-shaped object requests and does not support file DELETE or general bucket listing. Do not use the HTTP adapter for that endpoint.

## Native generators and resources

TPC-H defaults to `ceil(SF / 10)` parts and a 16 MiB Parquet row-group target. The thread environment variable is `RAYON_NUM_THREADS`; the CLI thread option is also passed explicitly when supported. Python collects only the last 64 KiB of subprocess output in a bounded buffer and does not load an entire table into memory. The native generator may have additional memory overhead.

TPC-DS uses a disk database and spill and runs COPY per table. Row-count queries return only a single aggregate result. `memory_limit` configures DuckDB; it is not an operating-system memory limit. This release does not promise that arbitrary SF values will succeed with a fixed amount of memory.

TPC-H cardinality baselines follow standard table cardinalities; SF1 lineitem has 6,001,215 rows. Rounding at fractional SF values and all 3.x variants still require verification with the actual generator. If a generator rejects a very small or large positive SF, the tool reports a failure without changing the SF, adding rows, or removing validation.

## Upstream references

The local single-node runs and independent verifier are recorded in [TEST_REPORT.md](TEST_REPORT.md). They do not establish distributed-deployment behavior.

- [PyIceberg Add Files](https://py.iceberg.apache.org/api/#adding-files)
- [PyIceberg FileIO interface](https://py.iceberg.apache.org/reference/pyiceberg/io/)
- [PyIceberg PyArrow FileIO](https://py.iceberg.apache.org/reference/pyiceberg/io/pyarrow/)
- [PyIceberg configuration](https://py.iceberg.apache.org/configuration/)
- [DuckDB TPC-DS extension](https://duckdb.org/docs/stable/core_extensions/tpcds)
- [tpchgen-cli PyPI project](https://pypi.org/project/tpchgen-cli/)

These upstream documents change over time and are not API snapshots for 0.10.x. Re-run acceptance checks before upgrading dependencies.
