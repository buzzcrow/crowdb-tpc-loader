# Compatibility boundaries and deployment configuration

## Version ranges: targets, not a completed test matrix

| Component | Constraint or choice for this candidate release | Verification in the current delivery environment |
|---|---|---|
| Python | `>=3.10` | Ran on 3.13.5; CI is configured for 3.10, 3.12, and 3.13, but was not run locally |
| PyArrow | `>=18,<24` | Not installed; real Parquet integration tests skipped |
| PyIceberg | `>=0.10,<0.11` | Not installed; real Catalog/metadata adaptation unverified |
| DuckDB | `>=1.4,<1.6` | Not installed; real TPC-DS data not generated |
| tpchgen-cli | Built-in downloader pins `3.0.0`; explicitly supplied executables may be `3.x` | Actual binary not obtained; command construction, version rejection, download verification, and cache logic have unit tests |
| CrowDB | Requires REST Catalog, `stage-create`, and readable/writable remote FileIO | No connection to a real deployment; compatibility with the current CrowDB version cannot be claimed |

`requirements-integration.txt` provides **pinned versions awaiting validation** for local reproduction, not "tested requirements." Reports record dependency versions for each run. An automatically downloaded tpchgen binary may not be an installed Python distribution, so `dependencies.tpchgen-cli` may say `not installed` while `generator.version` still records the actual binary version and source.

DuckDB is constrained to 1.4/1.5 to avoid mixing TPC-DS generator changes from 2.x into the same data baseline. The extension version and source and the DuckDB build are also recorded. Pinned versions do not replace data validation: any change to column types is an error; DECIMAL precision is not relaxed and date types are not changed automatically.

## Catalog and staged preflight

PyIceberg adaptation is concentrated in `backend.py` and `rest_catalog.py`. Two extension points require particular regression attention:

- `CreateTableTransaction._table`: obtains a valid location and FileIO for an uncommitted table. This object never enters the transaction commit context.
- `RestCatalog._create_session`: sets a bounded request timeout on the Catalog's own session, preserves authentication/signing adapters, and disables transport retries and HTTP redirects. It does not modify global requests or assume an unsupported `rest.timeout` option takes effect.

At runtime, load explicitly rejects PyIceberg versions outside 0.10.x. It also fails clearly if the class/method disappears or the server does not support `stage-create`. Do not remove the version check to claim compatibility; first run real integration tests and inspect the upstream API.

REST preflight may create a missing namespace. It does not publish benchmark tables before generating data. Whether the server retains uncommitted metadata from `stage-create` depends on the server implementation; this tool does not guess or delete internal server directories.

The table property `commit.retry.num-retries=0` is set, and the application calls `add_files` only once. After an exception, it performs read-only verification without another append. Retries by the server or other clients are outside this tool's control.

## FileIO selection

Prefer the storage URI, vended credentials, table properties, and FileIO returned by the Catalog. Do not treat paths inside a Docker container as client-writable paths.

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

Using local port 80 for both Catalog and FileIO does not prove that this HTTP protocol is supported. Existing CrowDB data URIs may use another scheme or a dedicated FileIO. If preflight fails, keep the complete redacted report and correct the deployment endpoint or FileIO configuration; do not bypass preflight by registering local files.

## Native generators and resources

TPC-H defaults to `ceil(SF / 10)` parts and a 16 MiB Parquet row-group target. The thread environment variable is `RAYON_NUM_THREADS`; the CLI thread option is also passed explicitly when supported. Python collects only the last 64 KiB of subprocess output in a bounded buffer and does not load an entire table into memory. The native generator may have additional memory overhead.

TPC-DS uses a disk database and spill and runs COPY per table. Row-count queries return only a single aggregate result. `memory_limit` configures DuckDB; it is not an operating-system memory limit. This release does not promise that arbitrary SF values will succeed with a fixed amount of memory.

TPC-H cardinality baselines follow standard table cardinalities; SF1 lineitem has 6,001,215 rows. Rounding at fractional SF values and all 3.x variants still require verification with the actual generator. If a generator rejects a very small or large positive SF, the tool reports a failure without changing the SF, adding rows, or removing validation.

## Upstream references

The CrowDB guide URL cited in the design remains in the original design document. Its contents could not be read for this delivery, so it cannot be treated as a verified server API contract.

- [PyIceberg Add Files](https://py.iceberg.apache.org/api/#adding-files)
- [PyIceberg FileIO interface](https://py.iceberg.apache.org/reference/pyiceberg/io/)
- [PyIceberg PyArrow FileIO](https://py.iceberg.apache.org/reference/pyiceberg/io/pyarrow/)
- [PyIceberg configuration](https://py.iceberg.apache.org/configuration/)
- [DuckDB TPC-DS extension](https://duckdb.org/docs/stable/core_extensions/tpcds)
- [tpchgen-cli PyPI project](https://pypi.org/project/tpchgen-cli/)

These upstream documents change over time and are not API snapshots for 0.10.x. Re-run acceptance checks before upgrading dependencies.
