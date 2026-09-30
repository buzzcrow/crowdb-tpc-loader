# Implementation notes against the original design

The original `design/design.md` is preserved as supplied. The table distinguishes an implemented path from completed acceptance against real systems.

| Design item | Implementation and behavior | Verification boundary |
|---|---|---|
| 8 TPC-H tables | `schemas.TPCH` + `generators/tpch.py`; native 3.x parquet subcommand and shard detection | Lists, arguments, and type decisions tested; SF 0.01 generator and load passed locally |
| 24 TPC-DS tables | `schemas.TPCDS` + `generators/tpcds.py`; disk database, extension, dsdgen, and per-table COPY | Lists and control paths tested; SF 0.01 generator and load passed locally |
| pip installation and entry point | `pyproject.toml` / `project.scripts`; wheel and sdist | Build and dependency-free entry-point installation checked in this delivery; see TEST_REPORT |
| No Rust build for tpchgen | `generators/binary.py`; compatible wheel, hash check, binary extraction only, no sdist fallback | Offline selection, cache, and verification tests; 3.0.0 binary obtained from a verified wheel and run locally |
| Argument, URI, and token precedence | `cli.py`; CLI overrides environment, redaction, argument validation | Unit tests |
| Complete Parquet/schema validation | `validation.py`; complete table list, every footer, types, and consistency across shards | Decision logic tested; real PyArrow tests and remote SF 0.01 passed |
| Upload to persistent FileIO | `transfers.py` + `backend.data_location`; rejects local/file/memory and credential-bearing URIs | Real byte-stream and HTTP tests; native CrowDB SF 0.01 upload passed |
| Upload integrity | Local size and SHA-256 are recorded; CROWDB validates the supplied signed payload/checksum before accepting upload; no full-object loader readback | Byte-stream and real CROWDB tests |
| `add_files` and snapshot verification | `loader.py` + `backend.py`; no `add_files` retry; verifies live manifest file set, row counts, and sizes | Fault injection tests; native CrowDB SF 0.01 and independent reads passed |
| Empty Parquet files | Validation requires them; manifest enumeration includes zero-row files without relying on data scan filtering | Decision tests; real integration test pending |
| Existing-table `error`/`skip` | `list_tables` first; `error` stops early, all-skipped runs return early; handles concurrent table creation | Unit tests |
| Partial failure | Sequential mode stops per table; concurrent mode completes the upload batch before ordered commits and skips all commits if any upload fails | Unit tests |
| Uncertain commit result | Three read-only verification rounds; separates `registered`, `uploaded_unregistered`, and `commit_unknown` | Fault injection for post-success timeout, missing commit, lost connection, and explicit rejection |
| Working directory and report | Unique subdirectory, ownership marker, lock, atomic JSON, cleanup on success and retention on failure | Unit tests |
| Resource limits | Heuristic local-space check, native generation, disk spill, bounded copy buffers and up to 24 concurrent uploads | Stream-size, directory, and space tests; peak memory at large SF not measured |
| Independent client read | `scripts/verify_crowdb.py`; reconnects, checks remote footers, batch reads, and PyIceberg scan | all 32 local single-node tables independently verified |
| PyPI publication | Metadata and build artifacts prepared | Not published; package-name availability not checked |

## Deliberate implementation choices

**Working directory behavior.** `--work-dir` is a parent directory in which a unique subdirectory is created. Cleanup after success therefore does not remove existing user files. The generate command's output directory belongs to the user and is not deleted.

**Preflight.** The native CrowDB FileIO cannot delete a probe object, so the loader verifies the first real upload instead of leaving an orphan. Custom FileIO paths retain the staged-table probe and require `stage-create`. No path inside a container is guessed.

**Validation scope.** The first release accepts generator files only when column names and types match the manifest and Parquet field IDs are absent. Integers may be 32 or 64 bits, and their actual width is retained. TPC-DS ticket/order numbers requiring 64 bits are not narrowed. DECIMAL values must match exactly and are not converted to floating point. The standard spelling `store.s_tax_percentage` is retained.

**Upload integrity.** CROWDB requires a signed payload digest, declared checksum, or Content-MD5 for each PUT or multipart part, and rejects a mismatch before accepting it. The loader trusts an accepted upload and does not read the object back. Its local SHA-256 in the report identifies the staged source file; it is not a claim that the server compared this exact digest. PyIceberg reads remote footers when registering files.

**Unregistered objects.** The tool does not automatically clean up uploaded benchmark objects or empty tables left after failure. Historical snapshots, branches, or delayed commits may still reference a file absent from the current snapshot. Reports provide evidence for manual inspection, not authorization for automatic deletion.

**Run reports.** Tokens, keys, and signed URLs are not written to JSON; userinfo, query, and fragment are removed from URLs. Reports record the run ID, actual versions, native arguments, schemas, row counts, byte counts, remote URIs, checksums, table UUIDs, snapshot IDs, and commit states.

**FileIO adapters.** `crowdb_fileio.py` is the default native CrowDB adapter. It uses exact-object opens for existence and length, bypassing PyArrow's S3 file-info listing. The separate optional HTTP adapter remains available only when explicitly selected and preflight succeeds; it supports bounded range reads, disk-spooled writes, and a read-only Arrow scan bridge.

## Unsupported capabilities

Outside the design scope: benchmark queries, official TPC certification, container images, Web UI, TPC-C/E. Also not promised: resume, overwrite of existing tables, automatic remote cleanup, transactions across tables, or compatibility with every SF, platform, and dependency version. Any claim of success or compatibility must be backed by real acceptance results.
