# Failure recovery: determine commit status before handling files

The tool does not provide automatic resume, overwrite, drop, purge, or remote garbage collection. There is no atomic transaction across all 8 or 24 tables. With concurrent table writes, each table uploads its files and commits one snapshot independently. An upload or commit failure in one table does not roll back successful snapshots in other tables.

## Reports and directories

Keep the JSON report and working subdirectory shown in the console. A persistent `--report-file` path is the most reliable choice. Concurrent workers can share one durable report save. Before uploading, the tool records the URI, run ID, and file status on disk. Before a commit, it marks the file `commit_unknown` and persists that state. If the process crashes between upload and commit, the unknown result will not be treated as definitely uncommitted.

Forced process termination, sudden power loss, and disk damage can still interrupt the latest checkpoint. Atomic replacement, fsync, and reports in two locations reduce this risk but do not provide a distributed transaction guarantee.

| Status | Meaning | Handling |
|---|---|---|
| `local` | No target remote URI yet | A locally generated file; this does not imply upload |
| `upload_started` | Target URI recorded; the object may be absent, partly uploaded, or complete | Check whether the object exists; do not infer its size from status alone |
| `uploaded_unregistered` | Upload accepted by the server; commit not attempted, or explicitly rejected and verified unregistered | Check other snapshots and references before cleanup |
| `commit_unknown` | Commit started or may have started; result cannot be confirmed | Keep the file; do not blindly repeat `add_files` |
| `registered` | URI observed in the verified current snapshot | Do not delete directly; follow the Iceberg snapshot and metadata lifecycle |

`summary.unregistered_uploads` lists **candidates for inspection**, not objects that can be deleted in bulk. Keep `summary.uncertain_uploads` as a priority. `summary.tables_requiring_inspection` includes tables that may have been created but not fully verified, or for which writes failed. `table_creation_state=unknown_or_unvalidated` does not mean the table is definitely absent.

## Common cases

### Generation or schema validation fails

This release creates benchmark tables only after validating the entire Parquet dataset. Check the generator version, actual schema, missing tables, and source-versus-export row counts in the report. Run again with a new output directory or namespace; do not overwrite the original files.

### FileIO preflight fails

A reachable REST endpoint does not prove that storage is writable. Check whether the returned storage address is reachable from the client, whether authentication needs a separate token, whether the correct FileIO was selected, and whether the HTTP service supports the required operations. Preflight uses an uncommitted staged table and does not fall back to a local path.

### Some tables succeed and a later table fails

First run the independent verifier on tables marked `succeeded` in the report. The failed table may exist but be empty, or it may have a committed or uncertain snapshot. A default rerun stops on existing tables as a protective measure.

`--on-exists skip` does not repair a failed table and may skip an empty one. Do not use it to claim that the full dataset has been completed. The easiest way to verify a fresh run is to use a new namespace, then handle old data according to Iceberg's rules. If the old namespace must be kept, inspect every snapshot, branch, and reference for the table before deciding how to handle empty tables or orphaned objects. The tool does not perform these destructive operations.

### Commit request times out

The tool makes up to three rounds of read-only verification without calling `add_files` again. It records success only if the current snapshot's file set, row counts, and sizes match exactly. Otherwise, it records `uncertain` and marks that table uncertain; other in-flight tables may still finish.

Another writer may subsequently change the final snapshot. Files recorded as `registered` may also be referenced by historical snapshots; absence from the current snapshot does not prove absence from history. Use the namespace, table UUID, run ID, and all file URIs to inspect server logs, snapshots, and branch references before proceeding.

### Insufficient local space or process interruption

Check the exit code, external report, and working path. The working directory is kept after a failure, and the tool does not delete remote benchmark objects. If local cleanup fails after a successful run, the tool records a cleanup warning without changing a verified successful remote commit into a failure. Before manually deleting a local directory, confirm that it is this run's working subdirectory, not the `--work-dir` parent.

## Information to include in a bug report

Keep the command with the token removed, Python/dependency/generator versions, run ID, error from the report, failed table, and FileIO type. Inspect the redacted report manually before sharing it. Do not include tokens, storage keys, signed URLs, complete environment variables, or private business data in an issue.
