"""PyIceberg adapter. All data paths and FileIO properties come from the catalog.

The only protected API access is the staged table of CreateTableTransaction.
It is isolated below and guarded so incompatible clients fail before data generation.
"""
from __future__ import annotations

import importlib.metadata
from dataclasses import dataclass
from typing import Any

from .errors import CompatibilityError, ExistingTablesError, LoadError
from .models import Options, TableData
from .security import Redactor
from .util import remote_uri

CROWDB_FILE_IO = "crowdb_tpc_loader.crowdb_fileio.CrowdbFileIO"


@dataclass(frozen=True)
class RemotePart:
    uri: str
    rows: int
    size_bytes: int


@dataclass(frozen=True)
class Inventory:
    snapshot_id: int | None
    files: dict[str, RemotePart]
    table_uuid: str


def data_location(table: Any, filename: str) -> str:
    """Honor the catalog's table location AND Iceberg location provider properties."""
    provider_getter = getattr(table, "location_provider", None)
    if callable(provider_getter):
        provider = provider_getter()
    else:
        from pyiceberg.table.locations import load_location_provider
        provider = load_location_provider(table.location(), table.metadata.properties)
    try:
        return remote_uri(provider.new_data_location(filename))
    except CompatibilityError:
        raise
    except Exception as exc:
        raise CompatibilityError(f"Cannot obtain a durable data location from the catalog's table metadata: {exc}") from exc


def table_uuid(table: Any) -> str:
    return str(table.metadata.table_uuid)


def inspect_inventory(table: Any) -> Inventory:
    """Read live manifest entries, not a data scan: empty Parquet files must count too."""
    snapshot = table.current_snapshot()
    files: dict[str, RemotePart] = {}
    if snapshot is not None:
        for manifest in snapshot.manifests(table.io):
            for entry in manifest.fetch_manifest_entry(table.io, discard_deleted=True):
                item = entry.data_file
                if int(item.content) != 0:
                    raise LoadError("Unexpected delete file in a newly created unpartitioned benchmark table")
                uri = str(item.file_path)
                if uri in files:
                    raise LoadError("Duplicate data URI found in the current snapshot's live manifests")
                files[uri] = RemotePart(uri, int(item.record_count), int(item.file_size_in_bytes))
    return Inventory(snapshot.snapshot_id if snapshot else None, files, table_uuid(table))


def schema_compatible(table: Any, data: TableData) -> None:
    """Validate the returned Iceberg schema before any benchmark upload."""
    from pyiceberg.io.pyarrow import schema_to_pyarrow
    import pyarrow as pa

    actual = schema_to_pyarrow(table.schema(), include_field_ids=False)
    if actual.names != data.schema.names:
        raise CompatibilityError(f"{data.name}: catalog returned different column names/order")
    for left, right in zip(actual, data.schema):
        both_strings = ((pa.types.is_string(left.type) or pa.types.is_large_string(left.type))
                        and (pa.types.is_string(right.type) or pa.types.is_large_string(right.type)))
        if left.type != right.type and not both_strings:
            raise CompatibilityError(f"{data.name}.{left.name}: Iceberg type {left.type} differs from Parquet type {right.type}")
        if not left.nullable and right.nullable:
            raise CompatibilityError(f"{data.name}.{left.name}: catalog strengthened a nullable column to required")
    if table.spec().fields:
        raise CompatibilityError("First-release benchmark tables must be unpartitioned")


class IcebergBackend:
    def __init__(self, options: Options, redactor: Redactor):
        self.options, self.redactor = options, redactor
        self.catalog: Any = None
        self.probe_cleanup_supported = True

    def connect(self) -> None:
        try:
            from packaging.version import Version
            from .rest_catalog import create_catalog
            version = importlib.metadata.version("pyiceberg")
            if not Version("0.10") <= Version(version) < Version("0.11"):
                raise CompatibilityError(f"PyIceberg {version} is outside this release's 0.10.x adapter range")
            properties = {"py-io-impl": CROWDB_FILE_IO}
            properties.update(self.options.catalog_properties)
            properties.update({"uri": self.options.catalog_uri, "http.timeout": str(self.options.timeout)})
            if self.options.token is not None:
                properties["token"] = self.options.token
            self.redactor.learn(properties)
            self.catalog = create_catalog("crowdb_tpc_loader", self.options.timeout, properties)
            self.probe_cleanup_supported = self.catalog.properties.get("py-io-impl") != CROWDB_FILE_IO
            self.redactor.learn(self.catalog.properties)
        except CompatibilityError:
            raise
        except ImportError as exc:
            raise CompatibilityError("PyIceberg and its FileIO dependencies are required for load") from exc
        except Exception as exc:
            raise LoadError(f"REST Catalog connection failed: {exc}. Check --catalog-uri/ICEBERG_URI and credentials.") from exc

    def existing(self, names: tuple[str, ...]) -> set[str]:
        from pyiceberg.exceptions import NoSuchNamespaceError
        try:
            identifiers = self.catalog.list_tables(self.options.namespace)
        except NoSuchNamespaceError:
            return set()
        except Exception as exc:
            raise LoadError(f"Cannot list target tables (credentials/permissions/network): {exc}") from exc
        return {identifier[-1] for identifier in identifiers if identifier[-1] in names}

    def ensure_namespace(self) -> None:
        from pyiceberg.exceptions import NamespaceAlreadyExistsError
        for length in range(1, len(self.options.namespace) + 1):
            try:
                self.catalog.create_namespace(self.options.namespace[:length])
            except NamespaceAlreadyExistsError:
                continue
            except Exception as exc:
                raise LoadError(f"Cannot create namespace {'.'.join(self.options.namespace[:length])}: {exc}") from exc

    def set_staging(self, scratch) -> None:
        self.catalog.properties.setdefault("http.spool-directory", str(scratch))

    def stage_probe(self, run_id: str):
        """Ask the REST server for an UNCOMMITTED location; never publish a probe table."""
        import pyarrow as pa
        try:
            transaction = self.catalog.create_table_transaction(
                (*self.options.namespace, f"__crowdb_tpc_probe_{run_id}"),
                schema=pa.schema([pa.field("probe", pa.int64(), nullable=True)]),
                properties={"format-version": "2"},
            )
            table = getattr(transaction, "_table", None)
            if table is None:
                raise CompatibilityError("PyIceberg staged table API is unavailable")
            self._learn(table)
            # Deliberately no `with transaction`, commit_transaction or add_files here.
            return table
        except CompatibilityError:
            raise
        except Exception as exc:
            raise CompatibilityError(
                f"Cannot obtain an uncommitted FileIO probe location using REST stage-create: {exc}. "
                "This release requires staged table creation for side-effect-safe preflight. "
                "Check CrowDB/PyIceberg compatibility and the configured py-io-impl; generate remains usable. "
                "No benchmark table has been created."
            ) from exc

    def _learn(self, table: Any) -> None:
        self.redactor.learn(getattr(table.io, "properties", {}))
        self.redactor.learn(getattr(table, "config", {}))
        self.redactor.learn(table.metadata.properties)
        for method in ("new_input", "new_output", "delete"):
            if not callable(getattr(table.io, method, None)):
                raise CompatibilityError(f"Catalog FileIO does not implement {method}")

    def create(self, data: TableData, run_id: str, generator: dict[str, Any]):
        from pyiceberg.exceptions import TableAlreadyExistsError
        properties = {
            "format-version": "2", "commit.retry.num-retries": "0", "crowdb-tpc-loader.run-id": run_id,
            "crowdb-tpc-loader.benchmark": self.options.benchmark,
            "crowdb-tpc-loader.scale-factor": str(self.options.sf),
            "crowdb-tpc-loader.generator": str(generator["implementation"]),
            "crowdb-tpc-loader.generator-version": str(generator["version"]),
        }
        try:
            table = self.catalog.create_table((*self.options.namespace, data.name), schema=data.schema,
                                              properties=properties)
            self._learn(table)
            schema_compatible(table, data)
            return table
        except TableAlreadyExistsError as exc:
            if self.options.on_exists == "skip":
                return None
            raise ExistingTablesError(f"Table {data.name} appeared during this run; refusing to modify it") from exc
        except (CompatibilityError, LoadError):
            raise
        except Exception as exc:
            raise LoadError(f"Could not create/validate table {data.name}: {exc}") from exc

    def validate_import(self, table: Any, uris: list[str]) -> None:
        """Run the exact PyIceberg Parquet-to-DataFile conversion without committing."""
        try:
            from pyiceberg.io.pyarrow import parquet_files_to_data_files
            for _ in parquet_files_to_data_files(table.io, table.metadata, iter(uris)):
                pass
        except Exception as exc:
            raise CompatibilityError(f"This FileIO/PyIceberg combination cannot inspect/register remote Parquet: {exc}") from exc

    def reload_inventory(self, table: Any) -> tuple[Any, Inventory]:
        refreshed = self.catalog.load_table(table.name())
        self._learn(refreshed)
        if table_uuid(refreshed) != table_uuid(table):
            raise LoadError("Table identity changed concurrently; refusing to treat a replacement table as this run's table")
        return refreshed, inspect_inventory(refreshed)

    def assert_empty(self, table: Any) -> Any:
        refreshed, state = self.reload_inventory(table)
        if state.snapshot_id is not None or state.files:
            raise LoadError("New benchmark table was modified by another writer; refusing to append")
        return refreshed

    def register(self, table: Any, uris: list[str], run_id: str) -> None:
        for uri in uris:
            remote_uri(uri)
        table.add_files(file_paths=uris, check_duplicate_files=True,
                        snapshot_properties={"crowdb-tpc-loader.run-id": run_id})

    @staticmethod
    def definite_rejection(error: Exception) -> bool:
        # A transport failure / generic server exception is never assumed to be a rejected commit.
        from pyiceberg.exceptions import (BadRequestError, CommitFailedException,
                                          ForbiddenError, UnauthorizedError)
        return isinstance(error, (BadRequestError, CommitFailedException, ForbiddenError, UnauthorizedError))
