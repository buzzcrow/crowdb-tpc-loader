"""A read-only Arrow filesystem bridge for independent PyIceberg HTTP scans.

Metadata/data writes still use HttpFileIO's conditional upload streams. Explicit
paths are supported; recursive object listing and filesystem mutations are not.
"""
from __future__ import annotations

from urllib.parse import urlsplit

from .errors import CompatibilityError


def make_filesystem(fileio, scheme: str, netloc: str):
    import pyarrow as pa
    from pyarrow.fs import FileInfo, FileSystemHandler, FileType, PyFileSystem
    from .http_fileio import origin

    expected_origin = origin(f"{scheme}://{netloc}")

    class Handler(FileSystemHandler):
        def _uri(self, path):
            if urlsplit(path).scheme in {"http", "https"}:
                uri = path
            else:
                path = path.lstrip("/")
                # PyIceberg's parse_location returns `netloc/path` for HTTP.
                uri = f"{scheme}://{path}" if path.startswith(netloc + "/") else f"{scheme}://{netloc}/{path}"
            if origin(uri) != expected_origin:
                raise CompatibilityError("Arrow HTTP filesystem path changed origin")
            return uri

        def get_type_name(self):
            return "crowdb-tpc-http"

        def normalize_path(self, path):
            self._uri(path)
            return path

        def get_file_info(self, paths):
            result = []
            for path in paths:
                try:
                    size = len(fileio.new_input(self._uri(path)))
                    result.append(FileInfo(path, FileType.File, size=size))
                except FileNotFoundError:
                    result.append(FileInfo(path, FileType.NotFound))
            return result

        def get_file_info_selector(self, selector):
            raise NotImplementedError("HTTP object listing is not supported; Iceberg supplies explicit manifest file paths")

        def open_input_file(self, path):
            return pa.PythonFile(fileio.new_input(self._uri(path)).open(), mode="r")

        def open_input_stream(self, path):
            return self.open_input_file(path)

        def create_dir(self, path, recursive=True):
            raise NotImplementedError("Use HttpFileIO output streams, not directory operations")

        def delete_dir(self, path):
            raise NotImplementedError("Recursive deletion is deliberately unavailable")

        def delete_dir_contents(self, path, missing_dir_ok=False):
            raise NotImplementedError("Recursive deletion is deliberately unavailable")

        def delete_root_dir_contents(self):
            raise NotImplementedError("Recursive deletion is deliberately unavailable")

        def delete_file(self, path):
            raise NotImplementedError("Use explicit HttpFileIO.delete for independently verified uncommitted objects")

        def move(self, src, dest):
            raise NotImplementedError("HTTP object move is unavailable")

        def copy_file(self, src, dest):
            raise NotImplementedError("Use bounded FileIO streams to copy objects")

        def open_output_stream(self, path, metadata=None):
            raise NotImplementedError("Use HttpFileIO.new_output().create() for conditional writes")

        def open_append_stream(self, path, metadata=None):
            raise NotImplementedError("Appending to immutable HTTP objects is unavailable")

    return PyFileSystem(Handler())
