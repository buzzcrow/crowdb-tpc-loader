"""PyIceberg FileIO for CrowDB's object-level Iceberg endpoint.

PyArrow's S3 get_file_info may list a bucket. CrowDB's Iceberg endpoint
authorizes exact objects, so use its HEAD-backed open_input_file for existence
and length while retaining PyArrow's streaming reads and writes.
"""

from __future__ import annotations

from pyiceberg.io.pyarrow import BUFFER_SIZE, ONE_MEGABYTE, PyArrowFile, PyArrowFileIO


class CrowdbFile(PyArrowFile):
    def exists(self) -> bool:
        try:
            with self._filesystem.open_input_file(self._path):
                return True
        except FileNotFoundError:
            return False

    def __len__(self) -> int:
        with self._filesystem.open_input_file(self._path) as stream:
            return stream.size()


class CrowdbFileIO(PyArrowFileIO):
    def _file(self, location: str) -> CrowdbFile:
        scheme, netloc, path = self.parse_location(location, self.properties)
        return CrowdbFile(
            fs=self.fs_by_scheme(scheme, netloc),
            location=location,
            path=path,
            buffer_size=int(self.properties.get(BUFFER_SIZE, ONE_MEGABYTE)),
        )

    def new_input(self, location: str) -> CrowdbFile:
        return self._file(location)

    def new_output(self, location: str) -> CrowdbFile:
        return self._file(location)
