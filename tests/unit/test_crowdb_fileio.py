"""The native FileIO must inspect exact objects without S3 bucket listing."""

from contextlib import nullcontext

from crowdb_tpc_loader.crowdb_fileio import CrowdbFile


class ExactObjectFilesystem:
    def __init__(self):
        self.opened = []

    def open_input_file(self, path):
        self.opened.append(path)
        if path == "missing":
            raise FileNotFoundError(path)
        return nullcontext(type("Stream", (), {"size": lambda self: 23})())

    def get_file_info(self, path):
        raise AssertionError("S3 listing must not be used for exact-object metadata")


def test_exists_and_length_use_exact_object_open():
    filesystem = ExactObjectFilesystem()
    found = CrowdbFile(fs=filesystem, location="s3://scope/found", path="found", buffer_size=1024)
    missing = CrowdbFile(fs=filesystem, location="s3://scope/missing", path="missing", buffer_size=1024)
    assert found.exists()
    assert len(found) == 23
    assert not missing.exists()
    assert filesystem.opened == ["found", "found", "missing"]
