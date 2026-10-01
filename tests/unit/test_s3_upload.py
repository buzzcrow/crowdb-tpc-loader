"""CrowDB data files use direct PUT until frame-aligned MPU is needed."""

from botocore.exceptions import ClientError
import pytest

from crowdb_tpc_loader import s3_upload


class FakeS3:
    def __init__(self, fail_part=False):
        self.fail_part = fail_part
        self.puts = []
        self.parts = []
        self.completed = None
        self.aborted = False

    def head_object(self, **_kwargs):
        raise ClientError({"Error": {"Code": "404"}, "ResponseMetadata": {"HTTPStatusCode": 404}}, "HeadObject")

    def put_object(self, **kwargs):
        self.puts.append((kwargs["ContentLength"], kwargs["IfNoneMatch"], kwargs["Body"].read(1)))

    def create_multipart_upload(self, **_kwargs):
        return {"UploadId": "upload-1"}

    def upload_part(self, **kwargs):
        if self.fail_part:
            raise OSError("injected part failure")
        reader = kwargs["Body"]
        assert reader.tell() == 0
        reader.seek(kwargs["ContentLength"] - 1)
        assert reader.read(1) == b"\0"
        self.parts.append(kwargs["ContentLength"])
        return {"ETag": f'"part-{len(self.parts)}"'}

    def complete_multipart_upload(self, **kwargs):
        self.completed = kwargs

    def abort_multipart_upload(self, **_kwargs):
        self.aborted = True


def test_direct_put_below_256_mib(monkeypatch, tmp_path):
    client = FakeS3()
    monkeypatch.setattr(s3_upload, "_client", lambda _properties: client)
    path = tmp_path / "object.parquet"
    with path.open("wb") as output:
        output.truncate(100 * s3_upload.MIB)
    progress = []
    s3_upload.upload_file({}, "s3://bucket/data/object.parquet", path, path.stat().st_size, progress.append)
    assert client.puts == [(100 * s3_upload.MIB, "*", b"\0")]
    assert client.parts == []
    assert progress == [100 * s3_upload.MIB]


def test_multipart_parts_align_and_meet_64_mib_minimum(monkeypatch, tmp_path):
    client = FakeS3()
    monkeypatch.setattr(s3_upload, "_client", lambda _properties: client)
    path = tmp_path / "large.parquet"
    with path.open("wb") as output:
        output.truncate(256 * s3_upload.MIB)
    progress = []
    s3_upload.upload_file({}, "s3://bucket/data/large.parquet", path, path.stat().st_size, progress.append)
    assert len(client.parts) == 4
    assert sum(client.parts) == path.stat().st_size
    assert all(size >= 64 * s3_upload.MIB for size in client.parts[:-1])
    assert all(size % s3_upload.FRAME_PAYLOAD_BYTES == 0 for size in client.parts[:-1])
    assert client.completed["IfNoneMatch"] == "*"
    assert len(client.completed["MultipartUpload"]["Parts"]) == 4
    assert progress[-1] == path.stat().st_size


def test_failed_part_aborts_multipart(monkeypatch, tmp_path):
    client = FakeS3(fail_part=True)
    monkeypatch.setattr(s3_upload, "_client", lambda _properties: client)
    path = tmp_path / "large.parquet"
    with path.open("wb") as output:
        output.truncate(256 * s3_upload.MIB)
    with pytest.raises(OSError, match="injected part failure"):
        s3_upload.upload_file({}, "s3://bucket/data/large.parquet", path, path.stat().st_size, lambda _: None)
    assert client.aborted
