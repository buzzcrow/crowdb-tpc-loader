"""Known-size CrowDB S3 uploads with bounded, frame-aligned multipart parts."""

from __future__ import annotations

import base64
import hashlib
from pathlib import Path
from typing import Callable
from urllib.parse import unquote, urlsplit

from botocore.exceptions import ClientError

from .util import hash_stream


MIB = 1024 * 1024
MULTIPART_THRESHOLD = 256 * MIB
FRAME_PAYLOAD_BYTES = 64 * 1024 - 14 - 20
PART_BYTES = ((64 * MIB + FRAME_PAYLOAD_BYTES - 1) // FRAME_PAYLOAD_BYTES) * FRAME_PAYLOAD_BYTES


class _PartReader:
    """Expose exactly one file range to botocore without a part-sized copy."""

    def __init__(self, source, length: int):
        self.source = source
        self.start = source.tell()
        self.length = length

    def read(self, size: int = -1) -> bytes:
        remaining = self.length - self.tell()
        return self.source.read(remaining if size is None or size < 0 else min(size, remaining))

    def seek(self, offset: int, whence: int = 0) -> int:
        if whence == 0:
            position = offset
        elif whence == 1:
            position = self.tell() + offset
        elif whence == 2:
            position = self.length + offset
        else:
            raise ValueError("invalid seek origin")
        if not 0 <= position <= self.length:
            raise ValueError("seek outside multipart part")
        self.source.seek(self.start + position)
        return position

    def tell(self) -> int:
        return self.source.tell() - self.start


def _bucket_key(uri: str) -> tuple[str, str]:
    parsed = urlsplit(uri)
    if parsed.scheme != "s3" or not parsed.netloc or not parsed.path.strip("/"):
        raise ValueError("CrowDB upload requires a complete s3://bucket/key location")
    return parsed.netloc, unquote(parsed.path.lstrip("/"))


def _client(properties: dict):
    from .upload_client import UploadClient

    transport = UploadClient(timeout=float(properties.get("http.timeout", 60)))
    try:
        return transport.bind(properties)
    except BaseException:
        transport.close()
        raise


def file_checksums(path: Path, size: int, buffer_size: int):
    """Compute file and multipart checksums in the same bounded read pass."""
    if buffer_size <= 0:
        raise ValueError("buffer_size must be positive")
    if size < MULTIPART_THRESHOLD:
        with path.open("rb") as source:
            count, digest = hash_stream(source, buffer_size)
        return count, digest, None
    whole = hashlib.md5()
    count, part_bytes = 0, 0
    part = hashlib.md5()
    digests = []
    with path.open("rb") as source:
        while chunk := source.read(min(buffer_size, PART_BYTES - part_bytes)):
            whole.update(chunk)
            part.update(chunk)
            count += len(chunk)
            part_bytes += len(chunk)
            if part_bytes == PART_BYTES:
                digests.append(part.hexdigest())
                part, part_bytes = hashlib.md5(), 0
    if part_bytes:
        digests.append(part.hexdigest())
    return count, whole.hexdigest(), digests


def upload_file(
    properties: dict,
    uri: str,
    path: Path,
    size: int,
    progress: Callable[[int], None],
    md5: str | None = None,
    client=None,
    part_digests: list[str] | None = None,
    buffer_size: int = 8 * MIB,
) -> None:
    """PUT files below 256 MiB; otherwise use 64-MiB-minimum logical parts."""
    bucket, key = _bucket_key(uri)
    owned_client = client is None
    client = client or _client(properties)
    try:
        _upload(client, bucket, key, path, size, progress, md5, part_digests, buffer_size)
    except ClientError as error:
        if error.response.get("ResponseMetadata", {}).get("HTTPStatusCode") == 412:
            raise FileExistsError("Unique target data URI unexpectedly already exists") from error
        raise
    finally:
        if owned_client:
            client.close()


def _content_md5(digest: str) -> str:
    return base64.b64encode(bytes.fromhex(digest)).decode("ascii")


def _upload(client, bucket, key, path, size, progress, md5, part_digests, buffer_size):
    if size < MULTIPART_THRESHOLD:
        with path.open("rb") as source:
            if md5 is None:
                count, md5 = hash_stream(source, buffer_size)
                if count != size:
                    raise OSError("Local file changed size before upload")
                source.seek(0)
            client.put_object(
                Bucket=bucket,
                Key=key,
                Body=source,
                ContentLength=size,
                ContentMD5=_content_md5(md5),
                IfNoneMatch="*",
            )
        progress(size)
        return

    upload_id = None
    try:
        created = client.create_multipart_upload(Bucket=bucket, Key=key)
        upload_id = created["UploadId"]
        parts = []
        with path.open("rb") as source:
            offset = 0
            while offset < size:
                length = min(PART_BYTES, size - offset)
                reader = _PartReader(source, length)
                if part_digests is None:
                    count, part_md5 = hash_stream(reader, buffer_size)
                    if count != length:
                        raise OSError("Local file changed size before multipart upload")
                    reader.seek(0)
                else:
                    part_md5 = part_digests[len(parts)]
                uploaded = client.upload_part(
                    Bucket=bucket,
                    Key=key,
                    UploadId=upload_id,
                    PartNumber=len(parts) + 1,
                    Body=reader,
                    ContentLength=length,
                    ContentMD5=_content_md5(part_md5),
                )
                parts.append({"ETag": uploaded["ETag"], "PartNumber": len(parts) + 1})
                offset += length
                source.seek(offset)
                progress(offset)
        client.complete_multipart_upload(
            Bucket=bucket,
            Key=key,
            UploadId=upload_id,
            MultipartUpload={"Parts": parts},
            IfNoneMatch="*",
        )
    except BaseException:
        if upload_id is not None:
            try:
                client.abort_multipart_upload(Bucket=bucket, Key=key, UploadId=upload_id)
            except Exception:
                pass
        raise
