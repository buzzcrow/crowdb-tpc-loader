"""Known-size CrowDB S3 uploads with bounded, frame-aligned multipart parts."""

from __future__ import annotations

from pathlib import Path
from typing import Callable
from urllib.parse import unquote, urlsplit

from botocore.config import Config
from botocore.exceptions import ClientError
from botocore.session import get_session


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
    required = ("s3.endpoint", "s3.access-key-id", "s3.secret-access-key")
    if any(not properties.get(key) for key in required):
        raise OSError("Catalog did not provide delegated S3 endpoint and credentials")
    timeout = float(properties.get("http.timeout", 60))
    return get_session().create_client(
        "s3",
        region_name=properties.get("client.region", properties.get("s3.region", "us-east-1")),
        endpoint_url=properties["s3.endpoint"],
        aws_access_key_id=properties["s3.access-key-id"],
        aws_secret_access_key=properties["s3.secret-access-key"],
        aws_session_token=properties.get("s3.session-token"),
        config=Config(
            signature_version="s3v4",
            s3={"addressing_style": "path"},
            request_checksum_calculation="when_required",
            response_checksum_validation="when_required",
            connect_timeout=timeout,
            read_timeout=timeout,
            retries={"mode": "standard", "max_attempts": 3},
        ),
    )


def upload_file(
    properties: dict,
    uri: str,
    path: Path,
    size: int,
    progress: Callable[[int], None],
) -> None:
    """PUT files below 256 MiB; otherwise use 64-MiB-minimum logical parts."""
    bucket, key = _bucket_key(uri)
    client = _client(properties)
    try:
        client.head_object(Bucket=bucket, Key=key)
    except ClientError as error:
        if error.response.get("ResponseMetadata", {}).get("HTTPStatusCode") != 404:
            raise OSError("CrowDB S3 existence check failed") from None
    else:
        raise FileExistsError("Unique target data URI unexpectedly already exists")

    if size < MULTIPART_THRESHOLD:
        with path.open("rb") as source:
            client.put_object(
                Bucket=bucket,
                Key=key,
                Body=source,
                ContentLength=size,
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
                uploaded = client.upload_part(
                    Bucket=bucket,
                    Key=key,
                    UploadId=upload_id,
                    PartNumber=len(parts) + 1,
                    Body=reader,
                    ContentLength=length,
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
