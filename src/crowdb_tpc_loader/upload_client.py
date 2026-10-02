"""One S3 transport with request-local catalog credentials and endpoints."""

from __future__ import annotations

from functools import partial
from threading import local
from urllib.parse import urlsplit, urlunsplit

from botocore.config import Config
from botocore.credentials import Credentials
from botocore.session import get_session


class UploadClient:
    """Construct before workers start; never mutate the shared client's signer."""

    def __init__(self, timeout: float = 60, connections: int = 8):
        self.context = local()
        self.client = get_session().create_client(
            "s3",
            region_name="us-east-1",
            endpoint_url="https://upload.invalid",
            aws_access_key_id="request-local",
            aws_secret_access_key="request-local",
            config=Config(
                signature_version="s3v4",
                s3={"addressing_style": "path", "payload_signing_enabled": False},
                max_pool_connections=connections,
                request_checksum_calculation="when_required",
                response_checksum_validation="when_required",
                connect_timeout=timeout,
                read_timeout=timeout,
                retries={"mode": "standard", "max_attempts": 3},
            ),
        )
        self.client.meta.events.register("before-call.s3", self._endpoint)
        self.client.meta.events.register("before-sign.s3", self._sign)

    def _endpoint(self, params, **_kwargs):
        target = urlsplit(self.context.properties["s3.endpoint"])
        current = urlsplit(params["url"])
        params["url"] = urlunsplit(
            (target.scheme, target.netloc, target.path.rstrip("/") + current.path, current.query, "")
        )

    def _sign(self, request, **_kwargs):
        properties = self.context.properties
        target = urlsplit(properties["s3.endpoint"])
        current = urlsplit(request.url)
        if (current.scheme, current.netloc) != (target.scheme, target.netloc):
            raise OSError("Refusing to redirect delegated S3 credentials to another endpoint")
        signing = request.context.setdefault("signing", {})
        signing["request_credentials"] = Credentials(
            properties["s3.access-key-id"],
            properties["s3.secret-access-key"],
            properties.get("s3.session-token"),
        )
        signing["region"] = properties.get("client.region", properties.get("s3.region", "us-east-1"))

    def _call(self, properties, method, **kwargs):
        self.context.properties = properties
        try:
            return getattr(self.client, method)(**kwargs)
        finally:
            del self.context.properties

    def bind(self, properties):
        required = ("s3.endpoint", "s3.access-key-id", "s3.secret-access-key")
        if any(not properties.get(key) for key in required):
            raise OSError("Catalog did not provide delegated S3 endpoint and credentials")
        target = urlsplit(properties["s3.endpoint"])
        if target.scheme not in {"http", "https"} or not target.netloc or target.query or target.fragment:
            raise OSError("Catalog provided an invalid S3 endpoint")
        return _BoundClient(self, properties)

    def close(self):
        self.client.close()


class _BoundClient:
    def __init__(self, transport, properties):
        self.transport, self.properties = transport, properties

    def __getattr__(self, method):
        return partial(self.transport._call, self.properties, method)
