"""Shared transport preserves each request's credentials, body and checksum."""

import base64
from concurrent.futures import ThreadPoolExecutor
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import hashlib
from io import BytesIO
from threading import Thread


from crowdb_tpc_loader.upload_client import UploadClient


class CountingBody(BytesIO):
    def __init__(self, value):
        super().__init__(value)
        self.bytes_read = 0

    def read(self, size=-1):
        value = super().read(size)
        self.bytes_read += len(value)
        return value


def test_one_client_concurrent_request_credentials_without_payload_preread():
    received = []

    class Handler(BaseHTTPRequestHandler):
        def do_PUT(self):
            body = self.rfile.read(int(self.headers["Content-Length"]))
            received.append((self.path, dict(self.headers), body))
            digest = hashlib.md5(body).digest()
            assert self.headers["Content-MD5"] == base64.b64encode(digest).decode()
            self.send_response(200)
            self.send_header("Content-Length", "0")
            self.send_header("ETag", '"' + digest.hex() + '"')
            self.end_headers()

        def log_message(self, *_args):
            pass

    server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    thread = Thread(target=server.serve_forever)
    thread.start()
    transport = UploadClient(connections=4)
    try:

        def upload(index):
            properties = {
                "s3.endpoint": f"http://127.0.0.1:{server.server_port}/prefix",
                "s3.access-key-id": f"table-key-{index}",
                "s3.secret-access-key": f"table-secret-{index}",
                "s3.session-token": f"table-token-{index}",
                "client.region": f"region-{index}",
            }
            value = bytes([index]) * 128_000
            body = CountingBody(value)
            transport.bind(properties).put_object(
                Bucket="bucket",
                Key=f"file-{index}",
                Body=body,
                ContentLength=len(value),
                ContentMD5=base64.b64encode(hashlib.md5(value).digest()).decode(),
                IfNoneMatch="*",
            )
            assert body.bytes_read == len(value)

        with ThreadPoolExecutor(max_workers=4) as pool:
            list(pool.map(upload, range(12)))
        assert len(received) == 12
        for path, headers, body in received:
            index = int(path.rsplit("-", 1)[1])
            assert path == f"/prefix/bucket/file-{index}"
            assert f"Credential=table-key-{index}/" in headers["Authorization"]
            assert f"/region-{index}/s3/aws4_request" in headers["Authorization"]
            assert headers["X-Amz-Security-Token"] == f"table-token-{index}"
            assert headers["X-Amz-Content-SHA256"] == "UNSIGNED-PAYLOAD"
            assert body == bytes([index]) * 128_000
    finally:
        transport.close()
        server.shutdown()
        thread.join()
        server.server_close()
