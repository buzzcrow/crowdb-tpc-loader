"""Central redaction for console output, library logs and persisted run reports."""

from __future__ import annotations

import logging
import re
from collections.abc import Mapping
from typing import Any
from urllib.parse import quote, urlsplit, urlunsplit

_URL = re.compile(r"[A-Za-z][A-Za-z0-9+.-]*://[^\s<>\"']+")
_SECRET_KEY = re.compile(
    r"token|secret|password|credential|authorization|access[._-]?key|api[._-]?key|private[._-]?key|cookie|signature",
    re.I,
)
_ASSIGN = re.compile(
    r"(?i)((?:[\w.-]*(?:token|secret|password|credential|authorization|access[._-]?key|api[._-]?key|private[._-]?key|cookie|signature)[\w.-]*)"
    r"[\"']?\s*[:=]\s*[\"']?)(?:Bearer\s+)?[^\s,;\"'}]+"
)
_BEARER = re.compile(r"(?i)\bBearer\s+[^\s,;\"'}]+")


def safe_uri(value: str) -> str:
    """Strip userinfo, query parameters and fragments, retaining an actionable location."""
    try:
        p = urlsplit(value)
        host = p.netloc.rsplit("@", 1)[-1]
        return urlunsplit((p.scheme, host, p.path, "", ""))
    except ValueError:
        return "[invalid/redacted URI]"


class Redactor:
    def __init__(self, secrets: list[str] | tuple[str, ...] = ()):
        self.secrets: set[str] = set()
        for value in secrets:
            self.add(value)

    def add(self, value: Any) -> None:
        if isinstance(value, str) and value:
            self.secrets.update((value, quote(value, safe="")))

    def learn(self, properties: Mapping[str, Any]) -> None:
        for key, value in properties.items():
            if isinstance(value, Mapping):
                self.learn(value)
            elif _SECRET_KEY.search(str(key)):
                self.add(value)

    def text(self, value: Any) -> str:
        text = str(value)
        # Exact secret replacement also protects opaque exceptions without a key name.
        for secret in sorted(self.secrets, key=len, reverse=True):
            text = text.replace(secret, "[REDACTED]")
        text = _URL.sub(lambda m: safe_uri(m.group(0)), text)
        text = _BEARER.sub("Bearer [REDACTED]", text)
        text = _ASSIGN.sub(lambda m: m.group(1) + "[REDACTED]", text)
        # Avoid terminal control / escape injection in upstream exception messages.
        return "".join(c if ord(c) >= 32 or c in "\n\t" else "?" for c in text)

    def data(self, value: Any) -> Any:
        if isinstance(value, dict):
            return {
                str(k): "[REDACTED]" if _SECRET_KEY.search(str(k)) else self.data(v) for k, v in value.items()
            }
        if isinstance(value, (list, tuple)):
            return [self.data(v) for v in value]
        if isinstance(value, str):
            return self.text(value)
        return value


class RedactingFilter(logging.Filter):
    def __init__(self, redactor: Redactor):
        super().__init__()
        self.redactor = redactor

    def filter(self, record: logging.LogRecord) -> bool:
        record.msg = self.redactor.text(record.getMessage())
        record.args = ()
        if record.exc_info:
            formatter = logging.Formatter()
            record.exc_text = self.redactor.text(formatter.formatException(record.exc_info))
            record.exc_info = None
        return True


def configure_logging(redactor: Redactor) -> None:
    """CLI owns logging; never enable verbose HTTP wire logging."""
    handler = logging.StreamHandler()
    handler.addFilter(RedactingFilter(redactor))
    handler.setFormatter(logging.Formatter("%(levelname)s: %(message)s"))
    logging.basicConfig(level=logging.WARNING, handlers=[handler], force=True)
    for name in ("urllib3", "requests", "httpx", "httpcore", "pyiceberg"):
        logger = logging.getLogger(name)
        logger.handlers.clear()
        logger.propagate = True
        logger.setLevel(logging.WARNING)
