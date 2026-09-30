"""Version-scoped REST client with finite request timeouts and no transport retries.

PyIceberg 0.10's REST client has no portable documented timeout property. Wrap
only this catalog's sessions, never requests globally. Keep PyIceberg auth and
signing adapters intact. This guarded extension point is integration-tested when
PyIceberg is installed; --help does not import this module.
"""
from __future__ import annotations

from functools import wraps
from typing import Any

from .errors import CompatibilityError


def bound_session(session: Any, timeout: float) -> Any:
    """Preserve authentication while preventing timeout=None and automatic retries."""
    from urllib3.util.retry import Retry

    request = session.request

    @wraps(request)
    def bounded_request(method, url, **kwargs):
        if kwargs.get("timeout") is None:
            kwargs["timeout"] = timeout
        # REST endpoints are configured explicitly. Refuse redirecting credentials
        # or replaying a state-changing POST against a different endpoint.
        kwargs["allow_redirects"] = False
        return request(method, url, **kwargs)

    session.request = bounded_request
    for adapter in session.adapters.values():
        if hasattr(adapter, "max_retries"):
            adapter.max_retries = Retry(total=0, connect=0, read=0, status=0, redirect=0)
    return session


def create_catalog(name: str, timeout: float, properties: dict[str, Any]):
    from pyiceberg.catalog.rest import RestCatalog

    if not callable(getattr(RestCatalog, "_create_session", None)):
        raise CompatibilityError("PyIceberg REST session API changed; cannot safely apply request timeouts")

    class BoundedRestCatalog(RestCatalog):
        def _create_session(self):
            return bound_session(super()._create_session(), timeout)

    return BoundedRestCatalog(name, **properties)
