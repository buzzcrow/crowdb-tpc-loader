import requests

from crowdb_tpc_loader.rest_catalog import bound_session


def test_bound_session_timeout_no_redirects_no_retries():
    session = requests.Session()
    calls = []
    session.request = lambda method, url, **kwargs: calls.append((method, url, kwargs))
    bound_session(session, 7.5)
    session.get("https://catalog.example/config")
    session.post("https://catalog.example/commit", timeout=None, json={"x": 1})
    assert all(kwargs["timeout"] == 7.5 for _, _, kwargs in calls)
    assert all(kwargs["allow_redirects"] is False for _, _, kwargs in calls)
    assert calls[1][2]["json"] == {"x": 1}
    assert all(adapter.max_retries.total == 0 for adapter in session.adapters.values())


def test_existing_auth_header_preserved():
    session = requests.Session()
    session.headers["Authorization"] = "Bearer test-secret"
    original_adapters = dict(session.adapters)
    bound_session(session, 3)
    assert session.headers["Authorization"] == "Bearer test-secret"
    assert session.adapters == original_adapters
