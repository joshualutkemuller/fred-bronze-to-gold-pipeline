"""Tests for the shared source transport and the SourceClient contract."""

import pytest

from fred_pipeline.sources.base import HTTPSource, RateLimiter, SourceClient, SourceError
from fred_pipeline.sources.bls import BLSClient
from fred_pipeline.sources.fred import FredClient


class _Source(HTTPSource):
    """Minimal concrete source used to exercise the shared transport."""

    source_name = "TEST"

    def __init__(self, session, **kw):
        sleep = kw.pop("sleep", lambda _s: None)
        super().__init__(base_url="https://example.test/api", session=session,
                         sleep=sleep, **kw)

    def _default_query(self):
        return {"token": "abc"}

    def fetch(self, endpoint):
        return self._request(endpoint, {"q": "1"})


def test_default_query_is_injected(fake_session_cls, fake_response_cls):
    session = fake_session_cls([fake_response_cls({"ok": True})])
    out = _Source(session).fetch("thing")
    assert out == {"ok": True}
    # both the per-call param and the source's default query are sent
    assert session.calls[0]["params"] == {"q": "1", "token": "abc"}


def test_shared_retry_then_success(fake_session_cls, fake_response_cls):
    session = fake_session_cls([
        fake_response_cls(None, status_code=503),
        fake_response_cls(None, status_code=502),
        fake_response_cls({"ok": True}, status_code=200),
    ])
    out = _Source(session, max_retries=5).fetch("thing")
    assert out == {"ok": True}
    assert len(session.calls) == 3


def test_retry_after_header_controls_retry_sleep(fake_session_cls, fake_response_cls):
    sleeps = []
    retry = fake_response_cls(None, status_code=429)
    retry.headers = {"Retry-After": "7"}
    session = fake_session_cls([
        retry,
        fake_response_cls({"ok": True}, status_code=200),
    ])

    out = _Source(
        session, max_retries=1, rate_limit_per_minute=0, sleep=sleeps.append
    ).fetch("thing")

    assert out == {"ok": True}
    assert sleeps == [7.0]


def test_non_retryable_raises_source_error(fake_session_cls, fake_response_cls):
    session = fake_session_cls([fake_response_cls({"error_message": "nope"},
                                                  status_code=400)])
    with pytest.raises(SourceError) as exc:
        _Source(session).fetch("thing")
    assert exc.value.status_code == 400
    assert len(session.calls) == 1


def test_exhausts_retries(fake_session_cls, fake_response_cls):
    session = fake_session_cls([fake_response_cls(None, status_code=503)
                                for _ in range(3)])
    with pytest.raises(SourceError):
        _Source(session, max_retries=2).fetch("thing")
    assert len(session.calls) == 3


@pytest.mark.parametrize("client", [
    FredClient(api_key="k", session=object(), sleep=lambda _s: None),
    BLSClient(session=object(), sleep=lambda _s: None),
])
def test_clients_satisfy_source_protocol(client):
    # runtime_checkable Protocol: both real clients present the contract the
    # pipeline depends on (get_observations + normalize + source_name).
    assert isinstance(client, SourceClient)
    assert hasattr(client, "source_name")


def test_rate_limiter_is_shared_type():
    # the rate limiter lives on the base, so every source gets the same one
    assert isinstance(FredClient(api_key="k", session=object())._rate_limiter,
                      RateLimiter)
    assert isinstance(BLSClient(session=object())._rate_limiter, RateLimiter)
