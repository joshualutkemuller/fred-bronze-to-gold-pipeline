from fred_pipeline.audit import EtlRun, RunStatus
from fred_pipeline.config import Environment, PipelineConfig
from fred_pipeline.fred_client import FredAPIError
from fred_pipeline.manifest import SeriesSpec
from fred_pipeline.notify import (
    format_run_summary,
    send_notification,
    should_notify,
)
from fred_pipeline.pipeline import FredPipeline


def _run(status, failed=0, total=2):
    run = EtlRun(environment="dev")
    run.status = status
    run.series_total = total
    run.series_failed = failed
    run.series_succeeded = total - failed
    run.ended_at = run.started_at
    return run


def test_should_notify_policies():
    ok = _run(RunStatus.SUCCEEDED)
    bad = _run(RunStatus.FAILED, failed=2)
    part = _run(RunStatus.PARTIAL, failed=1)
    assert should_notify(ok, "never") is False
    assert should_notify(bad, "never") is False
    assert should_notify(ok, "always") is True
    assert should_notify(ok, "failure") is False
    assert should_notify(bad, "failure") is True
    assert should_notify(part, "failure") is True


def test_format_includes_status_and_failures():
    run = EtlRun(environment="prod")
    sr = run.start_series("GDP")
    sr.complete(RunStatus.FAILED, error_message="boom")
    run.finalize()
    text = format_run_summary(run, environment="prod")
    assert "FAILED" in text
    assert "GDP" in text and "boom" in text
    assert "prod" in text


def test_send_uses_transport_on_failure():
    captured = {}

    def transport(url, payload):
        captured["url"] = url
        captured["payload"] = payload

    run = _run(RunStatus.FAILED, failed=1)
    sent = send_notification(
        run, webhook_url="https://hook", notify_on="failure", transport=transport
    )
    assert sent is True
    assert captured["url"] == "https://hook"
    assert "text" in captured["payload"]


def test_no_send_on_success_when_failure_only():
    calls = []
    run = _run(RunStatus.SUCCEEDED)
    sent = send_notification(
        run, webhook_url="https://hook", notify_on="failure",
        transport=lambda u, p: calls.append(1),
    )
    assert sent is False
    assert calls == []


def test_no_webhook_logs_but_returns_false():
    run = _run(RunStatus.FAILED, failed=1)
    assert send_notification(run, webhook_url="", notify_on="failure") is False


def test_pipeline_notifies_on_failure(observations_payload):
    cfg = PipelineConfig(
        environment=Environment.DEV, fred_api_key="k",
        alert_webhook_url="https://hook", notify_on="failure",
    )
    captured = []

    class Client:
        def get_observations(self, series_id, **kw):
            raise FredAPIError("bad id", 400)

    pipe = FredPipeline(
        cfg, client=Client(), warehouse=None,
        notify_transport=lambda url, payload: captured.append(payload),
    )
    run = pipe.run([SeriesSpec(series_id="X", title="X", frequency="d")])
    assert run.status == RunStatus.FAILED
    assert len(captured) == 1
    assert "X" in captured[0]["text"]
