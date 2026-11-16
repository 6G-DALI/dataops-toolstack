"""
Tests for the DAG's side of the PyPOTS service: dali.pypots (the client) and the way dali.processing turns
`imputation.runs` with lib "pypots" into runs and runs them.

A real HTTP server on localhost stands in for the service, so what is checked includes the request on the wire and how
the files it left on the shared volume become the summary the scoring and upload steps read.

Run with:
    cd airflow
    PYTHONPATH=plugins pytest tests/test_pypots.py -v
"""

from __future__ import annotations

import json
import threading
from http.server import BaseHTTPRequestHandler, HTTPServer
from pathlib import Path

import pandas as pd
import pytest

from dali import pypots
from dali.processing import _impute_one, imputation_runs


class FakeService:
    def __init__(self):
        self.requests: list[dict] = []
        self.headers_seen: list[dict] = []
        self.fail_with: str | None = None
        self.refuse: tuple[int, str] | None = None
        service = self

        class Handler(BaseHTTPRequestHandler):
            def log_message(self, *args):
                pass

            def _send(self, status: int, body: dict):
                data = json.dumps(body).encode()
                self.send_response(status)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(data)))
                self.end_headers()
                self.wfile.write(data)

            def do_POST(self):
                body = json.loads(self.rfile.read(int(self.headers["Content-Length"])))
                service.requests.append(body)
                service.headers_seen.append(dict(self.headers))
                if service.refuse:
                    return self._send(service.refuse[0], {"detail": service.refuse[1]})
                self._send(202, {"id": "job1", "status": "queued"})

            def do_GET(self):
                body = service.requests[-1]
                prepared = Path(body["prepared_dir"])
                out = prepared.parent / (prepared.name.removesuffix("_regularized") + "_generated")
                if service.fail_with:
                    return self._send(200, {"id": "job1", "status": "error", "error": service.fail_with,
                                            "log_tail": ["Traceback ...", "out of memory"]})
                out.mkdir(exist_ok=True)
                for kind, src in (("train", "train.csv"), ("test", "test_input.csv")):
                    if kind in body["inputs"]:
                        pd.read_csv(prepared / src).fillna(0.5).to_csv(
                            out / f"pypots_{body['method']}_{kind}_imputed.csv", index=False)
                self._send(200, {"id": "job1", "status": "done", "output_dir": str(out)})

        self.server = HTTPServer(("127.0.0.1", 0), Handler)
        self.url = f"http://127.0.0.1:{self.server.server_address[1]}"
        threading.Thread(target=self.server.serve_forever, daemon=True).start()

    def stop(self):
        self.server.shutdown()


def make_bundle(root: Path) -> Path:
    prepared = root / "dali-dataops-abc" / "data_regularized"
    prepared.mkdir(parents=True)
    (prepared / "meta.json").write_text(json.dumps({"target_cols": ["cpu", "ram"]}))
    pd.DataFrame({"t": [1, 2, 3, 4], "cpu": [1.0, None, 3.0, 4.0], "ram": [1.0, 2.0, None, 4.0]}) \
        .to_csv(prepared / "train.csv", index=False)
    pd.DataFrame({"t": [5, 6], "cpu": [None, 6.0], "ram": [5.0, 6.0]}).to_csv(prepared / "test_input.csv", index=False)
    return prepared


@pytest.fixture
def stack(tmp_path, monkeypatch):
    root = tmp_path / "wsp-work"
    root.mkdir()
    service = FakeService()
    monkeypatch.setenv("PYPOTS_URL", service.url)
    monkeypatch.setenv("DALI_WORK_ROOT", str(root))
    monkeypatch.setenv("PYPOTS_POLL_SECONDS", "0")
    monkeypatch.delenv("PYPOTS_TOKEN", raising=False)
    monkeypatch.delenv("PYPOTS_TIMEOUT_SECONDS", raising=False)
    yield root, service
    service.stop()


def test_a_job_is_submitted_and_summarised_from_the_files_the_service_wrote(stack):
    root, service = stack
    prepared = make_bundle(root)

    summary = pypots.impute(str(prepared), method="timesnet", params={"epochs": 5})

    assert service.requests == [{"prepared_dir": str(prepared), "method": "timesnet", "inputs": ["train", "test"],
                                 "params": {"epochs": 5}}]
    out = root / "dali-dataops-abc" / "data_generated"
    assert summary["lib"] == "pypots" and summary["engine"] == "pypots" and summary["output_dir"] == str(out)
    assert summary["files"]["train"] == {"path": str(out / "pypots_timesnet_train_imputed.csv"), "rows": 4,
                                         "nan_before": 2, "nan_after": 0, "filled": 2}
    assert summary["files"]["test"]["filled"] == 1


def test_the_token_is_sent_as_a_bearer_token(stack, monkeypatch):
    root, service = stack
    monkeypatch.setenv("PYPOTS_TOKEN", "s3cret")
    pypots.impute(str(make_bundle(root)), method="saits")
    assert service.headers_seen[0]["Authorization"] == "Bearer s3cret"


def test_a_failed_job_raises_with_the_service_error_and_the_end_of_its_log(stack):
    root, service = stack
    service.fail_with = "RuntimeError: saits failed (exit 1)"
    with pytest.raises(pypots.PypotsError, match=r"(?s)saits failed.*out of memory"):
        pypots.impute(str(make_bundle(root)), method="saits")


def test_a_refused_job_says_what_the_service_said(stack):
    root, service = stack
    service.refuse = (422, "pypots has no option 'lr'")
    with pytest.raises(pypots.PypotsError, match="422.*no option 'lr'"):
        pypots.impute(str(make_bundle(root)), method="saits", params={"lr": 1})


def test_without_a_service_url_the_run_fails_with_that_reason(stack, monkeypatch):
    root, _ = stack
    monkeypatch.delenv("PYPOTS_URL")
    with pytest.raises(pypots.PypotsError, match="PYPOTS_URL is not set"):
        pypots.impute(str(make_bundle(root)), method="saits")


def test_a_bundle_the_service_cannot_see_is_refused_before_anything_is_sent(stack, tmp_path):
    _, service = stack
    elsewhere = make_bundle(tmp_path / "elsewhere")
    with pytest.raises(pypots.PypotsError, match="not under DALI_WORK_ROOT"):
        pypots.impute(str(elsewhere), method="saits")
    assert service.requests == []


def test_an_unknown_method_is_refused(stack):
    root, _ = stack
    with pytest.raises(pypots.PypotsError, match="no method"):
        pypots.impute(str(make_bundle(root)), method="nope")


def test_a_pypots_run_goes_to_the_pypots_engine_with_its_options():
    runs = imputation_runs({"runs": [{"lib": "pypots", "method": "saits", "params": {"epochs": 20}}, "pypots/timesnet"]})
    assert runs == [{"lib": "pypots", "method": "saits", "engine": "pypots", "params": {"epochs": 20}},
                    {"lib": "pypots", "method": "timesnet", "engine": "pypots"}]


def test_pypots_is_not_in_the_default_runs():
    assert all(r["lib"] != "pypots" for r in imputation_runs({}))


def test_a_run_through_the_pipeline_helper_is_imputed_and_carries_its_files(stack):
    root, _ = stack
    prepared = make_bundle(root)
    run = imputation_runs({"runs": [{"lib": "pypots", "method": "transformer", "params": {"epochs": 2}}]})[0]

    summary = _impute_one(str(prepared), {"rows": 6}, **run)

    assert summary["status"] == "imputed", summary
    assert summary["lib"] == "pypots" and summary["engine"] == "pypots"
    assert summary["filled"] == 3 and summary["missing_before"] == 3


def test_a_service_failure_is_a_recorded_error_and_does_not_raise(stack):
    root, service = stack
    service.fail_with = "RuntimeError: brits failed (exit 3)"
    run = imputation_runs({"runs": ["pypots/brits"]})[0]

    summary = _impute_one(str(make_bundle(root)), {}, **run)

    assert summary["status"] == "error" and "brits failed (exit 3)" in summary["reason"]
    assert summary["files"] == {}


def test_the_pypots_engine_is_only_for_the_pypots_lib(stack):
    root, _ = stack
    summary = _impute_one(str(make_bundle(root)), {}, lib="darts", method="linear", engine="pypots")
    assert summary["status"] == "error" and "only applies to lib 'pypots'" in summary["reason"]
