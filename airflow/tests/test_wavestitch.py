"""
Tests for the DAG's side of the WaveStitch+ service: dali.wavestitch (the client) and the way dali.processing turns
`imputation.runs` into runs and runs them.

A real HTTP server on localhost stands in for the service, so what is checked includes the bytes on the wire: the
request the service would receive, how the DAG polls, and how the filled files it left on the shared volume become the
summary the scoring and upload steps read.

Run with:
    cd airflow
    PYTHONPATH=plugins pytest tests/test_wavestitch.py -v
"""

from __future__ import annotations

import json
import threading
from http.server import BaseHTTPRequestHandler, HTTPServer
from pathlib import Path

import pandas as pd
import pytest

from dali import wavestitch
from dali.processing import _impute_one, _scratch_dir, imputation_runs


# ── a stand-in service ──────────────────────────────────────────────────────────────────────────────────────────────

class FakeService:
    """Answers POST /jobs and GET /jobs/<id> the way the real service does, and writes the filled files."""

    def __init__(self, root: Path):
        self.root = root
        self.requests: list[dict] = []
        self.headers_seen: list[dict] = []
        self.polls_until_done = 1
        self.fail_with: str | None = None
        self.write_outputs = True
        self.refuse: tuple[int, str] | None = None
        self._polls = 0
        service = self

        class Handler(BaseHTTPRequestHandler):
            def log_message(self, *args):  # quiet
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
                self._send(202, {"id": "job1", "status": "queued", "steps": []})

            def do_GET(self):
                service._polls += 1
                body = service.requests[-1]
                prepared = Path(body["prepared_dir"])
                out = prepared.parent / (prepared.name.removesuffix("_regularized") + "_generated")
                if service._polls < service.polls_until_done:
                    return self._send(200, {"id": "job1", "status": "running",
                                            "steps": [{"method": body["method"], "status": "running"}]})
                if service.fail_with:
                    return self._send(200, {"id": "job1", "status": "error", "error": service.fail_with,
                                            "steps": [{"method": body["method"], "status": "error"}],
                                            "log_tail": ["Traceback ...", "CUDA out of memory"]})
                if service.write_outputs:
                    out.mkdir(exist_ok=True)
                    for kind, src in (("train", "train.csv"), ("test", "test_input.csv")):
                        if kind in body["inputs"]:
                            df = pd.read_csv(prepared / src).fillna(0.5)
                            df.to_csv(out / f"wavestitchplus_{body['method']}_{kind}_imputed.csv", index=False)
                self._send(200, {"id": "job1", "status": "done", "output_dir": str(out),
                                 "steps": [{"method": body["method"], "status": "done"}]})

        self.server = HTTPServer(("127.0.0.1", 0), Handler)
        self.url = f"http://127.0.0.1:{self.server.server_address[1]}"
        threading.Thread(target=self.server.serve_forever, daemon=True).start()

    def stop(self):
        self.server.shutdown()


def make_bundle(root: Path) -> Path:
    """A regularized bundle with two missing values in train and one in test."""
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
    service = FakeService(root)
    monkeypatch.setenv("WAVESTITCH_URL", service.url)
    monkeypatch.setenv("DALI_WORK_ROOT", str(root))
    monkeypatch.setenv("WAVESTITCH_POLL_SECONDS", "0")
    monkeypatch.delenv("WAVESTITCH_TOKEN", raising=False)
    monkeypatch.delenv("WAVESTITCH_TIMEOUT_SECONDS", raising=False)
    yield root, service
    service.stop()


# ── the client ──────────────────────────────────────────────────────────────────────────────────────────────────────

def test_a_job_is_submitted_polled_and_summarised_from_the_files_the_service_wrote(stack):
    root, service = stack
    service.polls_until_done = 3
    prepared = make_bundle(root)

    summary = wavestitch.impute(str(prepared), method="v1", device="cpu", params={"fast": True})

    assert service.requests == [{"prepared_dir": str(prepared), "method": "v1", "device": "cpu",
                                 "inputs": ["train", "test"], "params": {"fast": True}, "v1_params": {}}]
    assert service._polls == 3
    out = root / "dali-dataops-abc" / "data_generated"
    assert summary["lib"] == "wavestitchplus" and summary["method"] == "v1" and summary["engine"] == "wavestitch"
    assert summary["output_dir"] == str(out)
    assert summary["target_cols"] == ["cpu", "ram"]
    assert summary["files"]["train"] == {"path": str(out / "wavestitchplus_v1_train_imputed.csv"), "rows": 4,
                                         "nan_before": 2, "nan_after": 0, "filled": 2}
    assert summary["files"]["test"]["filled"] == 1


def test_the_summary_has_the_shape_impute_bundle_returns(stack):
    """Scoring, the final dataset and the uploads read exactly these keys."""
    root, _ = stack
    summary = wavestitch.impute(str(make_bundle(root)), method="harpoon")
    assert set(summary) == {"method", "lib", "engine", "output_dir", "target_cols", "files"}
    for info in summary["files"].values():
        assert set(info) == {"path", "rows", "nan_before", "nan_after", "filled"}


def test_the_runner_names_for_v1_and_v2_are_accepted(stack):
    root, service = stack
    prepared = make_bundle(root)
    assert wavestitch.impute(str(prepared), method="full")["method"] == "v1"
    assert wavestitch.impute(str(prepared), method="anchored")["method"] == "v2"
    assert [r["method"] for r in service.requests] == ["v1", "v2"]


def test_the_token_is_sent_as_a_bearer_token(stack, monkeypatch):
    root, service = stack
    monkeypatch.setenv("WAVESTITCH_TOKEN", "s3cret")
    wavestitch.impute(str(make_bundle(root)), method="v1")
    assert service.headers_seen[0]["Authorization"] == "Bearer s3cret"


def test_a_failed_job_raises_with_the_service_error_and_the_end_of_its_log(stack):
    root, service = stack
    service.fail_with = "RuntimeError: v1 failed (exit 1); see the job log"
    with pytest.raises(wavestitch.WaveStitchError) as err:
        wavestitch.impute(str(make_bundle(root)), method="v1")
    assert "v1 failed (exit 1)" in str(err.value) and "CUDA out of memory" in str(err.value)


def test_a_job_that_finishes_without_writing_files_is_an_error_not_an_empty_success(stack):
    root, service = stack
    service.write_outputs = False
    with pytest.raises(wavestitch.WaveStitchError, match="wrote no wavestitchplus_v1"):
        wavestitch.impute(str(make_bundle(root)), method="v1")


def test_a_refused_job_says_what_the_service_said(stack):
    root, service = stack
    service.refuse = (422, "v2 has no option 'tua'")
    with pytest.raises(wavestitch.WaveStitchError, match="answered 422.*no option 'tua'"):
        wavestitch.impute(str(make_bundle(root)), method="v2", params={"tua": 1})


def test_a_job_that_takes_longer_than_the_timeout_is_given_up_on(stack, monkeypatch):
    root, service = stack
    service.polls_until_done = 10_000
    monkeypatch.setenv("WAVESTITCH_TIMEOUT_SECONDS", "0")
    with pytest.raises(wavestitch.WaveStitchError, match="did not finish within 0 s"):
        wavestitch.impute(str(make_bundle(root)), method="v1")


def test_without_a_service_url_the_run_fails_with_that_reason(stack, monkeypatch):
    root, _ = stack
    monkeypatch.delenv("WAVESTITCH_URL")
    with pytest.raises(wavestitch.WaveStitchError, match="WAVESTITCH_URL is not set"):
        wavestitch.impute(str(make_bundle(root)), method="v1")


def test_an_unreachable_service_is_reported_as_such(stack, monkeypatch):
    root, _ = stack
    monkeypatch.setenv("WAVESTITCH_URL", "http://127.0.0.1:9")
    with pytest.raises(wavestitch.WaveStitchError, match="could not reach the WaveStitch"):
        wavestitch.impute(str(make_bundle(root)), method="v1")


def test_a_bundle_the_service_cannot_see_is_refused_before_anything_is_sent(stack, tmp_path, monkeypatch):
    root, service = stack
    elsewhere = make_bundle(tmp_path / "somewhere-else")
    with pytest.raises(wavestitch.WaveStitchError, match="not under DALI_WORK_ROOT"):
        wavestitch.impute(str(elsewhere), method="v1")
    monkeypatch.delenv("DALI_WORK_ROOT")
    with pytest.raises(wavestitch.WaveStitchError, match="DALI_WORK_ROOT is not set"):
        wavestitch.impute(str(make_bundle(root)), method="v1")
    assert service.requests == []


def test_an_unknown_method_is_refused(stack):
    root, service = stack
    with pytest.raises(wavestitch.WaveStitchError, match="no method 'saits'"):
        wavestitch.impute(str(make_bundle(root)), method="saits")
    assert service.requests == []


# ── imputation.runs ─────────────────────────────────────────────────────────────────────────────────────────────────

def test_a_wavestitchplus_run_goes_to_the_service_engine_with_its_options():
    runs = imputation_runs({"runs": [
        {"lib": "wavestitchplus", "method": "v1", "device": "gpu", "params": {"fast": True}},
        "wavestitchplus/harpoon",
        {"lib": "wavestitchplus", "method": "anchored", "v1_params": {"fast": True}},
    ]})
    assert runs == [
        {"lib": "wavestitchplus", "method": "v1", "engine": "wavestitch", "device": "gpu", "params": {"fast": True}},
        {"lib": "wavestitchplus", "method": "harpoon", "engine": "wavestitch"},
        {"lib": "wavestitchplus", "method": "v2", "engine": "wavestitch", "v1_params": {"fast": True}},
    ]


def test_v1_is_moved_in_front_of_the_runs_that_read_its_output():
    runs = imputation_runs({"runs": ["darts/linear", "wavestitchplus/v2", "darts/nearest", "wavestitchplus/v1"]})
    assert [(r["lib"], r["method"]) for r in runs] == [
        ("darts", "linear"), ("wavestitchplus", "v1"), ("wavestitchplus", "v2"), ("darts", "nearest")]


def test_runs_that_are_already_in_order_are_left_alone():
    runs = imputation_runs({"runs": ["wavestitchplus/v1", "darts/linear", "wavestitchplus/harpoon"]})
    assert [(r["lib"], r["method"]) for r in runs] == [
        ("wavestitchplus", "v1"), ("darts", "linear"), ("wavestitchplus", "harpoon")]


def test_v2_and_its_alias_are_one_run():
    runs = imputation_runs({"runs": ["wavestitchplus/v2", "wavestitchplus/anchored"]})
    assert len(runs) == 1


def test_the_other_engines_are_unchanged():
    runs = imputation_runs({"runs": ["darts/linear", "darts/kalman", "imputegap/mean"]})
    assert [(r["method"], r["engine"]) for r in runs] == [("linear", "pandas"), ("kalman", "darts"), ("mean", "pandas")]
    assert imputation_runs({}) == [{"lib": "darts", "method": "linear", "engine": "pandas"}]


def test_the_default_runs_are_the_darts_fills_and_kalman_then_the_three_wavestitch_methods_in_order():
    from dali.processing import DEFAULT_IMPUTATION_CONFIG

    runs = imputation_runs(DEFAULT_IMPUTATION_CONFIG)
    # v1 runs in its fast mode by default; the others take their own settings
    assert [r["params"] for r in runs if r.get("params")] == [{"fast": True}]
    assert next(r for r in runs if r["method"] == "v1")["params"] == {"fast": True}
    assert [(r["lib"], r["method"], r["engine"]) for r in runs] == [
        ("darts", "linear", "pandas"), ("darts", "nearest", "pandas"), ("darts", "kalman", "darts"),
        ("wavestitchplus", "v1", "wavestitch"), ("wavestitchplus", "v2", "wavestitch"),
        ("wavestitchplus", "harpoon", "wavestitch")]
    assert runs[0]["method"] == "linear"          # the quick fill stays the primary run


# ── a run, end to end through dali.processing ───────────────────────────────────────────────────────────────────────

def test_a_run_through_the_pipeline_helper_is_imputed_and_carries_its_files(stack):
    root, _ = stack
    prepared = make_bundle(root)
    bundle = {"rows": 6}
    run = imputation_runs({"runs": [{"lib": "wavestitchplus", "method": "v1", "params": {"fast": True}}]})[0]

    summary = _impute_one(str(prepared), bundle, **run)

    assert summary["status"] == "imputed", summary
    assert summary["lib"] == "wavestitchplus" and summary["engine"] == "wavestitch"
    assert summary["filled"] == 3 and summary["missing_before"] == 3
    assert summary["reason"] == "3 missing values filled with wavestitchplus/v1"


def test_a_service_failure_is_a_recorded_error_and_does_not_raise(stack):
    root, service = stack
    service.fail_with = "RuntimeError: v1 failed (exit 1)"
    run = imputation_runs({"runs": ["wavestitchplus/v1"]})[0]

    summary = _impute_one(str(make_bundle(root)), {}, **run)

    assert summary["status"] == "error"
    assert "v1 failed (exit 1)" in summary["reason"]
    assert summary["lib"] == "wavestitchplus" and summary["files"] == {}


def test_the_wavestitch_engine_is_only_for_the_wavestitchplus_lib(stack):
    root, _ = stack
    summary = _impute_one(str(make_bundle(root)), {}, lib="darts", method="linear", engine="wavestitch")
    assert summary["status"] == "error" and "only applies to lib 'wavestitchplus'" in summary["reason"]


# ── the shared scratch directory ────────────────────────────────────────────────────────────────────────────────────

def test_the_task_decorators_are_on_the_tasks_and_not_on_the_helper_between_them():
    """@task on the wrong function turned cleanup_workdir into a plain function, which the DAG then called at
    parse time with an XCom reference. The conftest stub of @task sets `.function`, so its presence tells."""
    from dali import processing
    for task in ("run_dataops_pipeline", "report_pipeline_outcome", "cleanup_workdir"):
        assert hasattr(getattr(processing, task), "function"), f"{task} lost its @task decorator"
    assert not hasattr(processing._scratch_dir, "function"), "_scratch_dir must be a plain helper, not a task"


def test_scratch_directories_go_under_the_shared_root_when_it_is_set(stack):
    root, _ = stack
    scratch = _scratch_dir()
    assert scratch.parent == root and scratch.name.startswith("dali-dataops-")


def test_scratch_directories_stay_in_the_system_temp_without_a_shared_root(monkeypatch, tmp_path):
    monkeypatch.delenv("DALI_WORK_ROOT", raising=False)
    scratch = _scratch_dir()
    assert tmp_path not in scratch.parents and scratch.name.startswith("dali-dataops-")


def test_a_shared_root_that_does_not_exist_yet_is_created(monkeypatch, tmp_path):
    monkeypatch.setenv("DALI_WORK_ROOT", str(tmp_path / "new" / "root"))
    assert _scratch_dir().parent == tmp_path / "new" / "root"


# ── unit conversion (convert_units) ─────────────────────────────────────────────────────────────────────────────────

def test_units_are_not_converted_by_default_whatever_the_run_passes():
    from dali.processing import DEFAULT_IMPUTATION_CONFIG, imputation_config_for

    assert imputation_config_for({})["convert_units"] is False
    assert imputation_config_for({"imputation": {}})["convert_units"] is False
    # A run's own imputation replaces the defaults (it is not merged), so the default is applied to it:
    assert imputation_config_for({"imputation": {"runs": ["darts/linear"]}})["convert_units"] is False
    assert DEFAULT_IMPUTATION_CONFIG["convert_units"] is False


def test_a_run_can_ask_for_the_conversions_and_the_callers_dict_is_left_alone():
    from dali.processing import DEFAULT_IMPUTATION_CONFIG, imputation_config_for

    mine = {"runs": ["darts/linear"], "convert_units": True}
    assert imputation_config_for({"imputation": mine})["convert_units"] is True
    asked = {"runs": ["darts/linear"]}
    imputation_config_for({"imputation": asked})
    assert asked == {"runs": ["darts/linear"]}                      # nothing added to the caller's dict
    assert "convert_units" in DEFAULT_IMPUTATION_CONFIG


@pytest.mark.parametrize("config, expected", [
    ({"convert_units": False}, False),
    ({"convert_units": True}, True),
    ({}, True),                      # the pipeline's own default is unchanged: on
])
def test_the_pipeline_passes_the_flag_to_preprocess_csv(monkeypatch, tmp_path, config, expected):
    pytest.importorskip("pandera")
    import data_process_modules.transform as transform
    import minimal_dataops

    seen = {}

    def fake_preprocess(**kwargs):
        seen.update(kwargs)
        return {"target_cols": ["a"]}

    monkeypatch.setattr(transform, "preprocess_csv", fake_preprocess)
    handoff = minimal_dataops._build_handoff(
        {"mode": "time_series", "report": {"recommendations": {"ts_imputation": True}}},
        output_csv=str(tmp_path / "x_remediated.csv"), ts_col="timestamp", imputation_cfg=config)

    assert handoff["bundle_written"] is True
    assert seen["convert_units"] is expected
