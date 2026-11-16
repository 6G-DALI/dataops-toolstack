"""
Tests for the PyPOTS service, with a stand-in runner instead of the real one (which needs torch and minutes).

The stand-in records the arguments it was called with and writes the files the real runner writes, so what is checked
is the service's own job: what it runs, with which arguments, where, and what it refuses.

Run with:
    cd pypots-service
    pytest tests -v
"""

from __future__ import annotations

import json
import sys
import time
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import app as service  # noqa: E402

STUB = """\
import argparse, json, sys
from pathlib import Path
p = argparse.ArgumentParser()
p.add_argument("--prepared-dir"); p.add_argument("--output-dir"); p.add_argument("--method")
p.add_argument("--inputs", nargs="+")
args, rest = p.parse_known_args()
out = Path(args.output_dir); out.mkdir(parents=True, exist_ok=True)
with open(out / "calls.jsonl", "a") as f:
    f.write(json.dumps({"argv": sys.argv[1:]}) + "\\n")
if args.method == "brits":
    print("boom", file=sys.stderr); sys.exit(3)
for split in args.inputs:
    (out / f"pypots_{args.method}_{split}_imputed.csv").write_text("a\\n1\\n")
print("stub done")
"""


@pytest.fixture
def env(tmp_path, monkeypatch):
    root = tmp_path / "work"
    app_dir = tmp_path / "runner"
    root.mkdir()
    app_dir.mkdir()
    (app_dir / "run_imputation.py").write_text(STUB)
    bundle = root / "data_regularized"
    bundle.mkdir()
    (bundle / "meta.json").write_text("{}")
    monkeypatch.setenv("PYPOTS_WORK_ROOT", str(root))
    monkeypatch.setenv("PYPOTS_APP_DIR", str(app_dir))
    monkeypatch.delenv("PYPOTS_TOKEN", raising=False)
    service._jobs.clear()
    return TestClient(service.app), bundle


def wait(client: TestClient, job_id: str) -> dict:
    for _ in range(100):
        job = client.get(f"/jobs/{job_id}").json()
        if job["status"] in ("done", "error"):
            return job
        time.sleep(0.05)
    raise AssertionError("the job did not finish")


def test_a_job_runs_the_runner_on_the_bundle_and_writes_next_to_it(env):
    client, bundle = env
    job = client.post("/jobs", json={"prepared_dir": str(bundle), "method": "saits",
                                     "params": {"epochs": 3, "window": 50}}).json()
    done = wait(client, job["id"])
    assert done["status"] == "done"
    out = bundle.parent / "data_generated"
    assert done["output_dir"] == str(out)
    assert (out / "pypots_saits_test_imputed.csv").exists() and (out / "pypots_saits_train_imputed.csv").exists()
    argv = json.loads((out / "calls.jsonl").read_text().splitlines()[0])["argv"]
    assert argv[argv.index("--method") + 1] == "saits"
    assert argv[argv.index("--epochs") + 1] == "3" and argv[argv.index("--window") + 1] == "50"


def test_inputs_limit_what_is_written(env):
    client, bundle = env
    job = client.post("/jobs", json={"prepared_dir": str(bundle), "method": "timesnet", "inputs": ["test"]}).json()
    assert wait(client, job["id"])["status"] == "done"
    out = bundle.parent / "data_generated"
    assert (out / "pypots_timesnet_test_imputed.csv").exists()
    assert not (out / "pypots_timesnet_train_imputed.csv").exists()


def test_a_failing_runner_is_an_error_job_with_its_log(env):
    client, bundle = env
    job = client.post("/jobs", json={"prepared_dir": str(bundle), "method": "brits"}).json()
    done = wait(client, job["id"])
    assert done["status"] == "error" and "exit 3" in done["error"]
    assert any("boom" in line for line in done["log_tail"])


@pytest.mark.parametrize("params", [{"lr": 1}, {"epochs": 0}, {"epochs": "5"}, {"window": -1}, {"epochs": True}])
def test_options_the_runner_does_not_take_or_with_bad_values_are_refused(env, params):
    client, bundle = env
    answer = client.post("/jobs", json={"prepared_dir": str(bundle), "method": "saits", "params": params})
    assert answer.status_code == 422


def test_window_and_stride_may_be_zero(env):
    client, bundle = env
    job = client.post("/jobs", json={"prepared_dir": str(bundle), "method": "saits",
                                     "params": {"window": 0, "stride": 0}})
    assert job.status_code == 202


def test_unknown_methods_and_bundles_outside_the_shared_volume_are_refused(env, tmp_path):
    client, bundle = env
    assert client.post("/jobs", json={"prepared_dir": str(bundle), "method": "nope"}).status_code == 422
    outside = tmp_path / "elsewhere_regularized"
    outside.mkdir()
    (outside / "meta.json").write_text("{}")
    assert client.post("/jobs", json={"prepared_dir": str(outside), "method": "saits"}).status_code == 422
    assert client.post("/jobs", json={"prepared_dir": str(bundle.parent), "method": "saits"}).status_code == 422


def test_the_token_protects_everything_but_health(env, monkeypatch):
    client, bundle = env
    monkeypatch.setenv("PYPOTS_TOKEN", "secret")
    assert client.get("/health").status_code == 200
    assert client.get("/jobs").status_code == 401
    assert client.post("/jobs", json={"prepared_dir": str(bundle), "method": "saits"}).status_code == 401
    assert client.get("/jobs", headers={"Authorization": "Bearer secret"}).status_code == 200
