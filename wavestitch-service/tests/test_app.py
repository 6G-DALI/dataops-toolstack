"""
Tests for the WaveStitch+ service, with stand-in runners instead of the real ones (which need torch and minutes).

The stand-ins record the arguments they were called with and write the files the real runners write, so what is
checked is the service's own job: which runners it starts, in what order, with which arguments, and where.

Run with:
    cd wavestitch-service
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
p.add_argument("--inputs", nargs="+"); p.add_argument("--device")
args, rest = p.parse_known_args()
tag = "{tag}"
out = Path(args.output_dir); out.mkdir(parents=True, exist_ok=True)
with open(out / "calls.jsonl", "a") as f:
    f.write(json.dumps({{"runner": tag, "argv": sys.argv[1:]}}) + "\\n")
if {fail}:
    print("boom", file=sys.stderr); sys.exit(3)
for split in args.inputs:
    (out / f"wavestitchplus_{{tag}}_{{split}}_imputed.csv").write_text("a\\n1\\n")
print("stub", tag, "done")
"""


@pytest.fixture
def env(tmp_path, monkeypatch):
    root = tmp_path / "work"
    app_dir = tmp_path / "runners"
    root.mkdir()
    app_dir.mkdir()
    for script, tag in (("run_imputation.py", "v1"), ("run_imputation_v2.py", "v2"),
                        ("run_imputation_harpoon.py", "harpoon")):
        (app_dir / script).write_text(STUB.format(tag=tag, fail="False"))
    monkeypatch.setenv("WSP_WORK_ROOT", str(root))
    monkeypatch.setenv("WSP_APP_DIR", str(app_dir))
    monkeypatch.delenv("WSP_TOKEN", raising=False)
    service._jobs.clear()
    return root, app_dir


def bundle(root: Path, name: str = "run-1_regularized") -> Path:
    prepared = root / "dali-dataops-abc" / name
    prepared.mkdir(parents=True)
    (prepared / "meta.json").write_text(json.dumps({"target_cols": ["a"]}))
    return prepared


def wait(client: TestClient, job_id: str, timeout: float = 20) -> dict:
    deadline = time.time() + timeout
    while time.time() < deadline:
        job = client.get(f"/jobs/{job_id}").json()
        if job["status"] in ("done", "error"):
            return job
        time.sleep(0.05)
    raise AssertionError("job did not finish")


def calls(prepared: Path) -> list[dict]:
    out = prepared.parent / "run-1_generated" / "calls.jsonl"
    return [json.loads(line) for line in out.read_text().splitlines()]


client = TestClient(service.app)


def test_v1_runs_one_runner_on_the_bundle_and_writes_where_every_engine_writes(env):
    root, _ = env
    prepared = bundle(root)
    posted = client.post("/jobs", json={"prepared_dir": str(prepared), "method": "v1"})
    assert posted.status_code == 202
    job = wait(client, posted.json()["id"])

    assert job["status"] == "done", job
    out = prepared.parent / "run-1_generated"
    assert job["output_dir"] == str(out)
    assert (out / "wavestitchplus_v1_train_imputed.csv").exists()
    assert (out / "wavestitchplus_v1_test_imputed.csv").exists()
    (call,) = calls(prepared)
    assert call["argv"][:6] == ["--prepared-dir", str(prepared), "--output-dir", str(out), "--method", "v1"]
    assert call["argv"][6:] == ["--inputs", "train", "test", "--device", "cpu"]


def test_v1_options_become_runner_flags(env):
    root, _ = env
    prepared = bundle(root)
    job = wait(client, client.post("/jobs", json={
        "prepared_dir": str(prepared), "method": "v1", "device": "auto", "inputs": ["test"],
        "params": {"fast": True, "em_iterations": 1, "guidance_scale": 0.2, "n_trials": 2}}).json()["id"])

    assert job["status"] == "done"
    argv = calls(prepared)[0]["argv"]
    assert argv[argv.index("--inputs") + 1:][:1] == ["test"]
    assert "--device" in argv and argv[argv.index("--device") + 1] == "auto"
    assert "--fast" in argv
    assert argv[argv.index("--em-iterations") + 1] == "1"
    assert argv[argv.index("--guidance-scale") + 1] == "0.2"


def test_v2_without_a_v1_run_runs_v1_first_with_the_v1_options_then_anchors_its_output(env):
    root, _ = env
    prepared = bundle(root)
    job = wait(client, client.post("/jobs", json={
        "prepared_dir": str(prepared), "method": "v2",
        "params": {"tau": 4.0}, "v1_params": {"fast": True}}).json()["id"])

    assert job["status"] == "done", job
    v1, v2 = calls(prepared)
    assert (v1["runner"], v2["runner"]) == ("v1", "v2")
    assert "--fast" in v1["argv"] and "--tau" not in v1["argv"]
    assert "--tau" in v2["argv"] and "--fast" not in v2["argv"]
    out = prepared.parent / "run-1_generated"
    assert v2["argv"][v2["argv"].index("--reuse-diffusion") + 1] == str(out / "wavestitchplus_v1_test_imputed.csv")
    assert v2["argv"][v2["argv"].index("--method") + 1] == "anchored"
    assert [s["method"] for s in job["steps"]] == ["v1", "v2"]


def test_a_later_job_reuses_the_v1_output_already_on_the_bundle(env):
    root, _ = env
    prepared = bundle(root)
    wait(client, client.post("/jobs", json={"prepared_dir": str(prepared), "method": "v1"}).json()["id"])
    job = wait(client, client.post("/jobs", json={"prepared_dir": str(prepared), "method": "harpoon"}).json()["id"])

    assert job["status"] == "done"
    assert [c["runner"] for c in calls(prepared)] == ["v1", "harpoon"]


def test_a_failing_runner_fails_the_job_with_its_log_and_stops_the_chain(env):
    root, app_dir = env
    (app_dir / "run_imputation.py").write_text(STUB.format(tag="v1", fail="True"))
    prepared = bundle(root)
    job = wait(client, client.post("/jobs", json={"prepared_dir": str(prepared), "method": "harpoon"}).json()["id"])

    assert job["status"] == "error"
    assert "v1 failed (exit 3)" in job["error"]
    assert "boom" in "\n".join(job["log_tail"])
    assert [c["runner"] for c in calls(prepared)] == ["v1"]   # harpoon never started


def test_a_bundle_outside_the_work_root_is_refused(env, tmp_path):
    elsewhere = tmp_path / "elsewhere"
    elsewhere.mkdir()
    (elsewhere / "meta.json").write_text("{}")
    response = client.post("/jobs", json={"prepared_dir": str(elsewhere), "method": "v1"})
    assert response.status_code == 422 and "must be inside" in response.json()["detail"]


def test_a_path_that_climbs_out_of_the_work_root_is_refused(env):
    root, _ = env
    bundle(root)
    sneaky = f"{root}/dali-dataops-abc/run-1_regularized/../../../"
    assert client.post("/jobs", json={"prepared_dir": sneaky, "method": "v1"}).status_code == 422


def test_a_directory_that_is_not_a_bundle_is_refused(env):
    root, _ = env
    empty = root / "x"
    empty.mkdir()
    response = client.post("/jobs", json={"prepared_dir": str(empty), "method": "v1"})
    assert response.status_code == 422 and "meta.json" in response.json()["detail"]


@pytest.mark.parametrize("method, params", [
    ("v1", {"learning_rate": 1}),                 # not an option of the runner
    ("v1", {"em_iterations": "three"}),           # wrong type
    ("v1", {"em_iterations": True}),              # a bool is not an int
    ("v2", {"prior": "cubic"}),                   # not one of the choices
    ("harpoon", {"model_tag": "--rm"}),           # would be read as another flag
])
def test_options_the_runner_does_not_have_or_cannot_read_are_refused(env, method, params):
    root, _ = env
    prepared = bundle(root)
    response = client.post("/jobs", json={"prepared_dir": str(prepared), "method": method, "params": params})
    assert response.status_code == 422


def test_an_unknown_method_is_refused(env):
    root, _ = env
    prepared = bundle(root)
    assert client.post("/jobs", json={"prepared_dir": str(prepared), "method": "darts"}).status_code == 422


def test_the_token_is_required_when_one_is_set_and_health_stays_open(env, monkeypatch):
    root, _ = env
    prepared = bundle(root)
    monkeypatch.setenv("WSP_TOKEN", "s3cret")
    body = {"prepared_dir": str(prepared), "method": "v1"}
    assert client.post("/jobs", json=body).status_code == 401
    assert client.post("/jobs", json=body, headers={"Authorization": "Bearer nope"}).status_code == 401
    assert client.get("/health").status_code == 200
    ok = client.post("/jobs", json=body, headers={"Authorization": "Bearer s3cret"})
    assert ok.status_code == 202
    wait_headers = {"Authorization": "Bearer s3cret"}
    deadline = time.time() + 20
    while time.time() < deadline and client.get(f"/jobs/{ok.json()['id']}", headers=wait_headers).json()["status"] \
            not in ("done", "error"):
        time.sleep(0.05)


def test_an_unknown_job_is_a_404(env):
    assert client.get("/jobs/nope").status_code == 404
