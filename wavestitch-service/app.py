"""
WaveStitch+ imputation service: the sidecar the Airflow DAG calls over HTTP.

Runs next to Airflow in the DataOps stack, inside the WaveStitch+ image (torch, pykeops and the app runners live
here, not in the Airflow image). The DAG hands it a regularized bundle (`<name>_regularized/`) that both containers
see on one shared volume, and reads the results back from the same volume. Only paths and small JSON cross HTTP.

    POST /jobs        {"prepared_dir", "method": "v1"|"v2"|"harpoon", "device", "inputs", "params", "v1_params"}
    GET  /jobs/{id}   status, steps and the end of the log
    GET  /jobs        the most recent jobs
    GET  /health

A job runs the method's runner from dockers/tools/WaveStitchPlus_app on the bundle, exactly as the project's own
dataops_imputation_completes pipeline does, with the outputs in `<bundle parent>/<base>_generated/` as
`wavestitchplus_<v1|v2|harpoon>_<train|test>_imputed.csv`. Jobs run one at a time: a run uses every core it can get.

v2 anchors v1's diffusion output and harpoon samples from the checkpoint v1 trained, so both need v1 to have run on
the same bundle. When it has not, the job runs v1 first (with `v1_params`) and then the requested method.

Configuration (environment):
    WSP_WORK_ROOT              the shared volume; every prepared_dir must be inside it   (/opt/airflow/wsp-work)
    WSP_APP_DIR                the runners                                              (/app/WaveStitchPlus_app)
    WSP_PYTHON                 interpreter that runs them                               (this one)
    WSP_STEP_TIMEOUT_SECONDS   longest one runner may take                              (21600, 6 h)
    WSP_TOKEN                  when set, every endpoint but /health needs `Authorization: Bearer <token>`
"""

from __future__ import annotations

import os
import queue
import subprocess
import sys
import threading
import time
import uuid
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Literal

from fastapi import Depends, FastAPI, Header, HTTPException
from pydantic import BaseModel, Field

app = FastAPI(title="WaveStitch+ imputation service")

# ── configuration ────────────────────────────────────────────────────────────────────────────────────────────────


def _work_root() -> Path:
    return Path(os.environ.get("WSP_WORK_ROOT", "/opt/airflow/wsp-work")).resolve()


def _app_dir() -> Path:
    return Path(os.environ.get("WSP_APP_DIR", "/app/WaveStitchPlus_app"))


def _python() -> str:
    return os.environ.get("WSP_PYTHON") or sys.executable


def _step_timeout() -> float:
    return float(os.environ.get("WSP_STEP_TIMEOUT_SECONDS", str(6 * 3600)))


# ── what each method runs ────────────────────────────────────────────────────────────────────────────────────────

# The kind of each option: int, float, flag (present or absent), str, or a tuple of the choices a string may take.
_SYNTHESIS: dict[str, Any] = {"ddim_steps": "int", "repaint_rounds": "int", "guidance_scale": "float"}

RUNNERS: dict[str, dict[str, Any]] = {
    # method v1: the EM diffusion pipeline. Trains, then synthesizes.
    "v1": {
        "script": "run_imputation.py",
        "fixed": ["--method", "v1"],
        "params": {**_SYNTHESIS, "fast": "flag", "em_iterations": "int", "epochs_per_em": "int", "n_trials": "int"},
    },
    # method v2 (the runner calls it "anchored"): v1's diffusion output anchored to an interpolation prior.
    "v2": {
        "script": "run_imputation_v2.py",
        "fixed": ["--method", "anchored"],
        "params": {**_SYNTHESIS, "prior": ("linear", "nearest", "auto"), "tau": "float", "hard_prior": "int",
                   "no_monotone": "flag"},
    },
    # HARPOON: inference with manifold-bound guidance on the checkpoint v1 trained.
    "harpoon": {
        "script": "run_imputation_harpoon.py",
        "fixed": ["--method", "harpoon"],
        "params": {**_SYNTHESIS, "model_tag": "str", "bound_lambda": "float", "bound_power": "float",
                   "project_bounds": ("True", "False", "true", "false"), "prior_lambda": "float",
                   "prior_method": ("nearest", "linear"), "smooth_lambda": "float", "monotone_lambda": "float",
                   "pos_eps": "float", "auto_ub_q": "float", "auto_ub_pad": "float",
                   "hard_project_positive": "flag"},
    },
}

V1_TEST_OUTPUT = "wavestitchplus_v1_test_imputed.csv"


def generated_dirname(prepared: Path) -> str:
    """`<base>_generated`, as dataops.imputation_runner names it, so every engine writes to the same place."""
    base = prepared.name.removesuffix("_regularized").removesuffix("_prepared").removeprefix("prepared_")
    return f"{base}_generated"


def cli_flags(method: str, params: dict[str, Any]) -> list[str]:
    """`--flag value` pairs for the runner's own options. Only options the runner has are accepted; anything else is
    a ValueError, so a typo in a DAG run fails visibly instead of being ignored."""
    spec = RUNNERS[method]["params"]
    flags: list[str] = []
    for name, value in params.items():
        if name not in spec:
            raise ValueError(f"{method} has no option {name!r}; it takes {sorted(spec)}")
        kind = spec[name]
        flag = "--" + name.replace("_", "-")
        if kind == "flag":
            if not isinstance(value, bool):
                raise ValueError(f"{name} must be true or false")
            if value:
                flags.append(flag)
        elif kind == "int":
            if isinstance(value, bool) or not isinstance(value, int):
                raise ValueError(f"{name} must be an integer")
            flags += [flag, str(value)]
        elif kind == "float":
            if isinstance(value, bool) or not isinstance(value, (int, float)):
                raise ValueError(f"{name} must be a number")
            flags += [flag, str(value)]
        elif kind == "str":
            if not isinstance(value, str) or not value or value.startswith("-"):
                raise ValueError(f"{name} must be a plain string")
            flags += [flag, value]
        else:  # a tuple of choices
            if str(value) not in kind:
                raise ValueError(f"{name} must be one of {list(kind)}")
            flags += [flag, str(value)]
    return flags


# ── jobs ─────────────────────────────────────────────────────────────────────────────────────────────────────────


@dataclass
class Job:
    id: str
    prepared_dir: str
    method: str
    device: str
    inputs: list[str]
    params: dict[str, Any]
    v1_params: dict[str, Any]
    status: str = "queued"          # queued | running | done | error
    error: str | None = None
    created: float = field(default_factory=time.time)
    started: float | None = None
    finished: float | None = None
    steps: list[dict[str, Any]] = field(default_factory=list)
    output_dir: str | None = None
    log_path: str | None = None

    def public(self, tail: int = 0) -> dict[str, Any]:
        out = {k: getattr(self, k) for k in (
            "id", "status", "error", "method", "device", "inputs", "prepared_dir", "output_dir", "steps",
            "created", "started", "finished")}
        if tail and self.log_path and Path(self.log_path).exists():
            lines = Path(self.log_path).read_text(encoding="utf-8", errors="replace").splitlines()
            out["log_tail"] = lines[-tail:]
        return out


_MAX_JOBS = 200
_jobs: dict[str, Job] = {}
_queue: "queue.Queue[str]" = queue.Queue()
_lock = threading.Lock()
_worker: threading.Thread | None = None


def _plan(job: Job, out_dir: Path) -> list[str]:
    """The runners to run, in order: v1 first when v2 or harpoon need it and it has not run on this bundle."""
    if job.method != "v1" and not (out_dir / V1_TEST_OUTPUT).exists():
        return ["v1", job.method]
    return [job.method]


def _run_step(job: Job, method: str, prepared: Path, out_dir: Path, log) -> None:
    params = job.v1_params if (method == "v1" and job.method != "v1") else job.params
    runner = RUNNERS[method]
    cmd = [_python(), str(_app_dir() / runner["script"]),
           "--prepared-dir", str(prepared), "--output-dir", str(out_dir), *runner["fixed"],
           "--inputs", *job.inputs, "--device", job.device]
    if method == "v2" and (out_dir / V1_TEST_OUTPUT).exists():
        # Anchoring is pure Python on v1's output: no torch, no re-synthesis.
        cmd += ["--reuse-diffusion", str(out_dir / V1_TEST_OUTPUT)]
    cmd += cli_flags(method, params)

    step = {"method": method, "status": "running", "started": time.time(), "finished": None, "exit_code": None}
    job.steps.append(step)
    log.write(f"\n$ {' '.join(cmd)}\n")
    log.flush()
    try:
        proc = subprocess.run(cmd, stdout=log, stderr=subprocess.STDOUT, timeout=_step_timeout(), cwd=str(_app_dir()))
        step["exit_code"] = proc.returncode
        step["status"] = "done" if proc.returncode == 0 else "error"
    except subprocess.TimeoutExpired:
        step["status"] = "error"
        step["exit_code"] = 124
        log.write(f"\ntimed out after {_step_timeout()} s\n")
    step["finished"] = time.time()
    if step["status"] != "done":
        raise RuntimeError(f"{method} failed (exit {step['exit_code']}); see the job log")


def _execute(job: Job) -> None:
    job.status = "running"
    job.started = time.time()
    prepared = Path(job.prepared_dir)
    out_dir = prepared.parent / generated_dirname(prepared)
    out_dir.mkdir(parents=True, exist_ok=True)
    job.output_dir = str(out_dir)
    log_path = out_dir / f"wavestitch_job_{job.id}.log"
    job.log_path = str(log_path)
    try:
        with log_path.open("w", encoding="utf-8") as log:
            for method in _plan(job, out_dir):
                _run_step(job, method, prepared, out_dir, log)
        job.status = "done"
    except Exception as exc:  # noqa: BLE001 - the error is the job's result
        job.status = "error"
        job.error = f"{exc.__class__.__name__}: {exc}"
    finally:
        job.finished = time.time()


def _work() -> None:
    while True:
        job_id = _queue.get()
        job = _jobs.get(job_id)
        if job is not None:
            _execute(job)
        _queue.task_done()


def _enqueue(job: Job) -> None:
    global _worker
    with _lock:
        _jobs[job.id] = job
        while len(_jobs) > _MAX_JOBS:  # forget the oldest finished jobs
            oldest = next((j for j in _jobs.values() if j.status in ("done", "error")), None)
            if oldest is None:
                break
            del _jobs[oldest.id]
        if _worker is None or not _worker.is_alive():
            _worker = threading.Thread(target=_work, name="wavestitch-worker", daemon=True)
            _worker.start()
    _queue.put(job.id)


# ── HTTP ─────────────────────────────────────────────────────────────────────────────────────────────────────────


def require_token(authorization: str | None = Header(default=None)) -> None:
    token = os.environ.get("WSP_TOKEN", "")
    if token and authorization != f"Bearer {token}":
        raise HTTPException(status_code=401, detail="missing or wrong bearer token")


class JobRequest(BaseModel):
    prepared_dir: str = Field(description="The regularized bundle, inside WSP_WORK_ROOT.")
    method: Literal["v1", "v2", "harpoon"]
    device: Literal["auto", "cpu", "gpu"] = "cpu"
    inputs: list[Literal["train", "test"]] = Field(default_factory=lambda: ["train", "test"], min_length=1)
    params: dict[str, Any] = Field(default_factory=dict, description="Options of the method's runner.")
    v1_params: dict[str, Any] = Field(default_factory=dict,
                                      description="Options for the v1 run that v2/harpoon start when v1 has not run.")


@app.get("/health")
def health() -> dict[str, Any]:
    running = next((j.id for j in _jobs.values() if j.status == "running"), None)
    return {"status": "ok", "queued": _queue.qsize(), "running": running, "work_root": str(_work_root())}


@app.post("/jobs", status_code=202, dependencies=[Depends(require_token)])
def submit(request: JobRequest) -> dict[str, Any]:
    prepared = Path(request.prepared_dir).resolve()
    root = _work_root()
    if root != prepared and root not in prepared.parents:
        raise HTTPException(status_code=422, detail=f"prepared_dir must be inside {root}")
    if not (prepared / "meta.json").is_file():
        raise HTTPException(status_code=422, detail=f"{prepared} is not a prepared bundle (no meta.json)")
    try:
        cli_flags(request.method, request.params)
        if request.method != "v1":
            cli_flags("v1", request.v1_params)
    except ValueError as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from exc
    job = Job(id=uuid.uuid4().hex[:12], prepared_dir=str(prepared), method=request.method, device=request.device,
              inputs=list(request.inputs), params=request.params, v1_params=request.v1_params)
    _enqueue(job)
    return job.public()


@app.get("/jobs/{job_id}", dependencies=[Depends(require_token)])
def job_status(job_id: str) -> dict[str, Any]:
    job = _jobs.get(job_id)
    if job is None:
        raise HTTPException(status_code=404, detail="no such job (the service keeps recent jobs in memory only)")
    return job.public(tail=40)


@app.get("/jobs", dependencies=[Depends(require_token)])
def jobs() -> list[dict[str, Any]]:
    return [j.public() for j in sorted(_jobs.values(), key=lambda j: j.created, reverse=True)[:50]]
