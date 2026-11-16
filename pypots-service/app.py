"""
PyPOTS imputation service: the sidecar the Airflow DAG calls over HTTP for the PyPOTS neural methods.

Runs next to Airflow in the DataOps stack, inside an image with PyPOTS and CPU torch (neither is in the Airflow image).
The DAG hands it a regularized bundle (`<name>_regularized/`) that both containers see on one shared volume, and reads
the results back from the same volume. Only paths and small JSON cross HTTP.

    POST /jobs        {"prepared_dir", "method": "saits"|"transformer"|"timesnet"|..., "inputs", "params"}
    GET  /jobs/{id}   status and the end of the log
    GET  /jobs        the most recent jobs
    GET  /health

A job runs dockers/tools/PyPOTS_app/run_imputation.py of the WaveStitchPlus repository on the bundle: it fits the model
on train.csv (standardized with the train statistics), imputes train and test in windows, and writes
`<bundle parent>/<base>_generated/pypots_<method>_<train|test>_imputed.csv`. Jobs run one at a time: training uses every
core it can get.

Configuration (environment):
    PYPOTS_WORK_ROOT              the shared volume; every prepared_dir must be inside it   (/opt/airflow/wsp-work)
    PYPOTS_APP_DIR                the runner                                               (/app/PyPOTS_app)
    PYPOTS_PYTHON                 interpreter that runs it                                  (this one)
    PYPOTS_STEP_TIMEOUT_SECONDS   longest one runner may take                               (21600, 6 h)
    PYPOTS_TOKEN                  when set, every endpoint but /health needs `Authorization: Bearer <token>`
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

app = FastAPI(title="PyPOTS imputation service")

METHODS = ("saits", "brits", "transformer", "gpvae", "mrnn", "csdi", "usgan", "timesnet")
# The runner's own options, all whole numbers. Anything else is refused, so a typo fails visibly.
PARAMS = ("epochs", "batch_size", "window", "stride")
_ABOVE_ZERO = ("epochs", "batch_size")   # window and stride may be 0 (the whole series; no overlap)


def _work_root() -> Path:
    return Path(os.environ.get("PYPOTS_WORK_ROOT", "/opt/airflow/wsp-work")).resolve()


def _app_dir() -> Path:
    return Path(os.environ.get("PYPOTS_APP_DIR", "/app/PyPOTS_app"))


def _python() -> str:
    return os.environ.get("PYPOTS_PYTHON") or sys.executable


def _step_timeout() -> float:
    return float(os.environ.get("PYPOTS_STEP_TIMEOUT_SECONDS", str(6 * 3600)))


def generated_dirname(prepared: Path) -> str:
    """`<base>_generated`, as dataops.imputation_runner names it, so every engine writes to the same place."""
    base = prepared.name.removesuffix("_regularized").removesuffix("_prepared").removeprefix("prepared_")
    return f"{base}_generated"


def cli_flags(params: dict[str, Any]) -> list[str]:
    """`--flag value` pairs for the runner's options."""
    flags: list[str] = []
    for name, value in params.items():
        if name not in PARAMS:
            raise ValueError(f"pypots has no option {name!r}; it takes {list(PARAMS)}")
        if isinstance(value, bool) or not isinstance(value, int) or value < 0 or (value == 0 and name in _ABOVE_ZERO):
            raise ValueError(f"{name} must be a whole number" + (" above 0" if name in _ABOVE_ZERO else ""))
        flags += ["--" + name.replace("_", "-"), str(value)]
    return flags


@dataclass
class Job:
    id: str
    prepared_dir: str
    method: str
    inputs: list[str]
    params: dict[str, Any]
    status: str = "queued"          # queued | running | done | error
    error: str | None = None
    created: float = field(default_factory=time.time)
    started: float | None = None
    finished: float | None = None
    output_dir: str | None = None
    log_path: str | None = None

    def public(self, tail: int = 0) -> dict[str, Any]:
        out = {k: getattr(self, k) for k in (
            "id", "status", "error", "method", "inputs", "prepared_dir", "output_dir", "created", "started", "finished")}
        if tail and self.log_path and Path(self.log_path).exists():
            lines = Path(self.log_path).read_text(encoding="utf-8", errors="replace").splitlines()
            out["log_tail"] = lines[-tail:]
        return out


_MAX_JOBS = 200
_jobs: dict[str, Job] = {}
_queue: "queue.Queue[str]" = queue.Queue()
_lock = threading.Lock()
_worker: threading.Thread | None = None


def _execute(job: Job) -> None:
    job.status = "running"
    job.started = time.time()
    prepared = Path(job.prepared_dir)
    out_dir = prepared.parent / generated_dirname(prepared)
    out_dir.mkdir(parents=True, exist_ok=True)
    job.output_dir = str(out_dir)
    log_path = out_dir / f"pypots_job_{job.id}.log"
    job.log_path = str(log_path)
    cmd = [_python(), str(_app_dir() / "run_imputation.py"), "--prepared-dir", str(prepared),
           "--output-dir", str(out_dir), "--method", job.method, "--inputs", *job.inputs, *cli_flags(job.params)]
    try:
        with log_path.open("w", encoding="utf-8") as log:
            log.write(f"$ {' '.join(cmd)}\n")
            log.flush()
            try:
                code = subprocess.run(cmd, stdout=log, stderr=subprocess.STDOUT, timeout=_step_timeout(),
                                      cwd=str(_app_dir())).returncode
            except subprocess.TimeoutExpired:
                code = 124
                log.write(f"\ntimed out after {_step_timeout()} s\n")
        if code != 0:
            raise RuntimeError(f"{job.method} failed (exit {code}); see the job log")
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
            _worker = threading.Thread(target=_work, name="pypots-worker", daemon=True)
            _worker.start()
    _queue.put(job.id)


def require_token(authorization: str | None = Header(default=None)) -> None:
    token = os.environ.get("PYPOTS_TOKEN", "")
    if token and authorization != f"Bearer {token}":
        raise HTTPException(status_code=401, detail="missing or wrong bearer token")


class JobRequest(BaseModel):
    prepared_dir: str = Field(description="The regularized bundle, inside PYPOTS_WORK_ROOT.")
    method: Literal["saits", "brits", "transformer", "gpvae", "mrnn", "csdi", "usgan", "timesnet"]
    inputs: list[Literal["train", "test"]] = Field(default_factory=lambda: ["train", "test"], min_length=1)
    params: dict[str, Any] = Field(default_factory=dict, description="epochs, batch_size, window, stride.")


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
        cli_flags(request.params)
    except ValueError as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from exc
    job = Job(id=uuid.uuid4().hex[:12], prepared_dir=str(prepared), method=request.method,
              inputs=list(request.inputs), params=request.params)
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
