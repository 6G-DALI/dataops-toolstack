"""
Client for the WaveStitch+ imputation service (wavestitch-service/ in this repository).

WaveStitch+ is not imported or run by Airflow. Its runners need torch and pykeops and take minutes, so they live in a
container of their own next to Airflow in the DataOps stack. This module is the DAG's side of that: it hands the service
the regularized bundle the pipeline just wrote, waits for the job, and reports the result in the shape
dataops.imputation_runner.impute_bundle returns, so scoring, the final dataset and the uploads work on it unchanged.

The bundle crosses over a volume both containers mount, not HTTP: only its path is sent, and the filled splits are
read from `<bundle parent>/<base>_generated/` where the service wrote them. That is why `run_dataops_pipeline` puts its
scratch directory under DALI_WORK_ROOT when it is set, and why a bundle outside it is refused here with that reason.

DAG run input, in `imputation.runs`:

    {"lib": "wavestitchplus", "method": "v1"}                      # EM diffusion: trains, then imputes
    {"lib": "wavestitchplus", "method": "v2"}                      # v1's output anchored to an interpolation prior
    {"lib": "wavestitchplus", "method": "harpoon"}                 # inference on v1's checkpoint, with bound guidance
    {"lib": "wavestitchplus", "method": "v1", "device": "cpu", "params": {"fast": true}}

Options of each method are listed in wavestitch-service/app.py (RUNNERS). v2 and harpoon need v1 to have run on the
same bundle; dali.processing.imputation_runs orders the runs so it has, and the service runs it first when it has not.

Configuration (environment):
    WAVESTITCH_URL              the service, e.g. http://wavestitch:8000. Unset: a wavestitchplus run is recorded as
                                failed ("WAVESTITCH_URL is not set") and the other runs still happen.
    WAVESTITCH_TOKEN            sent as a bearer token when the service has WSP_TOKEN set
    WAVESTITCH_POLL_SECONDS     how often a job is asked about                                  (10)
    WAVESTITCH_TIMEOUT_SECONDS  how long one job is waited for                                  (14400, 4 h)
    DALI_WORK_ROOT              the shared volume, the same path in this container and the service's
"""

from __future__ import annotations

import json
import os
import time
import urllib.error
import urllib.request
from pathlib import Path
from typing import Any, Iterable

LIB = "wavestitchplus"
ENGINE = "wavestitch"
METHODS = ("v1", "v2", "harpoon")
# Names the runners and the project's own pipeline also use for these methods.
_ALIASES = {"full": "v1", "anchored": "v2"}
# v2 anchors v1's diffusion output and harpoon samples from v1's checkpoint.
NEEDS_V1 = ("v2", "harpoon")


class WaveStitchError(RuntimeError):
    """The service could not be reached, refused the job, or the job failed."""


def normalize_method(method: str) -> str:
    return _ALIASES.get(method, method)


def service_url() -> str:
    return os.environ.get("WAVESTITCH_URL", "").strip().rstrip("/")


def _request(verb: str, path: str, body: dict | None = None, timeout: float = 30) -> dict:
    url = service_url()
    if not url:
        raise WaveStitchError("WAVESTITCH_URL is not set: there is no WaveStitch+ service to run wavestitchplus on")
    headers = {"Accept": "application/json"}
    data = None
    if body is not None:
        data = json.dumps(body).encode("utf-8")
        headers["Content-Type"] = "application/json"
    token = os.environ.get("WAVESTITCH_TOKEN", "").strip()
    if token:
        headers["Authorization"] = f"Bearer {token}"
    request = urllib.request.Request(url + path, data=data, headers=headers, method=verb)
    try:
        with urllib.request.urlopen(request, timeout=timeout) as response:
            return json.loads(response.read().decode("utf-8"))
    except urllib.error.HTTPError as exc:
        detail = exc.read().decode("utf-8", errors="replace")
        try:
            detail = json.loads(detail).get("detail", detail)
        except (ValueError, AttributeError):
            pass
        raise WaveStitchError(f"the WaveStitch+ service answered {exc.code} to {verb} {path}: {detail}") from exc
    except (urllib.error.URLError, TimeoutError, OSError) as exc:
        raise WaveStitchError(f"could not reach the WaveStitch+ service at {url}: {exc}") from exc


def _check_shared(prepared_dir: str) -> None:
    root = os.environ.get("DALI_WORK_ROOT", "").strip()
    if not root:
        raise WaveStitchError(
            "DALI_WORK_ROOT is not set: the bundle is in a scratch directory the WaveStitch+ service cannot see. "
            "Set it to the volume Airflow and the service share")
    resolved = Path(prepared_dir).resolve()
    shared = Path(root).resolve()
    if shared != resolved and shared not in resolved.parents:
        raise WaveStitchError(f"the bundle {prepared_dir} is not under DALI_WORK_ROOT ({root})")


def _summarize(prepared_dir: str, method: str, inputs: Iterable[str], job: dict) -> dict:
    """What impute_bundle returns, read from the files the service wrote: per split the rows, the missing values
    before and after, and how many were filled."""
    import pandas as pd
    from dataops.imputation_runner import INPUT_FILES, _load_meta

    prepared = Path(prepared_dir)
    out_dir = Path(job.get("output_dir") or "")
    target_cols = _load_meta(prepared)["target_cols"]
    files: dict[str, dict] = {}
    for kind in inputs:
        src = prepared / INPUT_FILES[kind]
        out_path = out_dir / f"{LIB}_{method}_{kind}_imputed.csv"
        if not src.exists() or not out_path.exists():
            continue
        before = int(pd.read_csv(src)[target_cols].isna().sum().sum())
        out_df = pd.read_csv(out_path)
        after = int(out_df[target_cols].isna().sum().sum())
        files[kind] = {"path": str(out_path), "rows": int(len(out_df)), "nan_before": before,
                       "nan_after": after, "filled": before - after}
    if not files:
        raise WaveStitchError(f"the WaveStitch+ job finished but wrote no {LIB}_{method}_*_imputed.csv in {out_dir}")
    return {"method": method, "lib": LIB, "engine": ENGINE, "output_dir": str(out_dir),
            "target_cols": target_cols, "files": files}


def impute(
    prepared_dir: str,
    *,
    method: str,
    inputs: Iterable[str] = ("train", "test"),
    device: str | None = None,
    params: dict[str, Any] | None = None,
    v1_params: dict[str, Any] | None = None,
) -> dict:
    """Run one WaveStitch+ method over the bundle and return its summary, in the shape impute_bundle returns.

    Raises WaveStitchError when the service is not configured or not reachable, refuses the job, the job fails, or it
    does not finish within WAVESTITCH_TIMEOUT_SECONDS. (The service keeps going with a job nobody waits for any more.)
    """
    method = normalize_method(method)
    if method not in METHODS:
        raise WaveStitchError(f"wavestitchplus has no method {method!r}; choose from {list(METHODS)}")
    inputs = list(inputs)
    _check_shared(prepared_dir)

    job = _request("POST", "/jobs", {
        "prepared_dir": str(prepared_dir), "method": method, "device": device or "cpu", "inputs": inputs,
        "params": params or {}, "v1_params": v1_params or {}})
    job_id = job["id"]
    print(f"[dataops] wavestitchplus/{method}: job {job_id} submitted to {service_url()}")

    poll = float(os.environ.get("WAVESTITCH_POLL_SECONDS", "10"))
    deadline = time.monotonic() + float(os.environ.get("WAVESTITCH_TIMEOUT_SECONDS", str(4 * 3600)))
    seen = None
    while job["status"] in ("queued", "running"):
        if time.monotonic() > deadline:
            raise WaveStitchError(f"wavestitchplus/{method} job {job_id} did not finish within "
                                  f"{os.environ.get('WAVESTITCH_TIMEOUT_SECONDS', str(4 * 3600))} s")
        time.sleep(poll)
        job = _request("GET", f"/jobs/{job_id}")
        steps = ", ".join(f"{s['method']} {s['status']}" for s in job.get("steps", []))
        state = (job["status"], steps)
        if state != seen:
            seen = state
            print(f"[dataops] wavestitchplus/{method}: job {job_id} {job['status']}" + (f" ({steps})" if steps else ""))

    if job["status"] != "done":
        tail = "\n".join(job.get("log_tail") or [])
        raise WaveStitchError(f"{job.get('error') or 'the job failed'}" + (f"\n{tail}" if tail else ""))
    return _summarize(prepared_dir, method, inputs, job)
