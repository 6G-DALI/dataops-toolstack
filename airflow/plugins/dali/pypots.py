"""
Client for the PyPOTS imputation service (pypots-service/ in this repository).

PyPOTS is not imported or run by Airflow. Its models need torch and train for minutes, so they live in a container of
their own next to Airflow in the DataOps stack. This module is the DAG's side of that: it hands the service the
regularized bundle the pipeline just wrote, waits for the job, and reports the result in the shape
dataops.imputation_runner.impute_bundle returns, so scoring, the final dataset and the uploads work on it unchanged.

The bundle crosses over the volume both containers mount (DALI_WORK_ROOT), exactly as for WaveStitch+: only its path is
sent, and the filled splits are read from `<bundle parent>/<base>_generated/pypots_<method>_<split>_imputed.csv`.

DAG run input, in `imputation.runs`:

    {"lib": "pypots", "method": "saits"}
    {"lib": "pypots", "method": "timesnet", "params": {"epochs": 50, "window": 100}}
    {"lib": "pypots", "method": "transformer", "params": {"epochs": 30, "batch_size": 64}}

Methods: saits, transformer, timesnet, brits, mrnn, gpvae, csdi, usgan. `params` takes epochs (200 by default in the
runner), batch_size (32), window (100: the series is tiled into windows of this many steps) and stride (0: no overlap).

Configuration (environment):
    PYPOTS_URL              the service, e.g. http://pypots:8000. Unset: a pypots run is recorded as failed
                            ("PYPOTS_URL is not set") and the other runs still happen.
    PYPOTS_TOKEN            sent as a bearer token when the service has PYPOTS_TOKEN set
    PYPOTS_POLL_SECONDS     how often a job is asked about                                   (10)
    PYPOTS_TIMEOUT_SECONDS  how long one job is waited for                                   (14400, 4 h)
    DALI_WORK_ROOT          the shared volume, the same path in this container and the service's
"""

from __future__ import annotations

import json
import os
import time
import urllib.error
import urllib.request
from pathlib import Path
from typing import Any, Iterable

LIB = "pypots"
ENGINE = "pypots"
METHODS = ("saits", "transformer", "timesnet", "brits", "mrnn", "gpvae", "csdi", "usgan")


class PypotsError(RuntimeError):
    """The service could not be reached, refused the job, or the job failed."""


def service_url() -> str:
    return os.environ.get("PYPOTS_URL", "").strip().rstrip("/")


def _request(verb: str, path: str, body: dict | None = None, timeout: float = 30) -> dict:
    url = service_url()
    if not url:
        raise PypotsError("PYPOTS_URL is not set: there is no PyPOTS service to run pypots on")
    headers = {"Accept": "application/json"}
    data = None
    if body is not None:
        data = json.dumps(body).encode("utf-8")
        headers["Content-Type"] = "application/json"
    token = os.environ.get("PYPOTS_TOKEN", "").strip()
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
        raise PypotsError(f"the PyPOTS service answered {exc.code} to {verb} {path}: {detail}") from exc
    except (urllib.error.URLError, TimeoutError, OSError) as exc:
        raise PypotsError(f"could not reach the PyPOTS service at {url}: {exc}") from exc


def _check_shared(prepared_dir: str) -> None:
    root = os.environ.get("DALI_WORK_ROOT", "").strip()
    if not root:
        raise PypotsError(
            "DALI_WORK_ROOT is not set: the bundle is in a scratch directory the PyPOTS service cannot see. "
            "Set it to the volume Airflow and the service share")
    resolved = Path(prepared_dir).resolve()
    shared = Path(root).resolve()
    if shared != resolved and shared not in resolved.parents:
        raise PypotsError(f"the bundle {prepared_dir} is not under DALI_WORK_ROOT ({root})")


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
        raise PypotsError(f"the PyPOTS job finished but wrote no {LIB}_{method}_*_imputed.csv in {out_dir}")
    return {"method": method, "lib": LIB, "engine": ENGINE, "output_dir": str(out_dir),
            "target_cols": target_cols, "files": files}


def impute(
    prepared_dir: str,
    *,
    method: str,
    inputs: Iterable[str] = ("train", "test"),
    params: dict[str, Any] | None = None,
) -> dict:
    """Run one PyPOTS method over the bundle and return its summary, in the shape impute_bundle returns.

    Raises PypotsError when the service is not configured or not reachable, refuses the job, the job fails, or it does
    not finish within PYPOTS_TIMEOUT_SECONDS. (The service keeps going with a job nobody waits for any more.)
    """
    if method not in METHODS:
        raise PypotsError(f"pypots has no method {method!r}; choose from {list(METHODS)}")
    inputs = list(inputs)
    _check_shared(prepared_dir)

    job = _request("POST", "/jobs", {"prepared_dir": str(prepared_dir), "method": method, "inputs": inputs,
                                     "params": params or {}})
    job_id = job["id"]
    print(f"[dataops] pypots/{method}: job {job_id} submitted to {service_url()}")

    poll = float(os.environ.get("PYPOTS_POLL_SECONDS", "10"))
    limit = os.environ.get("PYPOTS_TIMEOUT_SECONDS", str(4 * 3600))
    deadline = time.monotonic() + float(limit)
    seen = None
    while job["status"] in ("queued", "running"):
        if time.monotonic() > deadline:
            raise PypotsError(f"pypots/{method} job {job_id} did not finish within {limit} s")
        time.sleep(poll)
        job = _request("GET", f"/jobs/{job_id}")
        if job["status"] != seen:
            seen = job["status"]
            print(f"[dataops] pypots/{method}: job {job_id} {seen}")

    if job["status"] != "done":
        tail = "\n".join(job.get("log_tail") or [])
        raise PypotsError(f"{job.get('error') or 'the job failed'}" + (f"\n{tail}" if tail else ""))
    return _summarize(prepared_dir, method, inputs, job)
