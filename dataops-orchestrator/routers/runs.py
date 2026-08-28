import json

from fastapi import APIRouter, HTTPException, Query, Response

import airflow_client as af
import datalake_client as dlc

router = APIRouter(prefix="/dags/{dag_id}/runs", tags=["DAG Runs"])


@router.get("")
async def list_runs(dag_id: str, limit: int = Query(10, ge=1, le=200), offset: int = Query(0, ge=0)):
    """List DAG runs for a given DAG, ordered by most recent first."""
    return await af.list_dag_runs(dag_id, limit=limit, offset=offset)


@router.get("/{run_id}")
async def get_run(dag_id: str, run_id: str):
    """Get the status and details of a specific DAG run."""
    return await af.get_dag_run(dag_id, run_id)


@router.get("/{run_id}/tasks")
async def list_task_instances(dag_id: str, run_id: str):
    """List all task instances for a DAG run."""
    return await af.list_task_instances(dag_id, run_id)


@router.get("/{run_id}/tasks/{task_id}")
async def get_task_instance(dag_id: str, run_id: str, task_id: str):
    """Get the status of a specific task instance."""
    return await af.get_task_instance(dag_id, run_id, task_id)


@router.get("/{run_id}/tasks/{task_id}/logs/{try_number}")
async def get_task_logs(dag_id: str, run_id: str, task_id: str, try_number: int = 1):
    """Retrieve the logs for a task instance attempt."""
    log_text = await af.get_task_logs(dag_id, run_id, task_id, try_number)
    return {"dag_id": dag_id, "run_id": run_id, "task_id": task_id,
            "try_number": try_number, "log": log_text}


# ── Run results ───────────────────────────────────────────────────────────────
#
# dali_dataspace_process_dataset uploads its artifacts to the Data Space bucket
# and returns {name: object key} from its upload_artifacts task. That XCom is
# the link between a run and its output: rather than guessing at key patterns,
# the run tells us what it produced. Everything below reads it.

_ARTIFACTS_TASK_ID = "upload_artifacts"

# The report is small and always wanted; the CSVs are not, and a remediated
# frame can be very large. Only a prefix of a CSV is ever served — enough for
# the results chart — with the true size reported so the UI can say so.
_MAX_REPORT_BYTES = 8 * 1024 * 1024
_DEFAULT_CSV_BYTES = 2 * 1024 * 1024
_MAX_CSV_BYTES = 16 * 1024 * 1024


async def _artifact_map(dag_id: str, run_id: str) -> dict:
    """{name: object key} for a run, or {} if it produced none (yet)."""
    try:
        value = await af.get_xcom(dag_id, run_id, _ARTIFACTS_TASK_ID)
    except HTTPException as e:
        # 404 means the task has not run or pushed nothing — not an error here.
        if e.status_code == 404:
            return {}
        raise
    return value if isinstance(value, dict) else {}


@router.get("/{run_id}/artifacts")
async def list_run_artifacts(dag_id: str, run_id: str):
    """The artifacts a run produced, with its report inlined.

    The bucket is the run's own `catalogue_id` conf value — the same bucket the
    distribution was read from — so nothing here has to know the deployment's
    storage layout.
    """
    run = await af.get_dag_run(dag_id, run_id)
    conf = run.get("conf") or {}
    catalogue_id = conf.get("catalogue_id")

    artifacts = await _artifact_map(dag_id, run_id)
    report = None
    if artifacts.get("report_json") and catalogue_id:
        body, _ = dlc.get_object(catalogue_id, artifacts["report_json"], max_bytes=_MAX_REPORT_BYTES)
        try:
            report = json.loads(body)
        except ValueError:
            # A truncated or malformed report should not take the whole view
            # down: the artifact list and download links still work without it.
            report = None

    return {
        "dag_id":       dag_id,
        "run_id":       run_id,
        "state":        run.get("state"),
        "catalogue_id": catalogue_id,
        "dataset_id":   conf.get("dataset_id"),
        "asset_id":     conf.get("asset_id"),
        "artifacts":    [{"name": n, "key": k} for n, k in sorted(artifacts.items())],
        "report":       report,
    }


@router.get("/{run_id}/artifacts/{name}")
async def get_run_artifact(
    dag_id: str,
    run_id: str,
    name: str,
    max_bytes: int = Query(_DEFAULT_CSV_BYTES, ge=1024, le=_MAX_CSV_BYTES),
):
    """Serve one artifact, truncated to `max_bytes`.

    Truncation is on a byte range, not a row count, so a large frame is never
    pulled out of storage in full. A CSV cut mid-line would give the browser a
    malformed final row, so the trailing partial line is dropped and the
    response says whether anything was left behind.
    """
    run = await af.get_dag_run(dag_id, run_id)
    catalogue_id = (run.get("conf") or {}).get("catalogue_id")
    if not catalogue_id:
        raise HTTPException(status_code=400, detail="Run has no catalogue_id in its conf")

    artifacts = await _artifact_map(dag_id, run_id)
    key = artifacts.get(name)
    if not key:
        raise HTTPException(status_code=404, detail=f"Run produced no artifact named '{name}'")

    body, total = dlc.get_object(catalogue_id, key, max_bytes=max_bytes)
    truncated = total > len(body)

    if key.endswith(".json"):
        media_type = "application/json"
    elif key.endswith(".csv"):
        media_type = "text/csv"
        if truncated:
            cut = body.rfind(b"\n")
            if cut != -1:
                body = body[:cut + 1]
    else:
        media_type = "application/octet-stream"

    return Response(
        content=body,
        media_type=media_type,
        headers={
            "X-Artifact-Key":       key,
            "X-Artifact-Total-Size": str(total),
            "X-Artifact-Truncated":  "true" if truncated else "false",
        },
    )
