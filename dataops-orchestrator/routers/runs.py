import json
import re

from fastapi import APIRouter, HTTPException, Query, Response

import airflow_client as af
import datalake_client as dlc
from config import DATAOPS_BUCKET

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
# dali_dataspace_validate_dataset uploads its artifacts to the Data Space bucket
# and returns {name: object key} from its upload_artifacts task. That XCom is
# the link between a run and its output: rather than guessing at key patterns,
# the run tells us what it produced. Everything below reads it.
#
# report_json carries the pipeline's report with the merged quality report —
# both GX suites plus pandera, totalled — nested under "dali_quality" (see
# dali.datalake.upload_artifacts), which is why inlining that one artifact is
# enough to render a run's whole verdict.

_ARTIFACTS_TASK_ID = "upload_artifacts"

# Runs write their outputs to the DataOps bucket under "runs/...". Runs made before that wrote
# them next to the dataset, in the catalogue's bucket of the Data Lake:
#
#     <dataset>/<asset>_<timestamp>_raw.csv   (also _soft_cleaned.csv, _remediated.csv,
#                                              _report.json, _imputed_*.csv)
#
# Those runs still hold the old keys in their XCom. The files have been copied into the DataOps
# bucket as runs/<catalogue>/<dataset>/<asset>/legacy-<timestamp>/<name>, so such a key is looked
# up there first, and in the catalogue bucket if it has not been copied.
_DATAOPS_KEY_PREFIX = "runs/"
_LEGACY_KEY = re.compile(
    r"^(?P<dataset>[^/]+)/(?P<asset>[^/]+?)_(?P<ts>\d{8}T\d{6}Z)"
    r"(?P<suffix>_raw\.csv|_soft_cleaned\.csv|_remediated\.csv|_report\.json|_imputed(?:_[A-Za-z0-9.-]+?)*\.csv)$"
)


def _legacy_dataops_key(key: str, catalogue_id: str | None) -> str | None:
    """Where a pre-migration key's file lives in the DataOps bucket, or None if it is not one."""
    m = _LEGACY_KEY.match(key)
    if not m or not catalogue_id:
        return None
    return (f"{_DATAOPS_KEY_PREFIX}{catalogue_id}/{m['dataset']}/{m['asset']}/"
            f"legacy-{m['ts']}/{m['suffix'].lstrip('_')}")


def _locations(key: str, catalogue_id: str | None) -> list[tuple[str, str, str]]:
    """Every (bucket, key, store) a run artifact may be read from, most likely first."""
    if key.startswith(_DATAOPS_KEY_PREFIX):
        return [(DATAOPS_BUCKET, key, "dataops")]
    found = []
    migrated = _legacy_dataops_key(key, catalogue_id)
    if migrated:
        found.append((DATAOPS_BUCKET, migrated, "dataops"))
    if catalogue_id:
        found.append((catalogue_id, key, "dataspace"))
    return found


def _read_artifact(catalogue_id: str | None, key: str, **window) -> tuple[bytes, int]:
    """dlc.get_object over _locations: the first place that has the object. A missing object
    (404) moves on to the next place; any other failure is raised."""
    locations = _locations(key, catalogue_id)
    if not locations:
        raise HTTPException(status_code=400, detail="Run has no catalogue_id in its conf")
    last = None
    for bucket, location_key, store in locations:
        try:
            return dlc.get_object(bucket, location_key, store=store, **window)
        except HTTPException as exc:
            if exc.status_code != 404:
                raise
            last = exc
    raise last

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
    report_key = artifacts.get("report_json")
    if report_key and (catalogue_id or report_key.startswith(_DATAOPS_KEY_PREFIX)):
        body, _ = _read_artifact(catalogue_id, report_key, max_bytes=_MAX_REPORT_BYTES)
        try:
            # The pipeline writes report.json with Python's json, which emits NaN/Infinity
            # for missing cells. They are not valid JSON and FastAPI refuses to serialise
            # them (a 500 on this whole endpoint), so read them as null.
            report = json.loads(body, parse_constant=lambda _constant: None)
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
    offset: int = Query(0, ge=0),
):
    """Serve a window of one artifact: `max_bytes` of it, starting at `offset`.

    The window is a byte range, not a row count, so a large frame is never
    pulled out of storage in full. A CSV cut mid-line would give the browser a
    malformed row, so both partial lines are trimmed — the trailing one when the
    window stops short of the end, and the leading one whenever `offset` lands
    mid-line, which it does for every window after the first.

    Because that trimming happens here, `X-Artifact-Next-Offset` is always a row
    boundary: a caller that walks the object by feeding it back in gets whole
    rows every time and never stitches a split line together itself. Only the
    first window carries the CSV header — remembering it is the caller's job.
    """
    run = await af.get_dag_run(dag_id, run_id)
    catalogue_id = (run.get("conf") or {}).get("catalogue_id")

    artifacts = await _artifact_map(dag_id, run_id)
    key = artifacts.get(name)
    if not key:
        raise HTTPException(status_code=404, detail=f"Run produced no artifact named '{name}'")

    if not _locations(key, catalogue_id):
        raise HTTPException(status_code=400, detail="Run has no catalogue_id in its conf")

    # One byte before the window, when there is one. That single byte is what
    # lets this tell a row boundary from a mid-row offset: without it, trimming
    # the leading line would eat a good row whenever the caller passed back the
    # boundary this endpoint had just handed them.
    probe = 1 if offset and key.endswith(".csv") else 0
    body, total = _read_artifact(
        catalogue_id, key, max_bytes=max_bytes + probe, offset=offset - probe,
    )
    reached_end = (offset - probe) + len(body) >= total

    if key.endswith(".json"):
        media_type = "application/json"
    elif key.endswith(".csv"):
        media_type = "text/csv"
        if probe:
            if body[:1] == b"\n":
                body = body[1:]           # the offset was already a boundary
            else:
                # It opened mid-row; that row belongs to the previous window.
                nl = body.find(b"\n")
                if nl == -1:
                    # A whole window with no row boundary in it — nothing usable
                    # here, but the offset must still advance or a caller
                    # walking the object would ask for these bytes forever.
                    offset += len(body) - probe
                    body = b""
                else:
                    body = body[nl + 1:]
                    offset += nl + 1 - probe
        if not reached_end:
            cut = body.rfind(b"\n")
            body = body[:cut + 1] if cut != -1 else b""
    else:
        media_type = "application/octet-stream"

    next_offset = offset + len(body)
    truncated = next_offset < total

    return Response(
        content=body,
        media_type=media_type,
        headers={
            "X-Artifact-Key":         key,
            "X-Artifact-Total-Size":  str(total),
            "X-Artifact-Truncated":   "true" if truncated else "false",
            "X-Artifact-Offset":      str(offset),
            # Where the next window starts. Always a line boundary for a CSV,
            # because both partial lines are trimmed above — so a caller
            # walking the object never has to stitch a split row together.
            "X-Artifact-Next-Offset": str(next_offset),
        },
    )
