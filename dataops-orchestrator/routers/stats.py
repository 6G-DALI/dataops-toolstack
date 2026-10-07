import asyncio
import logging
import os
import time

from fastapi import APIRouter

import airflow_client as af
import piveau_client

log = logging.getLogger(__name__)

router = APIRouter(prefix="/stats", tags=["Stats"])

# The landing page of the portal is public and calls /stats/portal on every visit, so the
# answer is kept for a short time instead of querying piveau and Airflow each time.
_PORTAL_STATS_TTL = float(os.getenv("PORTAL_STATS_CACHE_SECONDS", "60"))
_portal_stats_cache: dict = {"at": None, "value": None}
_portal_stats_lock = asyncio.Lock()


async def _count_pipelines() -> int:
    data = await af.list_dags(limit=500)
    dags = data.get("dags", [])
    return int(data.get("total_entries", len(dags)))


@router.get("/portal")
async def get_portal_stats():
    """The headline counts for the portal's landing page.

    {"datasets", "catalogues", "pipelines", "models"}. A figure whose source cannot be reached is
    null rather than 0 or a guess, so the page can leave it out. `models` is always null for now:
    nothing the orchestrator talks to holds MLOps models.

    Public and read-only (the landing page is shown before sign-in), and cached for
    PORTAL_STATS_CACHE_SECONDS (default 60).
    """
    async with _portal_stats_lock:
        now = time.monotonic()
        at = _portal_stats_cache["at"]
        if at is not None and now - at < _PORTAL_STATS_TTL:
            return _portal_stats_cache["value"]

        datasets, catalogues, pipelines = await asyncio.gather(
            piveau_client.count_indexed("dataset"),
            piveau_client.count_indexed("catalogue"),
            _count_pipelines(),
            return_exceptions=True,
        )

        def figure(name: str, result):
            if isinstance(result, BaseException):
                log.warning("portal stats: %s unavailable: %s", name, result)
                return None
            return result

        value = {
            "datasets": figure("datasets", datasets),
            "catalogues": figure("catalogues", catalogues),
            "pipelines": figure("pipelines", pipelines),
            "models": None,
        }
        _portal_stats_cache.update(at=now, value=value)
        return value


@router.get("")
async def get_stats():
    """Aggregate deployment statistics for the dashboard."""
    dags_data = await af.list_dags(limit=500)
    dags = dags_data.get("dags", [])

    custom_tasks = await af.list_custom_tasks()

    # Fetch recent runs across DAGs (up to first 20 DAGs, 5 runs each)
    recent_runs = []
    for dag in dags[:20]:
        try:
            runs_data = await af.list_dag_runs(dag["dag_id"], limit=5)
            for run in runs_data.get("dag_runs", []):
                recent_runs.append({**run, "dag_id": dag["dag_id"]})
        except Exception:
            pass

    recent_runs.sort(key=lambda r: r.get("execution_date", ""), reverse=True)

    run_states = {}
    for run in recent_runs:
        state = run.get("state", "unknown")
        run_states[state] = run_states.get(state, 0) + 1

    return {
        "dags": {
            "total": len(dags),
            "active": sum(1 for d in dags if not d.get("is_paused")),
            "paused": sum(1 for d in dags if d.get("is_paused")),
        },
        "tasks": {
            "custom": custom_tasks.get("total_entries", 0),
        },
        "run_states": run_states,
        "recent_runs": recent_runs[:10],
    }
