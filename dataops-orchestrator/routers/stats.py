from fastapi import APIRouter
import airflow_client as af

router = APIRouter(prefix="/stats", tags=["Stats"])


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
