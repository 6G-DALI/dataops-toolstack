from fastapi import APIRouter, Query
import airflow_client as af

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
