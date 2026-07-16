from fastapi import APIRouter, Body, Query
from typing import Any
from pydantic import BaseModel
import airflow_client as af

router = APIRouter(prefix="/dags", tags=["DAGs"])


class DagCreateRequest(BaseModel):
    dag_id: str
    description: str = ""
    schedule: str = "@daily"
    task_ids: list[str]
    owner: str = "airflow"


@router.get("")
async def list_dags(limit: int = Query(100, ge=1, le=500)):
    """List all available DAGs."""
    return await af.list_dags(limit=limit)


@router.get("/{dag_id}")
async def get_dag(dag_id: str):
    """Get details of a specific DAG."""
    return await af.get_dag(dag_id)


@router.get("/{dag_id}/details")
async def get_dag_details(dag_id: str):
    """Get full DAG details including params."""
    return await af.get_dag_details(dag_id)


@router.patch("/{dag_id}/pause")
async def pause_dag(dag_id: str, is_paused: bool = Query(...)):
    """Pause or unpause a DAG."""
    return await af.set_dag_paused(dag_id, is_paused)


@router.post("/{dag_id}/trigger")
async def trigger_dag(dag_id: str, conf: dict[str, Any] = Body(default_factory=dict)):
    """
    Trigger a new DAG run. `conf` is passed straight through as dag_run.conf.

    Different DAGs declare different params (e.g. dataset_id/distribution_id/
    catalogue_id vs. input_key/edc_provider_url) — the DataOps UI already
    builds the request body per-DAG from that DAG's own param schema
    (see TriggerModal.tsx), so this endpoint must not constrain it to a
    fixed field set or it will silently drop whatever it doesn't declare.
    """
    return await af.trigger_dag(dag_id, conf)


@router.get("/{dag_id}/tasks")
async def list_tasks(dag_id: str):
    """List all tasks defined in a DAG."""
    return await af.list_tasks(dag_id)


@router.get("/{dag_id}/tasks/{task_id}")
async def get_task(dag_id: str, task_id: str):
    """Get details of a specific task in a DAG."""
    return await af.get_task(dag_id, task_id)


@router.delete("/{dag_id}")
async def delete_dag(dag_id: str):
    """Delete a DAG from Airflow."""
    return await af.delete_dag(dag_id)


@router.post("")
async def create_dag(body: DagCreateRequest):
    """Create a new DAG by combining existing tasks."""
    return await af.create_dag(body.dag_id, body.description, body.schedule, body.task_ids, body.owner)
