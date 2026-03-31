from fastapi import APIRouter
import airflow_client as af

router = APIRouter(prefix="/tasks", tags=["Tasks"])


@router.get("")
async def list_all_tasks():
    """List all tasks across all DAGs."""
    return await af.list_all_tasks()
