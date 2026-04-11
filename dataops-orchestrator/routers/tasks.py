from fastapi import APIRouter
from pydantic import BaseModel
import airflow_client as af

router = APIRouter(prefix="/tasks", tags=["Tasks"])


class TaskCreateRequest(BaseModel):
    task_id: str
    description: str = ""
    code: str


@router.get("")
async def list_all_tasks():
    """List all tasks across all DAGs."""
    return await af.list_all_tasks()


@router.get("/custom")
async def list_custom_tasks():
    """List user-defined task files."""
    return await af.list_custom_tasks()


@router.get("/custom/{task_id}")
async def get_custom_task(task_id: str):
    """Get a single user-defined task file with its code."""
    return await af.get_custom_task(task_id)


@router.post("")
async def create_task(req: TaskCreateRequest):
    """Create or update a user-defined task Python file."""
    return await af.create_task(req.task_id, req.description, req.code)
