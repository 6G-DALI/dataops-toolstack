from fastapi import APIRouter
import airflow_client as af

router = APIRouter(prefix="/datasets", tags=["Datasets"])


@router.get("")
async def list_datasets():
    """List all datasets known to Airflow."""
    return await af.list_datasets()


@router.get("/{dataset_id}")
async def get_dataset(dataset_id: int):
    """Get details of a specific dataset."""
    return await af.get_dataset(dataset_id)
