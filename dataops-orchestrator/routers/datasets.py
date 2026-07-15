import json
import uuid

from fastapi import APIRouter, Form, HTTPException, UploadFile
import airflow_client as af

import datalake_client as dlc
import piveau_dataset_client as pdc
from config import CONTRIBUTED_DATASETS_CATALOGUE, DATASPACE_S3_ENDPOINT_URL, VALIDATION_DAG_ID
from dataset_models import DatasetSubmission

router = APIRouter(prefix="/datasets", tags=["Datasets"])


@router.get("")
async def list_datasets(catalogue_id: str | None = None):
    """List datasets known to Airflow, optionally scoped to a single catalogue."""
    return await af.list_datasets(catalogue_id)


@router.get("/catalogues")
async def list_catalogues():
    """List all catalogues known to piveau, for scoping a subsequent dataset listing."""
    return await af.list_catalogues()


@router.get("/{dataset_id}")
async def get_dataset(dataset_id: int):
    """Get details of a specific dataset."""
    return await af.get_dataset(dataset_id)


@router.post("/submit")
async def submit_dataset(file: UploadFile, metadata: str = Form(...), expectations: str = Form("[]")):
    """
    Submit a new dataset: upload its file to the Data Lake, register a full
    MAP (DCAT-AP + GAIA-X + CMT) record in the Staging Catalogue, and trigger
    the data quality validation DAG against it.

    `metadata` is a JSON-encoded DatasetSubmission (see dataset_models.py).
    `expectations` is a JSON-encoded list of Great Expectations configs, e.g.
    [{"type": "expect_table_row_count_to_be_between", "min_value": 1},
     {"type": "expect_column_to_exist", "column": "timestamp"}] — passed
    straight through to the validation DAG (see dali_dataspace_validate_dataset).
    """
    try:
        sub = DatasetSubmission.model_validate(json.loads(metadata))
    except (json.JSONDecodeError, ValueError) as e:
        raise HTTPException(status_code=422, detail=f"Invalid metadata: {e}")

    try:
        exp_list = json.loads(expectations)
        if not isinstance(exp_list, list):
            raise ValueError("expectations must be a JSON array")
    except (json.JSONDecodeError, ValueError) as e:
        raise HTTPException(status_code=422, detail=f"Invalid expectations: {e}")

    content = await file.read()
    if not content:
        raise HTTPException(status_code=422, detail="Uploaded file is empty")

    dataset_id = str(uuid.uuid4())
    catalogue_id = CONTRIBUTED_DATASETS_CATALOGUE

    object_key = dlc.upload_dataset_file(catalogue_id, dataset_id, file.filename, content)

    distribution_url = None
    if DATASPACE_S3_ENDPOINT_URL:
        distribution_url = f"{DATASPACE_S3_ENDPOINT_URL.rstrip('/')}/{catalogue_id}/{object_key}"

    piveau_result = await pdc.submit_dataset(dataset_id, catalogue_id, sub, distribution_url, file.content_type)

    dag_result = await af.trigger_dag(VALIDATION_DAG_ID, {
        "catalogue_id": catalogue_id,
        "dataset_id":   dataset_id,
        "asset_title":  file.filename,
        "expectations": exp_list,
    })

    return {
        "dataset_id":       dataset_id,
        "catalogue_id":     catalogue_id,
        "object_key":       object_key,
        "distribution_url": distribution_url,
        "piveau":           piveau_result,
        "validation_run":   dag_result,
    }
