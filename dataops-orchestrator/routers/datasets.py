import json
import uuid

from fastapi import APIRouter, Form, HTTPException, UploadFile
import airflow_client as af

import datalake_client as dlc
import piveau_dataset_client as pdc
from config import CONTRIBUTED_DATASETS_CATALOGUE, DATASPACE_S3_ENDPOINT_URL, VALIDATION_DAG_ID
from dataset_models import DatasetCreateRequest, DistributionMetrics

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


@router.get("/{dataset_id}/distributions")
async def list_distributions(dataset_id: str, catalogue_id: str | None = None):
    """List the distributions of a single dataset, for the second step of the
    Catalogue -> Dataset -> Distribution DAG-trigger picker."""
    return await af.list_distributions(dataset_id, catalogue_id)


@router.post("")
async def create_dataset(payload: DatasetCreateRequest):
    """
    Step 1 of dataset submission: register the dataset's own metadata (MAP
    Identity / Object Characteristics / Testbed Context) in the Staging
    Catalogue. No file, no distribution yet — the dataset isn't validatable
    until at least one distribution is added via
    POST /datasets/{dataset_id}/distributions.
    """
    dataset_id = str(uuid.uuid4())
    catalogue_id = CONTRIBUTED_DATASETS_CATALOGUE

    piveau_result = await pdc.create_dataset(
        dataset_id, catalogue_id, payload.identity, payload.object, payload.testbed_context
    )

    return {
        "dataset_id":   dataset_id,
        "catalogue_id": catalogue_id,
        "piveau":       piveau_result,
    }


@router.post("/{dataset_id}/distributions")
async def add_distribution(
    dataset_id: str,
    file: UploadFile,
    catalogue_id: str = Form(...),
    metrics: str = Form("{}"),
    expectations: str = Form("[]"),
):
    """
    Step 2 of dataset submission: upload a file as a new distribution of an
    already-created dataset (see POST /datasets), register the distribution
    in piveau, and trigger the data quality validation DAG against it.

    Can be called more than once per dataset to add further distributions —
    each gets the next sequential distribution_id.

    `metrics` is a JSON-encoded DistributionMetrics (see dataset_models.py) —
    the column list (`schema:variableMeasured`) and measurement technique for
    *this* distribution's file, not the dataset as a whole.
    `expectations` is a JSON-encoded list of Great Expectations configs, e.g.
    [{"type": "expect_table_row_count_to_be_between", "min_value": 1},
     {"type": "expect_column_to_exist", "column": "timestamp"}] — passed
    straight through to the validation DAG (see dali_dataspace_validate_dataset).
    """
    try:
        dist_metrics = DistributionMetrics.model_validate(json.loads(metrics))
    except (json.JSONDecodeError, ValueError) as e:
        raise HTTPException(status_code=422, detail=f"Invalid metrics: {e}")

    try:
        exp_list = json.loads(expectations)
        if not isinstance(exp_list, list):
            raise ValueError("expectations must be a JSON array")
    except (json.JSONDecodeError, ValueError) as e:
        raise HTTPException(status_code=422, detail=f"Invalid expectations: {e}")

    content = await file.read()
    if not content:
        raise HTTPException(status_code=422, detail="Uploaded file is empty")

    if not DATASPACE_S3_ENDPOINT_URL:
        raise HTTPException(status_code=503, detail="DATASPACE_S3_ENDPOINT_URL not configured")

    # asset_id is a fresh UUID, independent of distribution_id (which only
    # numbers/locates the dcat:Distribution node in the dataset's graph). The
    # object is named "{asset_id}.{ext}" (ext derived from content-type), so
    # the validate DAG can later resolve this exact object key from
    # dali:assetId + dcat:mediaType alone (see dali.dataspace.resolve_asset_title).
    # add_distribution below writes this same asset_id as the new
    # distribution's dali:assetId, and the uploaded file's original name as
    # its dct:title.
    distribution_id = await pdc.next_distribution_id(dataset_id, catalogue_id)
    asset_id = str(uuid.uuid4())
    ext = pdc.extension_for_media_type(file.content_type)
    object_filename = f"{asset_id}.{ext}"
    object_key = dlc.upload_dataset_file(catalogue_id, dataset_id, object_filename, content)
    distribution_url = f"{DATASPACE_S3_ENDPOINT_URL.rstrip('/')}/{catalogue_id}/{object_key}"

    piveau_result = await pdc.add_distribution(
        dataset_id, catalogue_id, distribution_id, asset_id,
        distribution_url, file.filename, file.content_type, dist_metrics
    )

    # The DAG's `distribution_id` param is used to locate this exact
    # dcat:Distribution node in piveau (see dali.utils.dist_keys), which only
    # matches a node's own @id / dct:identifier — never our internal
    # sequential `distribution_id` counter above, which piveau has no
    # knowledge of, nor asset_id directly. piveau mints its own new @id for
    # the distribution on write, so add_distribution re-fetches the dataset
    # afterwards and resolves that real, piveau-assigned id (matched via
    # asset_id, which is preserved verbatim) — that's what has to be passed
    # here for the DAG to actually find the node.
    dag_result = await af.trigger_dag(VALIDATION_DAG_ID, {
        "catalogue_id":    catalogue_id,
        "dataset_id":      dataset_id,
        "distribution_id": piveau_result["distribution_id"],
        "expectations":    exp_list,
    })

    return {
        "dataset_id":       dataset_id,
        "catalogue_id":     catalogue_id,
        "distribution_id":  distribution_id,
        "object_key":       object_key,
        "distribution_url": distribution_url,
        "piveau":           piveau_result,
        "validation_run":   dag_result,
    }
