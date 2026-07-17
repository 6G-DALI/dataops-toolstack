import json
import uuid

from fastapi import APIRouter, Form, HTTPException, UploadFile
import airflow_client as af

import datalake_client as dlc
import edc_client as edc
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
    # the validate DAG can later resolve this exact object key by listing the
    # bucket for that prefix (see dali.datalake.download_dataset) — no piveau
    # round trip needed. add_distribution below writes this same asset_id as
    # the new distribution's dali:assetId, and the uploaded file's original
    # name as its dct:title.
    distribution_id = await pdc.next_distribution_id(dataset_id, catalogue_id)
    asset_id = str(uuid.uuid4())
    # Prefer the original filename's own extension — more reliable than
    # content-type, since browsers/clients often send generic or wrong
    # content-types for less common formats (e.g. .jsonl as
    # application/octet-stream). Only fall back to the content-type mapping
    # when the filename itself has no extension.
    ext = (
        file.filename.rsplit(".", 1)[-1].lower()
        if file.filename and "." in file.filename
        else pdc.extension_for_media_type(file.content_type)
    )
    object_filename = f"{asset_id}.{ext}"
    # Step 1: upload the file to the Data Lake (S3) first — everything below
    # (the EDC asset's dataAddress, piveau's dcat:accessURL) points at this
    # object, so it has to exist before either is registered.
    object_key = dlc.upload_dataset_file(catalogue_id, dataset_id, object_filename, content)
    distribution_url = f"{DATASPACE_S3_ENDPOINT_URL.rstrip('/')}/{catalogue_id}/{object_key}"

    # dcat:mediaType: trust the browser/client's content-type unless it's
    # missing or too generic (e.g. application/octet-stream, which browsers
    # send for less common formats like .jsonl) to be worth recording as-is —
    # in that case, register the canonical media type for the extension we
    # actually resolved above instead.
    media_type = pdc.resolve_media_type(file.content_type, ext)

    # Step 2: register this distribution as an EDC asset on our own provider
    # connector, using the exact same object_key convention the consumer
    # side (dali.datalake.download_dataset_edc) later filters a provider's
    # catalogue by — so it becomes discoverable/negotiable over EDC. Runs
    # before the piveau publish below since it's independent of it (piveau
    # doesn't need to know about the EDC asset, or vice versa). Best-effort:
    # the S3 upload above already succeeded, so an EDC hiccup is reported,
    # not raised as a 5xx, and doesn't block the piveau publish that follows.
    edc_result = await edc.register_asset(catalogue_id, asset_id, object_key, media_type, file.filename)

    # Step 3: publish the distribution to piveau.
    piveau_result = await pdc.add_distribution(
        dataset_id, catalogue_id, distribution_id, asset_id,
        distribution_url, file.filename, media_type, dist_metrics
    )

    # Step 4: trigger the validation DAG now that the distribution is fully
    # registered. The DAG's `asset_id` param is used both to resolve the
    # distribution's S3 object (dali.datalake.download_dataset lists the
    # bucket for "{dataset_id}/{asset_id}.*") and to locate its
    # dcat:Distribution node in piveau (dali.dataspace.publish_quality_to_piveau,
    # via dist_keys) — asset_id works for the latter because it's embedded
    # as the last path segment of dct:identifier (see
    # piveau_dataset_client.add_distribution), which is stable, unlike the
    # node's own @id (piveau mints its own UUID for that on write).
    dag_result = await af.trigger_dag(VALIDATION_DAG_ID, {
        "catalogue_id": catalogue_id,
        "dataset_id":   dataset_id,
        "asset_id":     asset_id,
        "expectations": exp_list,
    })

    return {
        "dataset_id":       dataset_id,
        "catalogue_id":     catalogue_id,
        "distribution_id":  distribution_id,
        "object_key":       object_key,
        "distribution_url": distribution_url,
        "piveau":           piveau_result,
        "validation_run":   dag_result,
        "edc":              edc_result,
    }
