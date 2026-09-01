import json
import uuid

from fastapi import APIRouter, Form, HTTPException, Query, Request, UploadFile
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


@router.post("/rdf")
async def create_dataset_rdf(request: Request):
    """
    Step 1 of dataset submission, RDF-first variant: register a dataset from a
    Turtle record the client built itself, instead of from the JSON field groups
    POST /datasets accepts. Same result — a dataset in the Staging Catalogue with
    no distributions yet — and the same next step
    (POST /datasets/{dataset_id}/distributions).

    This exists alongside POST /datasets, which is unchanged: JSON remains the
    stable API for programmatic clients, while the submission UI generates the
    MAP graph locally so the submitter can review the exact RDF before it is
    published, and the orchestrator forwards it to piveau essentially untouched.

    The body is `text/turtle`. Because dataset_id is minted here and the
    dataspace base URI is server configuration, the record must refer to the
    dataset as <urn:6gdali:dataset:self> and use "urn:6gdali:dataset:id" for
    dct:identifier; both are substituted on receipt (see
    piveau_dataset_client.resolve_sentinels). Nothing else in the graph is
    rewritten, so anything the MAP allows can be submitted this way — including
    properties the JSON models don't model.
    """
    turtle = (await request.body()).decode("utf-8", errors="replace")

    dataset_id = str(uuid.uuid4())
    catalogue_id = CONTRIBUTED_DATASETS_CATALOGUE

    piveau_result = await pdc.create_dataset_from_turtle(dataset_id, catalogue_id, turtle)

    return {
        "dataset_id":   dataset_id,
        "catalogue_id": catalogue_id,
        "piveau":       piveau_result,
    }


async def _register_distribution(
    dataset_id: str,
    file: UploadFile,
    catalogue_id: str,
    metrics: str,
    expectations: str,
    turtle: str | None = None,
) -> dict:
    """
    Shared body of both step-2 endpoints: upload the file to the Data Lake,
    register it as an EDC asset, publish the distribution to piveau, and trigger
    validation. Identical either way — only how the dcat:Distribution node is
    composed differs.

    `turtle`, when given, is a client-built distribution document whose
    placeholders this server resolves (see
    piveau_dataset_client.add_distribution_from_turtle). When None, the node is
    built here from the upload's own metadata (the original behaviour).
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
    # connector, under the same asset_id the consumer side
    # (dali.datalake.download_dataset_edc) later filters a provider's
    # catalogue by — so it becomes discoverable/negotiable over EDC. The
    # file's own name is registered alongside it, which is what the consumer
    # resolves the distribution's filename (and so its format) from. Runs
    # before the piveau publish below since it's independent of it (piveau
    # doesn't need to know about the EDC asset, or vice versa). Best-effort:
    # the S3 upload above already succeeded, so an EDC hiccup is reported,
    # not raised as a 5xx, and doesn't block the piveau publish that follows.
    edc_result = await edc.register_asset(catalogue_id, asset_id, object_key, media_type, file.filename)

    # Step 3: publish the distribution to piveau — either from the submitted
    # RDF, or (the original path) built here from the upload's own metadata.
    # byte_size/ext are passed through for dcat:byteSize and dct:format (the EU
    # file-type IRI) on the built node — both required by dali:DistributionShape
    # and knowable only here, from the upload itself. A submitted document
    # carries its own, so it needs neither.
    if turtle is not None:
        piveau_result = await pdc.add_distribution_from_turtle(
            dataset_id, catalogue_id, asset_id, turtle,
            distribution_url=distribution_url, access_url=pdc.EDC_CONNECTOR_URL,
        )
    else:
        piveau_result = await pdc.add_distribution(
            dataset_id, catalogue_id, distribution_id, asset_id,
            distribution_url, file.filename, media_type, dist_metrics,
            byte_size=len(content), ext=ext,
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
    already-created dataset (see POST /datasets), register the distribution in
    piveau, and trigger the data quality validation DAG against it.

    The dcat:Distribution node is composed here from the upload's own metadata.
    See POST /datasets/{dataset_id}/distributions/rdf for the variant that
    registers a distribution the client described itself. Unchanged: this remains
    the stable API for programmatic clients.
    """
    return await _register_distribution(dataset_id, file, catalogue_id, metrics, expectations)


@router.post("/{dataset_id}/distributions/rdf")
async def add_distribution_rdf(
    dataset_id: str,
    file: UploadFile,
    catalogue_id: str = Form(...),
    turtle: str = Form(...),
    metrics: str = Form("{}"),
    expectations: str = Form("[]"),
):
    """
    Step 2 of dataset submission, RDF-first variant: upload a file *together with
    the dcat:Distribution document describing it*, rather than having this server
    compose that node from form fields.

    Same multipart shape as POST /datasets/{dataset_id}/distributions (the file
    still has to be uploaded), plus a `turtle` field holding the distribution's
    own statements. Three of its values cannot be known before the upload — the
    asset id is minted here, the Data Lake object URL only exists afterwards, and
    the connector URL is server configuration — so the document refers to them as
    <urn:6gdali:distribution:self>, "urn:6gdali:distribution:asset-id",
    <urn:6gdali:datalake:object-url> and <urn:6gdali:edc:connector-url>, all
    substituted on receipt (see
    piveau_dataset_client.resolve_distribution_sentinels).

    `metrics` is still accepted but is not used to build the node — the submitted
    document already carries schema:variableMeasured/measurementTechnique. It is
    kept in the signature so the two endpoints take the same form fields.

    The response's `piveau.turtle` is the document as actually stored, with the
    sentinels resolved, so a caller can replace the placeholders it sent with the
    final graph.
    """
    return await _register_distribution(
        dataset_id, file, catalogue_id, metrics, expectations, turtle=turtle
    )


@router.delete("/{dataset_id}/distributions/{asset_id}")
async def delete_distribution(dataset_id: str, asset_id: str, catalogue_id: str = Query(...)):
    """
    Delete a single distribution — the reverse of POST
    .../distributions, in reverse order (piveau -> EDC -> S3, mirroring that
    endpoint's S3 -> EDC -> piveau creation order): unlink/remove its
    dcat:Distribution node from piveau first (stops it being discoverable
    immediately), then its EDC asset, then its S3 object(s). `asset_id` (not
    the sequential distribution_id) is required — it's the one stable
    identifier shared across all three systems (see piveau_dataset_client.add_distribution).
    """
    piveau_result = await pdc.delete_distribution(dataset_id, catalogue_id, asset_id)
    edc_result = await edc.delete_asset(asset_id)
    s3_deleted = dlc.delete_objects_by_prefix(catalogue_id, f"{dataset_id}/{asset_id}.")

    return {
        "dataset_id":     dataset_id,
        "catalogue_id":   catalogue_id,
        "asset_id":       asset_id,
        "piveau":         piveau_result,
        "edc":            edc_result,
        "s3_deleted_keys": s3_deleted,
    }


@router.delete("/{dataset_id}")
async def delete_dataset(dataset_id: str, catalogue_id: str = Query(...)):
    """
    Delete a dataset entirely, including all of its distributions. The
    distributions' asset_ids have to be read from piveau *before* the
    dataset record is deleted (that's the only place they're listed), so the
    order here is: list -> delete each EDC asset -> delete the piveau
    record (removes every dcat:Distribution node at once, since they live
    inside the same record) -> delete every S3 object under this dataset's
    prefix (covers each distribution's file plus any GX result files, which
    aren't tied to one specific asset_id).
    """
    asset_ids = await pdc.list_asset_ids(dataset_id, catalogue_id)
    edc_results = [await edc.delete_asset(asset_id) for asset_id in asset_ids]
    piveau_result = await pdc.delete_dataset(dataset_id, catalogue_id)
    s3_deleted = dlc.delete_objects_by_prefix(catalogue_id, f"{dataset_id}/")

    return {
        "dataset_id":         dataset_id,
        "catalogue_id":       catalogue_id,
        "distribution_count": len(asset_ids),
        "piveau":             piveau_result,
        "edc":                edc_results,
        "s3_deleted_keys":    s3_deleted,
    }
