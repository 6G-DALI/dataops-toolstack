"""
Registers newly submitted distributions as EDC (Eclipse Dataspace Connector)
assets in our own provider connector's Management API, so they become
discoverable/negotiable by EDC consumers.

The asset's @id is the distribution's dali:assetId — the same identifier
written into its piveau description (see piveau_dataset_client.add_distribution)
— so piveau and EDC agree on one identifier per distribution. The consumer
side matches this: dali.datalake.download_dataset_edc (Airflow) filters a
provider's catalogue by "https://w3id.org/edc/v0.0.1/ns/id" = that same
asset_id, and recovers the file's name from the `name` property registered
below. Change the two together.

Registration is best-effort and happens *after* a distribution is already
fully registered in piveau and uploaded to the Data Lake (see
routers/datasets.py) — a failure here is reported back but never rolls back
or blocks that already-successful registration.
"""

import logging

import httpx

from config import (
    DATASPACE_S3_ACCESS_KEY,
    DATASPACE_S3_ENDPOINT_URL,
    DATASPACE_S3_SECRET_KEY,
    EDC_API_KEY,
    EDC_PROVIDER_MANAGEMENT_URL,
)

log = logging.getLogger(__name__)

_EDC_CONTEXT = {"@vocab": "https://w3id.org/edc/v0.0.1/ns/"}

# A single global policy/contract-definition pair, created once (idempotently
# — a 409 because they already exist is treated as success) and reused for
# every asset: assetsSelector: [] matches every asset on this connector, so
# nothing further is needed per-asset for it to actually be negotiable. Same
# shapes as airflow/tests/edc_test_files/provider_helper.py's test scaffolding.
_POLICY_ID = "dali-no-constraint-policy"
_CONTRACT_DEFINITION_ID = "dali-contract-definition"


def _configured() -> bool:
    return bool(EDC_PROVIDER_MANAGEMENT_URL)


async def _post(client: httpx.AsyncClient, path: str, body: dict) -> httpx.Response:
    headers = {"X-Api-Key": EDC_API_KEY} if EDC_API_KEY else {}
    return await client.post(f"{EDC_PROVIDER_MANAGEMENT_URL.rstrip('/')}{path}", json=body, headers=headers)


async def _ensure_policy_and_contract_definition(client: httpx.AsyncClient) -> None:
    r = await _post(client, "/v3/policydefinitions", {
        "@context": _EDC_CONTEXT,
        "@id": _POLICY_ID,
        "policy": {"@context": "http://www.w3.org/ns/odrl.jsonld", "@type": "Set"},
    })
    if r.status_code not in (200, 201, 409):
        r.raise_for_status()

    r = await _post(client, "/v3/contractdefinitions", {
        "@context": _EDC_CONTEXT,
        "@id": _CONTRACT_DEFINITION_ID,
        "accessPolicyId": _POLICY_ID,
        "contractPolicyId": _POLICY_ID,
        "assetsSelector": [],
    })
    if r.status_code not in (200, 201, 409):
        r.raise_for_status()


async def register_asset(
    catalogue_id: str, asset_id: str, object_key: str, media_type: str | None = None, title: str | None = None
) -> dict:
    """Register one distribution's uploaded object as an EDC asset.

    `asset_id` — the same dali:assetId value written into the distribution's
    piveau description (see piveau_dataset_client.add_distribution) — becomes
    the EDC asset's own @id, so the two systems agree on one identifier for
    the same distribution. `object_key` (e.g. "{dataset_id}/{asset_id}.{ext}",
    see datalake_client.upload_dataset_file) is only used to locate the
    object within the bucket (dataAddress.prefix) — a separate concern from
    the asset's identifier.

    Returns {"status": "registered" | "already_registered" | "skipped" | "failed", ...}
    — never raises; callers get a result they can surface without the whole
    request failing over an EDC hiccup (see routers/datasets.py).
    """
    if not _configured():
        return {"status": "skipped", "reason": "EDC_PROVIDER_MANAGEMENT_URL not configured"}
    if not (DATASPACE_S3_ENDPOINT_URL and DATASPACE_S3_ACCESS_KEY and DATASPACE_S3_SECRET_KEY):
        return {"status": "skipped", "reason": "Data Lake S3 (DATASPACE_S3_*) not configured"}

    properties = {}
    if title:
        properties["https://w3id.org/edc/v0.0.1/ns/name"] = title
    if media_type:
        properties["https://w3id.org/edc/v0.0.1/ns/contenttype"] = media_type

    asset = {
        "@context": _EDC_CONTEXT,
        "@id": asset_id,
        "properties": properties,
        "dataAddress": {
            "type":       "MinioAsset",
            "endpoint":   DATASPACE_S3_ENDPOINT_URL,
            "bucketName": catalogue_id,
            "accessKey":  DATASPACE_S3_ACCESS_KEY,
            "secretKey":  DATASPACE_S3_SECRET_KEY,
            "prefix":     object_key,
        },
    }

    try:
        async with httpx.AsyncClient(timeout=15) as client:
            await _ensure_policy_and_contract_definition(client)
            r = await _post(client, "/v3/assets", asset)
            if r.status_code == 409:
                return {"status": "already_registered", "asset_id": asset_id}
            r.raise_for_status()
        return {"status": "registered", "asset_id": asset_id, "edc_response": r.json()}
    except httpx.HTTPStatusError as e:
        log.warning("[edc] asset registration failed for %s: %s %s", asset_id, e.response.status_code, e.response.text[:500])
        return {"status": "failed", "asset_id": asset_id, "error": f"{e.response.status_code} {e.response.text[:500]}"}
    except httpx.RequestError as e:
        log.warning("[edc] could not reach provider connector at %s: %s", EDC_PROVIDER_MANAGEMENT_URL, e)
        return {"status": "failed", "asset_id": asset_id, "error": f"Could not reach EDC provider connector: {e}"}


def register_upload_asset(asset_id: str, bucket: str, title: str) -> str:
    """Offer a hosted testbed's upload bucket as an asset of this (the facility) connector. Returns 'created' or
    'exists'; raises HTTPException(502) when the connector refuses, since provisioning records that as a failed step.

    The data address is the connector's MinIO files source ('6GDaliTestbedExperiments', as on the testbed
    connectors): a PiveauData transfer of it keeps polling the bucket and pushes every new file. It reads the bucket
    with the orchestrator's own Data Lake key, like the distribution assets above, so rotating the testbed's upload
    key never breaks a running transfer.
    """
    from fastapi import HTTPException

    if not _configured():
        raise HTTPException(status_code=503, detail="EDC_PROVIDER_MANAGEMENT_URL not configured")
    if not (DATASPACE_S3_ENDPOINT_URL and DATASPACE_S3_ACCESS_KEY and DATASPACE_S3_SECRET_KEY):
        raise HTTPException(status_code=503, detail="Data Lake S3 (DATASPACE_S3_*) not configured")
    headers = {"X-Api-Key": EDC_API_KEY} if EDC_API_KEY else {}
    base = EDC_PROVIDER_MANAGEMENT_URL.rstrip("/")
    asset = {
        "@context": _EDC_CONTEXT,
        "@id": asset_id,
        "properties": {"https://w3id.org/edc/v0.0.1/ns/name": title},
        "dataAddress": {
            "type":       "6GDaliTestbedExperiments",
            "endpoint":   DATASPACE_S3_ENDPOINT_URL,
            "bucketName": bucket,
            "accessKey":  DATASPACE_S3_ACCESS_KEY,
            "secretKey":  DATASPACE_S3_SECRET_KEY,
            "prefix":     "",
        },
    }
    try:
        with httpx.Client(timeout=15, headers=headers) as client:
            for path, body in (
                ("/v3/policydefinitions", {"@context": _EDC_CONTEXT, "@id": _POLICY_ID,
                                           "policy": {"@context": "http://www.w3.org/ns/odrl.jsonld", "@type": "Set"}}),
                ("/v3/contractdefinitions", {"@context": _EDC_CONTEXT, "@id": _CONTRACT_DEFINITION_ID,
                                             "accessPolicyId": _POLICY_ID, "contractPolicyId": _POLICY_ID,
                                             "assetsSelector": []}),
            ):
                r = client.post(f"{base}{path}", json=body)
                if r.status_code not in (200, 201, 409):
                    r.raise_for_status()
            r = client.post(f"{base}/v3/assets", json=asset)
            if r.status_code == 409:
                return "exists"
            r.raise_for_status()
            return "created"
    except httpx.HTTPStatusError as e:
        raise HTTPException(status_code=502, detail=f"The facility connector refused asset '{asset_id}': "
                                                    f"{e.response.status_code} {e.response.text[:300]}")
    except httpx.RequestError as e:
        raise HTTPException(status_code=502, detail=f"Could not reach the facility connector at {base}: {e}")


async def delete_asset(asset_id: str) -> dict:
    """Delete one EDC asset by id (see register_asset — asset_id is the
    distribution's dali:assetId). Best-effort like register_asset: never
    raises, so a delete request's other cleanup steps (piveau, S3 — see
    routers/datasets.py) still happen even if the connector is unreachable.
    """
    if not _configured():
        return {"status": "skipped", "reason": "EDC_PROVIDER_MANAGEMENT_URL not configured"}

    try:
        async with httpx.AsyncClient(timeout=15) as client:
            r = await client.delete(f"{EDC_PROVIDER_MANAGEMENT_URL.rstrip('/')}/v3/assets/{asset_id}",
                                    headers={"X-Api-Key": EDC_API_KEY} if EDC_API_KEY else {})
            if r.status_code == 404:
                return {"status": "not_found", "asset_id": asset_id}
            r.raise_for_status()
        return {"status": "deleted", "asset_id": asset_id}
    except httpx.HTTPStatusError as e:
        log.warning("[edc] asset deletion failed for %s: %s %s", asset_id, e.response.status_code, e.response.text[:500])
        return {"status": "failed", "asset_id": asset_id, "error": f"{e.response.status_code} {e.response.text[:500]}"}
    except httpx.RequestError as e:
        log.warning("[edc] could not reach provider connector at %s: %s", EDC_PROVIDER_MANAGEMENT_URL, e)
        return {"status": "failed", "asset_id": asset_id, "error": f"Could not reach EDC provider connector: {e}"}
