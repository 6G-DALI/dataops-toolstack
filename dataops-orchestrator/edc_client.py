"""
Registers newly submitted distributions as EDC (Eclipse Dataspace Connector)
assets in our own provider connector's Management API, so they become
discoverable/negotiable by EDC consumers — mirroring the shape
dali.datalake.download_dataset_edc (Airflow, consumer side) expects when it
later negotiates and pulls one of these assets from *some* provider.

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
    return await client.post(f"{EDC_PROVIDER_MANAGEMENT_URL.rstrip('/')}{path}", json=body)


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
    catalogue_id: str, object_key: str, media_type: str | None = None, title: str | None = None
) -> dict:
    """Register one distribution's uploaded object as an EDC asset.

    `object_key` (e.g. "{dataset_id}/{asset_id}.{ext}", see
    datalake_client.upload_dataset_file) becomes the asset's own @id — this
    exact string is what dali.datalake.download_dataset_edc (the consumer
    side) filters a provider's catalogue by
    (https://w3id.org/edc/v0.0.1/ns/id = input_key), so it must match that
    convention exactly for a distribution to later be pulled via EDC.

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
        "@id": object_key,
        "properties": properties,
        "dataAddress": {
            "type":       "MinioFiles",
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
                return {"status": "already_registered", "asset_id": object_key}
            r.raise_for_status()
        return {"status": "registered", "asset_id": object_key, "edc_response": r.json()}
    except httpx.HTTPStatusError as e:
        log.warning("[edc] asset registration failed for %s: %s %s", object_key, e.response.status_code, e.response.text[:500])
        return {"status": "failed", "asset_id": object_key, "error": f"{e.response.status_code} {e.response.text[:500]}"}
    except httpx.RequestError as e:
        log.warning("[edc] could not reach provider connector at %s: %s", EDC_PROVIDER_MANAGEMENT_URL, e)
        return {"status": "failed", "asset_id": object_key, "error": f"Could not reach EDC provider connector: {e}"}
