"""
Consumer side of the central connector: asks a testbed's connector what it
offers, over DSP, through our own connector's management API.

The testbed's management API is private, so everything here goes through the
central connector (EDC_PROVIDER_MANAGEMENT_URL / EDC_API_KEY, the same connector
edc_client.py registers assets on; it acts as consumer for other participants).
"""

import httpx
from fastapi import HTTPException

from config import EDC_API_KEY, EDC_PROVIDER_MANAGEMENT_URL

_CONTEXT = {"@vocab": "https://w3id.org/edc/v0.0.1/ns/"}


def _as_list(value) -> list:
    if value is None:
        return []
    return value if isinstance(value, list) else [value]


def _scalar(value) -> str:
    if isinstance(value, dict):
        value = value.get("@value", "")
    if isinstance(value, list):
        value = value[0] if value else ""
    return str(value) if value is not None else ""


def parse_catalog(catalog: dict) -> list[dict]:
    """Flatten an EDC DSP catalogue (JSON-LD) into one dict per offered asset.

    Tolerant of the two shapes JSON-LD compaction produces (a single object or a list
    for dcat:dataset / odrl:hasPolicy) and of the vocab-prefixed or plain property names.
    """
    assets = []
    for dataset in _as_list(catalog.get("dcat:dataset") or catalog.get("dataset")):
        asset_id = dataset.get("@id") or _scalar(dataset.get("id") or dataset.get("edc:id"))
        if not asset_id:
            continue
        offers = _as_list(dataset.get("odrl:hasPolicy") or dataset.get("hasPolicy"))
        title = next((_scalar(dataset[k]) for k in ("name", "edc:name", "dct:title") if dataset.get(k)), "")
        assets.append({
            "asset_id": asset_id,
            "title": title,
            "offer_id": offers[0].get("@id") if offers else None,
        })
    return assets


def fetch_catalog(dsp_url: str, participant_id: str) -> list[dict]:
    """Catalogue request to a testbed connector. Raises HTTPException(502) when it cannot be reached."""
    if not EDC_PROVIDER_MANAGEMENT_URL:
        raise HTTPException(status_code=503, detail="EDC_PROVIDER_MANAGEMENT_URL not configured")
    headers = {"X-Api-Key": EDC_API_KEY} if EDC_API_KEY else {}
    body = {
        "@context": _CONTEXT,
        "@type": "CatalogRequest",
        "counterPartyAddress": dsp_url,
        "counterPartyId": participant_id,
        "protocol": "dataspace-protocol-http",
    }
    try:
        r = httpx.post(f"{EDC_PROVIDER_MANAGEMENT_URL.rstrip('/')}/v3/catalog/request", json=body,
                       headers=headers, timeout=30)
    except httpx.RequestError as e:
        raise HTTPException(status_code=502, detail=f"Could not reach the central connector: {e}")
    if r.status_code != 200:
        raise HTTPException(
            status_code=502,
            detail=f"Catalogue request to {dsp_url} failed: {r.status_code} {r.text[:300]}",
        )
    try:
        return parse_catalog(r.json())
    except ValueError:
        raise HTTPException(status_code=502, detail=f"Central connector answered with non-JSON: {r.text[:200]}")


# Transfer-process states, per EDC: STARTED is the steady state of a running transfer (our PiveauData
# PUSH transfer stays STARTED while it keeps polling the bucket). The ones before it are on their way
# there; the ones after it are over.
IN_PROGRESS_STATES = {"INITIAL", "PROVISIONING", "PROVISIONED", "REQUESTING", "REQUESTED", "STARTING", "RESUMING"}
ENDED_STATES = {"COMPLETED", "TERMINATING", "TERMINATED", "DEPROVISIONING", "DEPROVISIONED", "SUSPENDED"}


def _norm_address(url: str | None) -> str:
    return (url or "").strip().rstrip("/").lower()


def parse_transfers(items: list, dsp_url: str | None = None) -> list[dict]:
    """Flatten transfer processes (EDC management API) into plain dicts, newest first.

    When `dsp_url` is given, transfers whose counterPartyAddress is a different connector are dropped,
    so an asset id that exists on two testbeds never mixes their transfers. A transfer without a
    counterPartyAddress is kept.
    """
    wanted = _norm_address(dsp_url)
    out = []
    for t in _as_list(items):
        address = _scalar(t.get("counterPartyAddress") or t.get("edc:counterPartyAddress"))
        if wanted and address and _norm_address(address) != wanted:
            continue
        state = _scalar(t.get("state") or t.get("edc:state"))
        stamp = t.get("stateTimestamp") or t.get("edc:stateTimestamp") or 0
        out.append({
            "transfer_id": t.get("@id") or _scalar(t.get("id")),
            "state": state,
            "active": state == "STARTED",
            "contract_id": _scalar(t.get("contractId") or t.get("edc:contractId")),
            "transfer_type": _scalar(t.get("transferType") or t.get("edc:transferType")),
            "state_timestamp": int(stamp) if str(stamp).isdigit() else 0,
        })
    return sorted(out, key=lambda x: x["state_timestamp"], reverse=True)


def best_transfer(transfers: list[dict]) -> dict | None:
    """The one to track for an asset: a STARTED transfer if any, else one still starting, else the newest."""
    for group in ({"STARTED"}, IN_PROGRESS_STATES):
        for t in transfers:
            if t["state"] in group:
                return t
    return transfers[0] if transfers else None


def find_transfers(asset_id: str, dsp_url: str) -> list[dict]:
    """Transfer processes on the central connector for `asset_id` with the testbed at `dsp_url`."""
    if not EDC_PROVIDER_MANAGEMENT_URL:
        raise HTTPException(status_code=503, detail="EDC_PROVIDER_MANAGEMENT_URL not configured")
    headers = {"X-Api-Key": EDC_API_KEY} if EDC_API_KEY else {}
    body = {
        "@context": _CONTEXT,
        "@type": "QuerySpec",
        "filterExpression": [{"operandLeft": "assetId", "operator": "=", "operandRight": asset_id}],
        "limit": 100,
    }
    try:
        r = httpx.post(f"{EDC_PROVIDER_MANAGEMENT_URL.rstrip('/')}/v3/transferprocesses/request", json=body,
                       headers=headers, timeout=30)
    except httpx.RequestError as e:
        raise HTTPException(status_code=502, detail=f"Could not reach the central connector: {e}")
    if r.status_code != 200:
        raise HTTPException(status_code=502, detail=f"Transfer query failed: {r.status_code} {r.text[:300]}")
    try:
        return parse_transfers(r.json(), dsp_url)
    except ValueError:
        raise HTTPException(status_code=502, detail=f"Central connector answered with non-JSON: {r.text[:200]}")
