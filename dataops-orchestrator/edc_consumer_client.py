"""
Consumer side of the central connector: asks a testbed's connector what it
offers, over DSP, through our own connector's management API.

The testbed's management API is private, so everything here goes through the
central connector (EDC_PROVIDER_MANAGEMENT_URL / EDC_API_KEY, the same connector
edc_client.py registers assets on; it acts as consumer for other participants).
"""

import logging
import time

import httpx
from fastapi import HTTPException

from config import EDC_API_KEY, EDC_PROVIDER_MANAGEMENT_URL

_CONTEXT = {"@vocab": "https://w3id.org/edc/v0.0.1/ns/"}

log = logging.getLogger(__name__)


class UpstreamNotFound(HTTPException):
    """The central connector answered 404 (an object it does not know). Still a 502 to our callers."""


def _bad_gateway(detail: str) -> HTTPException:
    """A failed call to the central connector: logged, because the HTTP response alone may not
    survive the proxies in front of the orchestrator (they can replace 5xx bodies)."""
    log.warning("[edc] %s", detail)
    return HTTPException(status_code=502, detail=detail)


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
    except httpx.HTTPError as e:
        raise _bad_gateway(f"Could not reach the central connector at {EDC_PROVIDER_MANAGEMENT_URL}: {type(e).__name__}: {e}")
    if r.status_code != 200:
        raise _bad_gateway(f"Catalogue request to {dsp_url} failed: {r.status_code} {r.text[:300]}",
        )
    try:
        return parse_catalog(r.json())
    except ValueError:
        raise _bad_gateway(f"Central connector answered with non-JSON: {r.text[:200]}")


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
    except httpx.HTTPError as e:
        raise _bad_gateway(f"Could not reach the central connector at {EDC_PROVIDER_MANAGEMENT_URL}: {type(e).__name__}: {e}")
    if r.status_code != 200:
        raise _bad_gateway(f"Transfer query failed: {r.status_code} {r.text[:300]}")
    try:
        return parse_transfers(r.json(), dsp_url)
    except ValueError:
        raise _bad_gateway(f"Central connector answered with non-JSON: {r.text[:200]}")


# --- contract agreements and negotiations -----------------------------------------------------

NEGOTIATION_DONE = {"FINALIZED"}
# NOT_FOUND is ours, not EDC's: the connector does not know a negotiation id we hold.
NEGOTIATION_FAILED = {"TERMINATED", "TERMINATING", "NOT_FOUND"}


def _mgmt(path: str) -> str:
    if not EDC_PROVIDER_MANAGEMENT_URL:
        raise HTTPException(status_code=503, detail="EDC_PROVIDER_MANAGEMENT_URL not configured")
    return f"{EDC_PROVIDER_MANAGEMENT_URL.rstrip('/')}{path}"


def _headers() -> dict:
    return {"X-Api-Key": EDC_API_KEY} if EDC_API_KEY else {}


def _call(method: str, path: str, body: dict | None = None, what: str = "request"):
    url = _mgmt(path)
    started = time.monotonic()
    try:
        r = httpx.request(method, url, json=body, headers=_headers(), timeout=30)
    except httpx.HTTPError as e:
        raise _bad_gateway(f"{what}: could not reach the central connector at {url}: {type(e).__name__}: {e}")
    log.info("[edc] %s %s -> %s (%.0f ms)", method, url, r.status_code, (time.monotonic() - started) * 1000)
    if r.status_code == 404:
        detail = f"{what} failed: 404 {r.text[:300]}"
        log.warning("[edc] %s", detail)
        raise UpstreamNotFound(status_code=502, detail=detail)
    if r.status_code not in (200, 201):
        raise _bad_gateway(f"{what} failed: {r.status_code} {r.text[:300]}")
    try:
        return r.json()
    except ValueError:
        raise _bad_gateway(f"{what}: central connector answered with non-JSON: {r.text[:200]}")


def parse_agreements(items, provider_id: str | None = None) -> list[dict]:
    """Contract agreements, newest first. With `provider_id`, agreements with another provider are dropped."""
    out = []
    for a in _as_list(items):
        provider = _scalar(a.get("providerId") or a.get("edc:providerId"))
        if provider_id and provider and provider != provider_id:
            continue
        signed = a.get("contractSigningDate") or a.get("edc:contractSigningDate") or 0
        out.append({
            "agreement_id": a.get("@id") or _scalar(a.get("id")),
            "provider_id": provider,
            "signing_date": int(signed) if str(signed).isdigit() else 0,
        })
    return sorted(out, key=lambda x: x["signing_date"], reverse=True)


def find_agreements(asset_id: str, provider_id: str) -> list[dict]:
    body = {"@context": _CONTEXT, "@type": "QuerySpec", "limit": 100,
            "filterExpression": [{"operandLeft": "assetId", "operator": "=", "operandRight": asset_id}]}
    return parse_agreements(_call("POST", "/v3/contractagreements/request", body, "Agreement query"), provider_id)


def _negotiation_summary(n: dict) -> dict:
    return {
        "negotiation_id": n.get("@id") or _scalar(n.get("id")),
        "state": _scalar(n.get("state") or n.get("edc:state")),
        "agreement_id": _scalar(n.get("contractAgreementId") or n.get("edc:contractAgreementId")) or None,
    }


def get_negotiation(negotiation_id: str) -> dict | None:
    """The negotiation's summary, or None when the connector does not know that id."""
    try:
        return _negotiation_summary(_call("GET", f"/v3/contractnegotiations/{negotiation_id}", what="Negotiation lookup"))
    except UpstreamNotFound:
        return None


def get_agreement_negotiation(agreement_id: str) -> dict | None:
    """The negotiation that produced an agreement; None when the connector no longer knows it."""
    try:
        return _negotiation_summary(_call("GET", f"/v3/contractagreements/{agreement_id}/negotiation",
                                          what="Agreement negotiation lookup"))
    except HTTPException:
        return None


def start_negotiation(dsp_url: str, provider_id: str, asset_id: str, offer_id: str) -> str:
    """Ask for a contract on an offer found in the testbed's catalogue. Returns the negotiation id."""
    body = {
        "@context": {"@vocab": "https://w3id.org/edc/v0.0.1/ns/", "odrl": "http://www.w3.org/ns/odrl/2/"},
        "@type": "ContractRequest",
        "counterPartyAddress": dsp_url,
        "providerId": provider_id,
        "protocol": "dataspace-protocol-http",
        "policy": {
            "@id": offer_id,
            "@type": "http://www.w3.org/ns/odrl/2/Offer",
            "odrl:permission": [], "odrl:prohibition": [], "odrl:obligation": [],
            "odrl:target": {"@id": asset_id},
            "odrl:assigner": {"@id": provider_id},
        },
    }
    result = _call("POST", "/v3/contractnegotiations", body, "Contract negotiation")
    return result.get("@id") or _scalar(result.get("id"))


def wait_negotiation(negotiation_id: str, timeout: float = 30, interval: float = 1, not_found_grace: float = 5) -> dict:
    """Poll until the negotiation is FINALIZED or TERMINATED, or `timeout` runs out (then returns the
    latest state, which is still in progress).

    A negotiation the connector does not know is retried for `not_found_grace` seconds (it may not be
    visible yet right after creation) and then reported as state NOT_FOUND.
    """
    started = time.monotonic()
    deadline = started + timeout
    while True:
        n = get_negotiation(negotiation_id)
        if n is None:
            if time.monotonic() - started >= not_found_grace:
                log.warning("[edc] negotiation %s is unknown to the central connector", negotiation_id)
                return {"negotiation_id": negotiation_id, "state": "NOT_FOUND", "agreement_id": None}
        elif n["state"] in NEGOTIATION_DONE or n["state"] in NEGOTIATION_FAILED or time.monotonic() >= deadline:
            return n
        time.sleep(interval)


def start_transfer(*, dsp_url: str, provider_id: str, asset_id: str, agreement_id: str, endpoint: str, bucket: str,
                   access_key: str, secret_key: str, piveau_url: str, piveau_api_key: str, prefix: str = "") -> str:
    """Start the long-lived PiveauData PUSH transfer: the testbed's data plane streams its bucket into the
    data lake (with the testbed's scoped key) and registers the files in piveau. Returns the transfer id."""
    body = {
        "@context": _CONTEXT,
        "@type": "TransferRequest",
        "dataDestination": {
            "type": "PiveauData", "endpoint": endpoint, "bucketName": bucket,
            "accessKey": access_key, "secretKey": secret_key, "prefix": prefix,
            "piveauUrl": piveau_url, "piveauApiKey": piveau_api_key,
        },
        "protocol": "dataspace-protocol-http",
        "assetId": asset_id,
        "contractId": agreement_id,
        "connectorId": provider_id,
        "counterPartyAddress": dsp_url,
        "transferType": "PiveauData-PUSH",
    }
    result = _call("POST", "/v3/transferprocesses", body, "Transfer start")
    return result.get("@id") or _scalar(result.get("id"))


def get_transfer_state(transfer_id: str) -> str:
    t = _call("GET", f"/v3/transferprocesses/{transfer_id}", what="Transfer lookup")
    return _scalar(t.get("state") or t.get("edc:state"))
