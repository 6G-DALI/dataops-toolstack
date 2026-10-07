"""
Testbed registry: the list of testbeds in the Data Space and the provisioning
that onboarding one needs (identity, Data Lake bucket + scoped key, piveau
catalogue, connector bundle). Admin-only: see auth.require_testbed_admin.

Provisioning is a series of idempotent steps, each recorded in `steps`, so a
failure can be fixed and retried without redoing (or duplicating) the others.
Connecting the connector and managing its transfers/assets come in a later phase.
"""

import re
import time
from datetime import datetime, timezone

from fastapi import APIRouter, Depends, HTTPException, Response
from pydantic import BaseModel, Field

import datalake_admin
import edc_consumer_client
import piveau_dataset_client as pdc
import piveau_catalogue_client
import testbed_store as store
from auth import require_testbed_admin
from config import DATALAKE_PUBLIC_ENDPOINT_URL, TESTBED_BUCKET_PREFIX, TESTBED_DOMAIN_SUFFIX
from testbed_bundle import build_bundle

router = APIRouter(prefix="/testbeds", tags=["Testbeds"], dependencies=[Depends(require_testbed_admin)])

_SLUG_RE = re.compile(r"^[a-z][a-z0-9-]{1,30}[a-z0-9]$")


class TestbedCreate(BaseModel):
    slug: str = Field(description="Short lowercase id, e.g. 'kul'. Drives the defaults below.")
    name: str
    organisation: str | None = None
    contact_email: str | None = None
    # Everything below is derived from the slug when omitted; give them to adopt an
    # already-running testbed with its existing identity and bucket.
    participant_id: str | None = None
    experiment_prefix: str | None = None
    bucket: str | None = None
    dsp_url: str | None = None
    produced_by_iri: str | None = None


def _now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def _actor(claims: dict) -> str:
    return claims.get("preferred_username") or claims.get("email") or claims.get("sub", "unknown")


def _get(slug: str) -> dict:
    tb = store.get_testbed(slug)
    if not tb:
        raise HTTPException(status_code=404, detail=f"Testbed '{slug}' not found")
    return tb


@router.get("")
def list_testbeds():
    items = store.list_testbeds()
    return {"testbeds": items, "total": len(items)}


@router.post("", status_code=201)
def register_testbed(body: TestbedCreate, claims: dict = Depends(require_testbed_admin)):
    if not _SLUG_RE.match(body.slug):
        raise HTTPException(status_code=422, detail="slug must be 3-32 chars: lowercase letters, digits, '-', starting with a letter")
    participant_id = body.participant_id or f"provider-{body.slug}"
    bucket = body.bucket or f"{TESTBED_BUCKET_PREFIX}{body.slug}"
    if not datalake_admin.valid_bucket_name(bucket):
        raise HTTPException(status_code=422, detail=f"'{bucket}' is not a valid bucket name (3-63 chars, lowercase, digits, '.', '-')")
    prefix = body.experiment_prefix or bucket
    return store.create_testbed(
        {
            "slug": body.slug, "name": body.name, "organisation": body.organisation,
            "contact_email": body.contact_email, "participant_id": participant_id,
            "ids_id": f"urn:connector:{participant_id}", "experiment_prefix": prefix,
            "bucket": bucket, "catalogue_id": bucket,
            "dsp_url": body.dsp_url or f"https://edc.{body.slug}.{TESTBED_DOMAIN_SUFFIX}/protocol",
            "produced_by_iri": body.produced_by_iri,
        },
        _actor(claims),
    )


@router.get("/{slug}")
def get_testbed(slug: str):
    return _get(slug)


@router.get("/{slug}/assets")
def list_assets(slug: str):
    _get(slug)
    assets = store.list_assets(slug)
    return {"assets": assets, "total": len(assets)}


@router.post("/{slug}/assets/discover")
def discover_assets(slug: str, claims: dict = Depends(require_testbed_admin)):
    """Ask the testbed's connector, through the central connector, what it offers, and store each offered
    asset. These stored assets are what contract negotiation and transfers are started for later.

    A successful catalogue request also proves the connector is reachable, so it is recorded as the
    'connector' step; a failure is recorded there too and returned as an error.
    """
    tb = _get(slug)
    steps = dict(tb["steps"])
    try:
        offered = edc_consumer_client.fetch_catalog(tb["dsp_url"], tb["participant_id"])
    except HTTPException as e:
        steps["connector"] = {"status": "failed", "detail": e.detail, "at": _now()}
        store.update_testbed(slug, steps=steps)
        store.audit(slug, _actor(claims), "discover-assets", f"failed: {e.detail}"[:200])
        raise
    steps["connector"] = {"status": "ok", "detail": "catalogue reachable", "at": _now()}
    store.update_testbed(slug, steps=steps)
    assets = store.sync_assets(slug, offered)
    store.audit(slug, _actor(claims), "discover-assets", f"{len(offered)} offered")
    return {"assets": assets, "total": len(assets), "offered": len(offered)}


def _asset(slug: str, asset_id: str) -> dict:
    asset = next((a for a in store.list_assets(slug) if a["asset_id"] == asset_id), None)
    if not asset:
        raise HTTPException(status_code=404, detail=f"Asset '{asset_id}' not found for testbed '{slug}'")
    return asset


def _contract_info(asset: dict) -> dict | None:
    if not (asset["contract_agreement_id"] or asset["negotiation_id"]):
        return None
    return {"agreement_id": asset["contract_agreement_id"], "negotiation_id": asset["negotiation_id"],
            "negotiation_state": asset["negotiation_state"]}


@router.post("/{slug}/assets/{asset_id}/transfers/find")
def find_transfers(slug: str, asset_id: str, claims: dict = Depends(require_testbed_admin)):
    """Look on the central connector for this asset's contract and its transfers, and store what is found
    on the asset: the agreement (and the negotiation behind it) and the transfer worth tracking (a running
    one if there is one)."""
    tb = _get(slug)
    asset = _asset(slug, asset_id)

    agreement = negotiation = None
    try:
        agreements = edc_consumer_client.find_agreements(asset_id, tb["participant_id"])
        agreement = agreements[0] if agreements else None
        if agreement:
            negotiation = edc_consumer_client.get_agreement_negotiation(agreement["agreement_id"])
        elif asset["negotiation_id"]:  # a negotiation we started that has no agreement yet
            negotiation = edc_consumer_client.get_negotiation(asset["negotiation_id"])
            if negotiation is None:  # the connector does not know it any more: forget it
                store.clear_negotiation(slug, asset_id)
    except HTTPException:
        pass  # the contract lookup must not stop the transfer lookup

    transfers = edc_consumer_client.find_transfers(asset_id, tb["dsp_url"])
    chosen = edc_consumer_client.best_transfer(transfers)

    agreement_id = (agreement or {}).get("agreement_id") or (chosen or {}).get("contract_id") or None
    if negotiation or agreement_id:
        store.record_contract(slug, asset_id, (negotiation or {}).get("negotiation_id"),
                              (negotiation or {}).get("state"), agreement_id)
    asset = store.record_transfer(slug, asset_id, chosen)
    store.audit(slug, _actor(claims), "find-transfer",
                f"{asset_id}: contract {agreement_id or 'none'}, transfer "
                f"{chosen['state'] if chosen else 'none'} ({len(transfers)} found)")
    return {"asset": asset, "transfers": transfers, "active": bool(chosen and chosen["active"]),
            "agreement": _contract_info(asset)}


@router.post("/{slug}/assets/{asset_id}/negotiate")
def negotiate_asset(slug: str, asset_id: str, claims: dict = Depends(require_testbed_admin)):
    """Negotiate a contract for the asset's offer through the central connector and store the agreement.

    Waits up to ~30 s for the negotiation to finish. If it is still running after that, it stays
    'negotiating' and "Find transfer" refreshes it later. Does nothing if the asset already has a
    finalized agreement, and continues a negotiation already in progress instead of starting another.
    """
    tb = _get(slug)
    asset = _asset(slug, asset_id)
    if asset["contract_agreement_id"] and asset["negotiation_state"] == "FINALIZED":
        return {"result": "already_agreed", "asset": asset, "agreement": _contract_info(asset)}
    if not asset["present"] or not asset["offer_id"]:
        raise HTTPException(status_code=409, detail="The testbed is not offering this asset right now. Run Find asset first.")

    negotiation_id = asset["negotiation_id"]
    if negotiation_id:
        known = edc_consumer_client.get_negotiation(negotiation_id)
        if known is None:  # stale id from an earlier attempt: the connector no longer has it
            store.clear_negotiation(slug, asset_id)
            negotiation_id = None
        elif known["state"] in edc_consumer_client.NEGOTIATION_DONE:  # it finished since we last looked
            asset = store.record_contract(slug, asset_id, negotiation_id, known["state"], known["agreement_id"])
            return {"result": "already_agreed", "asset": asset, "agreement": _contract_info(asset)}
        elif known["state"] in edc_consumer_client.NEGOTIATION_FAILED:
            negotiation_id = None  # ended without an agreement: negotiate again
    if not negotiation_id:
        negotiation_id = edc_consumer_client.start_negotiation(
            tb["dsp_url"], tb["participant_id"], asset_id, asset["offer_id"])
        store.record_contract(slug, asset_id, negotiation_id, "REQUESTING", None)

    outcome = edc_consumer_client.wait_negotiation(negotiation_id)
    finalized = outcome["state"] in edc_consumer_client.NEGOTIATION_DONE
    if outcome["state"] == "NOT_FOUND":
        store.clear_negotiation(slug, asset_id)
        asset = _asset(slug, asset_id)
    else:
        asset = store.record_contract(slug, asset_id, outcome["negotiation_id"] or negotiation_id, outcome["state"],
                                      outcome["agreement_id"] if finalized else None)
    result = "agreed" if finalized else "failed" if outcome["state"] in edc_consumer_client.NEGOTIATION_FAILED else "in_progress"
    store.audit(slug, _actor(claims), "negotiate", f"{asset_id}: {outcome['state']}")
    message = None
    if outcome["state"] == "NOT_FOUND":
        message = (f"The central connector accepted negotiation {negotiation_id} but then did not know it. "
                   "Check the connector's log and that the management URL points at the same connector that "
                   "received the request.")
    elif outcome["state"] in edc_consumer_client.NEGOTIATION_FAILED:
        message = "The negotiation was terminated by the testbed's connector or the central one. Check their logs."
    return {"result": result, "asset": asset, "agreement": _contract_info(asset), "message": message}


@router.post("/{slug}/assets/{asset_id}/transfers/start")
def start_asset_transfer(slug: str, asset_id: str, claims: dict = Depends(require_testbed_admin)):
    """Start the PiveauData PUSH transfer for an asset with a finalized contract, using the testbed's own
    scoped Data Lake key. Refuses to start a second transfer while one is running."""
    tb = _get(slug)
    asset = _asset(slug, asset_id)
    creds = store.get_s3_credentials(slug)
    if not creds:
        raise HTTPException(status_code=409, detail="This testbed has no Data Lake key yet. Run provisioning first.")
    if not asset["contract_agreement_id"] or asset["negotiation_state"] not in ("FINALIZED", None):
        raise HTTPException(status_code=409, detail="No finalized contract for this asset. Negotiate a contract first.")
    if not pdc.PIVEAU_HUB_URL or not pdc.PIVEAU_API_KEY:
        raise HTTPException(status_code=503, detail="PIVEAU_HUB_URL or PIVEAU_API_KEY not configured")
    if not DATALAKE_PUBLIC_ENDPOINT_URL:
        raise HTTPException(status_code=503, detail="DATASPACE_S3_ENDPOINT_URL not configured")

    running = edc_consumer_client.best_transfer(edc_consumer_client.find_transfers(asset_id, tb["dsp_url"]))
    if running and running["active"]:
        asset = store.record_transfer(slug, asset_id, running)
        return {"result": "already_running", "asset": asset, "transfer_id": running["transfer_id"], "state": running["state"]}

    transfer_id = edc_consumer_client.start_transfer(
        dsp_url=tb["dsp_url"], provider_id=tb["participant_id"], asset_id=asset_id,
        agreement_id=asset["contract_agreement_id"], endpoint=DATALAKE_PUBLIC_ENDPOINT_URL, bucket=tb["bucket"],
        access_key=creds[0], secret_key=creds[1],
        piveau_url=f"{pdc.PIVEAU_HUB_URL.rstrip('/')}/datasets", piveau_api_key=pdc.PIVEAU_API_KEY)

    state = "REQUESTED"
    deadline = time.monotonic() + 15  # usually STARTED within a few seconds
    while time.monotonic() < deadline:
        state = edc_consumer_client.get_transfer_state(transfer_id) or state
        if state == "STARTED" or state in edc_consumer_client.ENDED_STATES:
            break
        time.sleep(1)
    asset = store.record_transfer(slug, asset_id, {
        "transfer_id": transfer_id, "state": state, "active": state == "STARTED",
        "contract_id": asset["contract_agreement_id"]})
    store.audit(slug, _actor(claims), "start-transfer", f"{asset_id}: {transfer_id} {state}")
    return {"result": "started" if state == "STARTED" else "in_progress", "asset": asset,
            "transfer_id": transfer_id, "state": state}


@router.get("/{slug}/audit")
def get_audit(slug: str):
    _get(slug)
    return {"entries": store.audit_log(slug)}


@router.post("/{slug}/provision")
def provision(slug: str, claims: dict = Depends(require_testbed_admin)):
    """Run (or re-run) the central provisioning steps. Safe to repeat.

    Returns the one-time data-lake secret only if it was created by this call;
    it is stored encrypted for the transfer step and is never returned again.
    """
    tb = _get(slug)
    steps = dict(tb["steps"])
    issued: dict | None = None

    def record(name: str, fn):
        try:
            result = fn()
            steps[name] = {"status": "ok", "detail": result, "at": _now()}
        except HTTPException as e:
            steps[name] = {"status": "failed", "detail": e.detail, "at": _now()}

    record("bucket", lambda: datalake_admin.ensure_bucket(tb["bucket"]))
    record("catalogue", lambda: piveau_catalogue_client.ensure_catalogue(
        tb["catalogue_id"], f"{tb['name']} testbed datasets",
        f"Datasets contributed by the {tb['name']} testbed to the 6G-DALI Data Space.",
        tb["organisation"] or tb["name"]))

    if steps["bucket"]["status"] != "ok":
        steps["credentials"] = {"status": "skipped", "detail": "bucket not ready", "at": _now()}
    elif store.get_s3_credentials(slug):
        steps["credentials"] = {"status": "ok", "detail": "exists", "at": _now()}
    else:
        def make_credentials():
            nonlocal issued
            access_key, secret = datalake_admin.create_scoped_user(slug, tb["bucket"])
            store.set_s3_credentials(slug, access_key, secret)
            issued = {"access_key": access_key, "secret_key": secret}
            return "created"
        record("credentials", make_credentials)

    all_ok = all(steps[s]["status"] == "ok" for s in ("bucket", "catalogue", "credentials"))
    updated = store.update_testbed(slug, steps=steps, status="provisioned" if all_ok else tb["status"])
    store.audit(slug, _actor(claims), "provision", "ok" if all_ok else "partial")
    return {
        "testbed": updated,
        "issued_credentials": issued,
        # Not automated yet: the asset monitor takes its bucket list from its own env.
        "followups": [
            f"Add '{tb['bucket']}' to MONITOR_BUCKETS of the s3-asset-monitor service and restart it "
            "(until the registry feeds it directly)."
        ] if all_ok else [],
    }


@router.post("/{slug}/credentials/rotate")
def rotate_credentials(slug: str, claims: dict = Depends(require_testbed_admin)):
    """Issue a new scoped data-lake key and remove the old one. The new secret is returned once."""
    tb = _get(slug)
    old = store.get_s3_credentials(slug)
    access_key, secret = datalake_admin.create_scoped_user(slug, tb["bucket"])
    store.set_s3_credentials(slug, access_key, secret)
    if old:
        datalake_admin.remove_user(old[0])
    store.audit(slug, _actor(claims), "rotate-credentials", access_key)
    return {"access_key": access_key, "secret_key": secret,
            "note": "A running transfer keeps using the old key until it is restarted."}


@router.get("/{slug}/bundle")
def download_bundle(slug: str, claims: dict = Depends(require_testbed_admin)):
    tb = _get(slug)
    store.audit(slug, _actor(claims), "download-bundle")
    return Response(
        content=build_bundle(tb), media_type="application/zip",
        headers={"Content-Disposition": f'attachment; filename="connector-{slug}.zip"'},
    )


@router.get("/{slug}/deletion-preview")
def deletion_preview(slug: str):
    """What deregistering this testbed could also delete: the datasets in its catalogue, the objects in its
    bucket, and whether a transfer is running (it fails once the testbed's key is removed). Each part is
    reported separately, so one unreachable system does not hide the others."""
    tb = _get(slug)
    preview = {
        "slug": slug, "bucket": tb["bucket"], "catalogue_id": tb["catalogue_id"],
        "running_transfers": [a["asset_id"] for a in store.list_assets(slug) if a["transfer_state"] == "STARTED"],
    }
    try:
        preview["datasets"] = piveau_catalogue_client.count_datasets(tb["catalogue_id"])
    except HTTPException as e:
        preview["datasets"], preview["datasets_error"] = None, e.detail
    try:
        objects, truncated = datalake_admin.count_objects(tb["bucket"])
        preview["objects"], preview["objects_truncated"] = objects, truncated
    except HTTPException as e:
        preview["objects"], preview["objects_error"] = None, e.detail
    return preview


@router.delete("/{slug}")
def deregister(slug: str, delete_bucket: bool = False, delete_catalogue: bool = False, confirm: str | None = None,
               claims: dict = Depends(require_testbed_admin)):
    """Disable the testbed's data-lake key and remove it from the registry.

    By default its bucket and catalogue (and the data in them) are left untouched. `delete_catalogue` also
    deletes the catalogue in piveau (with the datasets in it) and `delete_bucket` empties and deletes the
    data-lake bucket. Both are irreversible, so they need `confirm=<slug>`.

    The registry entry is removed only when every requested step succeeded; otherwise it stays, so the
    request can be repeated (the steps are safe to repeat) and nothing is left unmanaged.
    """
    tb = _get(slug)
    if (delete_bucket or delete_catalogue) and confirm != slug:
        raise HTTPException(status_code=409, detail=f"Deleting the bucket or the catalogue is irreversible: pass confirm={slug}")

    results: dict = {}
    failures: list[str] = []

    creds = store.get_s3_credentials(slug)
    if creds:
        try:
            datalake_admin.remove_user(creds[0])
            store.set_s3_credentials(slug, None, None)
            results["key"] = {"status": "removed"}
        except HTTPException as e:
            results["key"] = {"status": "failed", "detail": e.detail}
            failures.append(f"Data Lake key: {e.detail}")
    else:
        results["key"] = {"status": "none"}
    if not failures:
        datalake_admin.remove_policy(slug)

    for requested, name, step in (
        (delete_catalogue, "catalogue", lambda: piveau_catalogue_client.delete_catalogue(tb["catalogue_id"])),
        (delete_bucket, "bucket", lambda: datalake_admin.delete_bucket(tb["bucket"])),
    ):
        if not requested:
            continue
        try:
            results[name] = step()
        except HTTPException as e:
            results[name] = {"status": "failed", "detail": e.detail}
            failures.append(f"{name}: {e.detail}")

    store.audit(slug, _actor(claims), "deregister",
                f"bucket={delete_bucket} catalogue={delete_catalogue} " + ("failed" if failures else "ok"))
    if failures:
        raise HTTPException(status_code=502, detail="Deregistration is incomplete and the testbed stays registered; "
                            "repeat it to retry. " + "; ".join(failures))
    store.delete_testbed(slug)

    followups = []
    if delete_bucket:
        followups.append(f"Remove '{tb['bucket']}' from MONITOR_BUCKETS of the s3-asset-monitor service and restart it.")
    if delete_bucket or delete_catalogue:
        followups.append("EDC assets that were registered on the central connector for the deleted files are not removed.")
    return {"slug": slug, "status": "deregistered", "results": results, "followups": followups}
