"""
Testbed registry: the list of testbeds in the Data Space and the provisioning
that onboarding one needs (identity, Data Lake bucket + scoped key, piveau
catalogue, connector bundle). Admin-only: see auth.require_testbed_admin.

Provisioning is a series of idempotent steps, each recorded in `steps`, so a
failure can be fixed and retried without redoing (or duplicating) the others.
Connecting the connector and managing its transfers/assets come in a later phase.
"""

import re
from datetime import datetime, timezone

from fastapi import APIRouter, Depends, HTTPException, Response
from pydantic import BaseModel, Field

import datalake_admin
import piveau_catalogue_client
import testbed_store as store
from auth import require_testbed_admin
from config import TESTBED_BUCKET_PREFIX, TESTBED_DOMAIN_SUFFIX
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


@router.delete("/{slug}")
def deregister(slug: str, claims: dict = Depends(require_testbed_admin)):
    """Disable the testbed's data-lake key and remove it from the registry.
    Its bucket and catalogue (and the data in them) are left untouched."""
    _get(slug)
    creds = store.get_s3_credentials(slug)
    if creds:
        datalake_admin.remove_user(creds[0])
    store.audit(slug, _actor(claims), "deregister")
    store.delete_testbed(slug)
    return {"slug": slug, "status": "deregistered"}
