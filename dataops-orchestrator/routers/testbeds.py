"""
Testbed registry: the list of testbeds in the Data Space and the provisioning
that onboarding one needs (identity, Data Lake bucket + scoped key, piveau
catalogue, connector bundle). Admins manage every testbed; a testbed's owners (members of its
Keycloak group) see and work with that testbed only: see auth.py.

Provisioning is a series of idempotent steps, each recorded in `steps`, so a
failure can be fixed and retried without redoing (or duplicating) the others.

A testbed either runs its own connector (hosting 'own'), or is hosted by the facility connector (hosting
'facility'): it gets an upload bucket in the Data Lake and a key for it, and the facility connector offers that
bucket as an asset and transfers it, to itself, into the testbed's Data Lake bucket. Everything after the asset
(contract, transfer, the asset monitor registering the files) is the same for both.
"""

import asyncio
import hashlib
import re
import time
from datetime import datetime, timezone
from typing import Literal

from fastapi import APIRouter, Depends, File, Header, HTTPException, Response, UploadFile
from pydantic import BaseModel, Field

import datalake_admin
import edc_client
import edc_consumer_client
import keycloak_admin
import logo_image
import piveau_access
import piveau_dataset_client as pdc
import piveau_catalogue_client
import testbed_datasets
import testbed_store as store
from auth import (
    current_claims, is_testbed_admin, owned_slugs, require_testbed_access, require_testbed_admin, require_testbed_user,
)
from config import (
    DATALAKE_PUBLIC_ENDPOINT_URL,
    FACILITY_DSP_URL,
    FACILITY_PARTICIPANT_ID,
    MONITOR_API_TOKEN,
    ORCHESTRATOR_PUBLIC_URL,
    TESTBED_BUCKET_PREFIX,
    TESTBED_DOMAIN_SUFFIX,
)
from testbed_bundle import build_bundle

# piveau reads made here (the dataset count before a deregistration) go out with the caller's token.
router = APIRouter(prefix="/testbeds", tags=["Testbeds"], dependencies=[Depends(piveau_access.forward_caller_token)])

_SLUG_RE = re.compile(r"^[a-z][a-z0-9]{2,31}$")
_DSP_URL_RE = re.compile(r"^https?://[^\s/]+(/\S*)?$")
_EMAIL_RE = re.compile(r"^[^@\s]+@[^@\s]+\.[^@\s]+$")


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
    logo_url: str | None = Field(default=None, description="Image URL shown as the testbed's catalogue logo (foaf:logo)")
    private: bool = Field(
        default=False,
        description="Create the testbed's catalogue private (dct:accessRights NON_PUBLIC): its datasets are visible to "
                    "its members only. Needs piveau access control; cannot be changed after the catalogue exists.")
    hosted: bool = Field(
        default=False,
        description="The testbed has no connector of its own: it uploads to a bucket in the Data Lake and the facility "
                    "connector transfers that into its Data Lake bucket. participant_id and dsp_url do not apply. "
                    "Fixed at registration.")


class TestbedUpdate(BaseModel):
    dsp_url: str | None = Field(default=None, description="The testbed connector's DSP endpoint (edc.dsp.callback.address)")
    logo_url: str | None = Field(
        default=None,
        description="Image URL for the catalogue logo (foaf:logo). Empty removes the logo; leave the field out to keep it.")


def _now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def _actor(claims: dict) -> str:
    return claims.get("preferred_username") or claims.get("email") or claims.get("sub", "unknown")


def _logo_or_422(value: str | None) -> str | None:
    try:
        return piveau_catalogue_client.clean_logo_url(value)
    except ValueError as e:
        raise HTTPException(status_code=422, detail=str(e))


def catalogue_text(name: str) -> tuple[str, str]:
    """Title and description of a testbed's catalogue. Testbeds are often named "X Testbed", so "testbed" is added
    only when the name lacks it."""
    label = name.strip()
    if not label.lower().endswith("testbed"):
        label += " Testbed"
    return f"{label} Catalogue", f"Datasets contributed by the {label} to the 6G-DALI Data Space."


def _get(slug: str) -> dict:
    tb = store.get_testbed(slug)
    if not tb:
        raise HTTPException(status_code=404, detail=f"Testbed '{slug}' not found")
    return tb


def _hosted(tb: dict) -> bool:
    return tb["hosting"] == "facility"


def _provider(tb: dict) -> str:
    """The participant id of the connector that offers the testbed's asset: its own, or the facility's."""
    return FACILITY_PARTICIPANT_ID if _hosted(tb) else tb["participant_id"]


def upload_asset_id(tb: dict) -> str:
    """A hosted testbed's single asset on the facility connector: named after its upload bucket."""
    return tb["upload_bucket"]


@router.get("")
def list_testbeds(claims: dict = Depends(require_testbed_user)):
    items = store.list_testbeds()
    if not is_testbed_admin(claims):
        mine = owned_slugs(claims)
        items = [t for t in items if t["slug"] in mine or _is_requester(t, claims)]
    # The assets come with each testbed: the list shows how far each has got (asset, contract, transfer).
    assets = store.list_all_assets()
    items = [{**t, "assets": assets.get(t["slug"], [])} for t in items]
    return {"testbeds": items, "total": len(items)}


def _testbed_values(body: TestbedCreate) -> dict:
    """Validate a registration (or a request for one) and derive the registry row's identity from it."""
    if not _SLUG_RE.match(body.slug):
        raise HTTPException(status_code=422, detail="slug must be 3-32 chars: lowercase letters and digits only, starting with a letter")
    bucket = body.bucket or f"{TESTBED_BUCKET_PREFIX}{body.slug}"
    if not datalake_admin.valid_bucket_name(bucket):
        raise HTTPException(status_code=422, detail=f"'{bucket}' is not a valid bucket name (3-63 chars, lowercase, digits, '.', '-')")
    upload_bucket = None
    if body.hosted:
        if body.participant_id or body.dsp_url:
            raise HTTPException(status_code=422, detail="A hosted testbed has no connector of its own: leave "
                                                        "participant_id and dsp_url out")
        upload_bucket = f"{bucket}-upload"
        if not datalake_admin.valid_bucket_name(upload_bucket):
            raise HTTPException(status_code=422, detail=f"The upload bucket '{upload_bucket}' would be too long: "
                                                        "use a shorter bucket name")
    # A hosted testbed's participant id only has to be unique here: the facility connector is the provider.
    participant_id = body.participant_id or (f"hosted-{body.slug}" if body.hosted else f"provider-{body.slug}")
    prefix = body.experiment_prefix or bucket
    logo_url = _logo_or_422(body.logo_url)
    if body.private and not piveau_access.enabled():
        # On piveau without access control a NON_PUBLIC catalogue is only a label: its datasets stay public.
        raise HTTPException(status_code=422, detail="A private catalogue needs piveau access control "
                                                    "(PIVEAU_ACCESS_CONTROL), which is not enabled on this deployment")
    return {
        "slug": body.slug, "name": body.name, "organisation": body.organisation,
        "contact_email": body.contact_email, "participant_id": participant_id,
        "ids_id": f"urn:connector:{participant_id}", "experiment_prefix": prefix,
        "bucket": bucket, "catalogue_id": bucket,
        "dsp_url": FACILITY_DSP_URL if body.hosted
        else body.dsp_url or f"https://edc.{body.slug}.{TESTBED_DOMAIN_SUFFIX}/protocol",
        "produced_by_iri": body.produced_by_iri, "logo_url": logo_url,
        "visibility": "private" if body.private else "public",
        "hosting": "facility" if body.hosted else "own", "upload_bucket": upload_bucket,
    }


@router.post("", status_code=201)
def register_testbed(body: TestbedCreate, claims: dict = Depends(require_testbed_admin)):
    return store.create_testbed(_testbed_values(body), _actor(claims))


# --- requests: a user asks for a testbed, an administrator approves it ------------------------------------------------
# A request is a testbed row in status 'requested'. It reserves the slug and identity, and nothing is provisioned. The
# requester sees it (and only it) through the user that made it; approving makes the requester the testbed's owner
# (a member of its Keycloak group), enables it and provisions it. A refused request stays as 'rejected', with the
# reason, until the requester withdraws it.

MAX_PENDING_REQUESTS = 3
REQUEST_STATUSES = ("requested", "rejected")


class TestbedRequest(BaseModel):
    slug: str = Field(description="Short lowercase id the testbed would be known by, e.g. 'kul'")
    name: str
    organisation: str | None = None
    contact_email: str | None = Field(default=None, description="Operator contact; defaults to the requester's e-mail")
    dsp_url: str | None = Field(default=None, description="The testbed connector's DSP endpoint (own connector only)")
    logo_url: str | None = None
    private: bool = False
    hosted: bool = False
    note: str | None = Field(default=None, max_length=2000, description="What the testbed is and why it is needed")


class Decision(BaseModel):
    note: str | None = Field(default=None, max_length=2000, description="Shown to the requester")


def _is_requester(tb: dict, claims: dict) -> bool:
    """The user who asked for the testbed, while it is still a request. Once approved, ownership is the group."""
    return tb["status"] in REQUEST_STATUSES and bool(claims.get("sub")) and tb["requested_by_sub"] == claims["sub"]


@router.post("/requests", status_code=201)
def request_testbed(body: TestbedRequest, claims: dict = Depends(require_testbed_user)):
    """Ask for a testbed to be registered. Needs no role (administrators may too); an administrator decides on it."""
    sub, email = claims.get("sub"), (claims.get("email") or "").strip()
    if not sub:
        raise HTTPException(status_code=401, detail="The token has no subject, so the request cannot be tied to you")
    pending = [t for t in store.list_testbeds() if t["status"] == "requested" and t["requested_by_sub"] == sub]
    if len(pending) >= MAX_PENDING_REQUESTS:
        raise HTTPException(status_code=429, detail=f"You already have {len(pending)} requests waiting for a decision")
    values = _testbed_values(TestbedCreate(
        slug=body.slug, name=body.name.strip(), organisation=body.organisation, contact_email=body.contact_email,
        dsp_url=body.dsp_url, logo_url=body.logo_url, private=body.private, hosted=body.hosted))
    values.update(
        status="requested", requested_by_sub=sub, requested_by_email=email or None,
        request_note=(body.note or "").strip() or None, contact_email=(body.contact_email or "").strip() or email or None)
    return store.create_testbed(values, _actor(claims))


@router.post("/{slug}/approve")
def approve_request(slug: str, body: Decision | None = None, claims: dict = Depends(require_testbed_admin)):
    """Enable a requested testbed: make the requester its owner, then provision it (as 'Provision' would).

    The requester is added to the testbed's group by their Keycloak user id (the `sub` of the token they asked with),
    first and, if Keycloak refuses, nothing changes, so the request can be approved again. Provisioning problems
    do not undo the approval: the testbed is enabled and its steps show what to retry.
    """
    tb = _get(slug)
    if tb["status"] != "requested":
        raise HTTPException(status_code=409, detail=f"Testbed '{slug}' is not waiting for approval (it is {tb['status']})")
    who = tb["requested_by_email"] or tb["created_by"] or tb["requested_by_sub"]
    followups: list[str] = []
    if keycloak_admin.configured():
        keycloak_admin.add_member_by_id(slug, tb["requested_by_sub"])
    else:
        followups.append(f"KEYCLOAK_ADMIN_CLIENT_ID/_SECRET not set: add {who} to the Keycloak "
                         f"group for '{slug}' by hand so they can manage it")
    store.update_testbed(slug, status="draft", decision_note=((body.note if body else None) or "").strip() or None)
    store.audit(slug, _actor(claims), "approve", who or "")
    try:
        provisioned = provision(slug, claims)
    except HTTPException as e:
        return {"testbed": _get(slug), "provision": None, "followups": [*followups, f"Provisioning failed: {e.detail}"]}
    provisioned["followups"] = [*followups, *provisioned["followups"]]
    return provisioned


@router.post("/{slug}/reject")
def reject_request(slug: str, body: Decision | None = None, claims: dict = Depends(require_testbed_admin)):
    tb = _get(slug)
    if tb["status"] != "requested":
        raise HTTPException(status_code=409, detail=f"Testbed '{slug}' is not waiting for approval (it is {tb['status']})")
    note = ((body.note if body else None) or "").strip() or None
    updated = store.update_testbed(slug, status="rejected", decision_note=note)
    store.audit(slug, _actor(claims), "reject", (note or "")[:200])
    return updated


@router.delete("/{slug}/request")
def withdraw_request(slug: str, claims: dict = Depends(current_claims)):
    """Remove a request that has not been approved, freeing its slug: by whoever asked for it, or an administrator."""
    tb = store.get_testbed(slug)
    if tb is None and is_testbed_admin(claims):
        raise HTTPException(status_code=404, detail=f"Testbed '{slug}' not found")
    if tb is None or not (is_testbed_admin(claims) or _is_requester(tb, claims)):
        raise HTTPException(status_code=403, detail=f"No access to testbed '{slug}'")
    if tb["status"] not in REQUEST_STATUSES:
        raise HTTPException(status_code=409, detail=f"Testbed '{slug}' is already approved: deregister it instead")
    store.audit(slug, _actor(claims), "withdraw-request", tb["status"])
    store.delete_testbed(slug)
    return {"slug": slug, "status": "withdrawn"}


@router.get("/{slug}")
def get_testbed(slug: str, claims: dict = Depends(current_claims)):
    """One testbed: for an administrator, an owner, or the user who asked for it while it is still a request."""
    if is_testbed_admin(claims) or slug in owned_slugs(claims):
        return _get(slug)
    tb = store.get_testbed(slug)
    if tb is None or not _is_requester(tb, claims):
        raise HTTPException(status_code=403, detail=f"No access to testbed '{slug}'")
    return tb


@router.patch("/{slug}")
def update_testbed(slug: str, body: TestbedUpdate, claims: dict = Depends(require_testbed_admin)):
    """Change the testbed's DSP endpoint and/or catalogue logo; fields left out are kept.

    A new DSP endpoint means the connector is no longer known to be reachable, so its 'connector' step is
    cleared until the next asset discovery succeeds. A logo is saved and then written to the testbed's
    catalogue in piveau (also when it is unchanged, which repairs a catalogue that missed an earlier update);
    if piveau refuses, the logo stays saved and 'Re-run provisioning' applies it later.
    """
    tb = _get(slug)
    changes = body.model_fields_set
    if "dsp_url" in changes and body.dsp_url is None:
        raise HTTPException(status_code=422, detail="dsp_url cannot be empty")
    if "dsp_url" in changes and _hosted(tb):
        raise HTTPException(status_code=422, detail="A hosted testbed uses the facility connector: it has no DSP endpoint of its own")
    fields: dict = {}
    if "dsp_url" in changes:
        dsp_url = body.dsp_url.strip()
        if not _DSP_URL_RE.match(dsp_url):
            raise HTTPException(status_code=422, detail="dsp_url must be an http(s) URL, e.g. https://edc.kul.6gdali.eu/protocol")
        if dsp_url != tb["dsp_url"]:
            fields["dsp_url"] = dsp_url
            fields["steps"] = {k: v for k, v in tb["steps"].items() if k != "connector"}
    logo_url = tb["logo_url"]
    if "logo_url" in changes:
        logo_url = _logo_or_422(body.logo_url)
        fields["logo_url"] = logo_url
        if not _is_hosted_logo_url(slug, logo_url):
            store.delete_logo(slug)  # a logo from another address replaces (or removes) the uploaded one
    updated = store.update_testbed(slug, **fields) if fields else tb
    if "dsp_url" in fields:
        store.audit(slug, _actor(claims), "update-dsp-url", f"{tb['dsp_url']} -> {fields['dsp_url']}"[:200])
    if "logo_url" in changes:
        store.audit(slug, _actor(claims), "update-logo", (logo_url or "removed")[:200])
        try:
            piveau_catalogue_client.set_catalogue_logo(tb["catalogue_id"], logo_url)
        except HTTPException as e:
            raise HTTPException(status_code=e.status_code,
                                detail=f"The logo is saved, but the catalogue in piveau was not updated: {e.detail}")
    return updated


# --- the testbed's logo as an uploaded file -------------------------------------------------------------------------
# The image is kept here and served from GET /testbeds/<slug>/logo, a public address (the catalogue's visitors load it,
# and a catalogue's logo is public anyway). That address, with the image's hash as ?v= so that a new upload is not
# shadowed by a cached one, is what becomes the testbed's logo_url and the catalogue's foaf:logo in piveau.

def _logo_base() -> str:
    base = ORCHESTRATOR_PUBLIC_URL.strip().rstrip("/")
    if not base.startswith(("http://", "https://")):
        raise HTTPException(status_code=503, detail="ORCHESTRATOR_PUBLIC_URL is not set (or is not an absolute http(s) "
                                                    "URL) on the orchestrator, so an uploaded logo has no public "
                                                    "address; paste a logo URL instead")
    return base


def _is_hosted_logo_url(slug: str, url: str | None) -> bool:
    return bool(url) and bool(ORCHESTRATOR_PUBLIC_URL.strip()) \
        and url.startswith(f"{ORCHESTRATOR_PUBLIC_URL.strip().rstrip('/')}/testbeds/{slug}/logo")


def _set_catalogue_logo(tb: dict, url: str | None) -> None:
    try:
        piveau_catalogue_client.set_catalogue_logo(tb["catalogue_id"], url)
    except HTTPException as e:
        raise HTTPException(status_code=e.status_code,
                            detail=f"The logo is saved, but the catalogue in piveau was not updated: {e.detail}")


@router.post("/{slug}/logo")
def upload_logo(slug: str, file: UploadFile = File(...), claims: dict = Depends(require_testbed_access)):
    """Set the testbed's logo from a file: a PNG, JPEG, WebP or GIF of up to 512 KB, or a PDF of up to 2 MB (judged by
    its bytes).

    A PDF is not kept: its first page, cropped to what is drawn on it, becomes a PNG (browsers do not show a PDF as an
    image), and that PNG is the logo. The file is kept here, its public address becomes the testbed's logo_url and is
    written to its catalogue in piveau. If piveau refuses, the logo stays saved and 'Re-run provisioning' applies it
    later.
    """
    tb = _get(slug)
    base = _logo_base()
    data = file.file.read(logo_image.MAX_PDF_BYTES + 1)
    content_type = logo_image.sniff(data)
    if content_type is None:
        raise HTTPException(status_code=415, detail=f"The logo must be a {logo_image.ALLOWED} file")
    limit = logo_image.MAX_PDF_BYTES if content_type == logo_image.PDF else logo_image.MAX_LOGO_BYTES
    if len(data) > limit:
        raise HTTPException(status_code=413, detail=f"The logo is larger than {limit // 1024} KB")
    from_pdf = content_type == logo_image.PDF
    if from_pdf:
        try:
            data, content_type = logo_image.pdf_to_png(data), "image/png"
        except logo_image.PdfTooLarge as e:
            raise HTTPException(status_code=413, detail=f"The PDF cannot be used as a logo: {e}")
        except logo_image.PdfError as e:
            raise HTTPException(status_code=422, detail=f"The PDF cannot be used as a logo: {e}")
    sha = hashlib.sha256(data).hexdigest()
    url = f"{base}/testbeds/{slug}/logo?v={sha[:12]}"
    store.set_logo(slug, content_type, data, sha)
    updated = store.update_testbed(slug, logo_url=url)
    store.audit(slug, _actor(claims), "upload-logo",
                f"{content_type}, {len(data)} bytes{' (from a PDF)' if from_pdf else ''}"[:200])
    _set_catalogue_logo(tb, url)
    return updated


@router.delete("/{slug}/logo")
def delete_logo(slug: str, claims: dict = Depends(require_testbed_access)):
    """Remove the uploaded logo, from the testbed and from its catalogue in piveau."""
    tb = _get(slug)
    if store.get_logo(slug) is None and not _is_hosted_logo_url(slug, tb["logo_url"]):
        raise HTTPException(status_code=404, detail=f"Testbed '{slug}' has no uploaded logo")
    store.delete_logo(slug)
    updated = store.update_testbed(slug, logo_url=None)
    store.audit(slug, _actor(claims), "delete-logo", "")
    _set_catalogue_logo(tb, None)
    return updated


@router.get("/{slug}/logo")
def get_logo(slug: str, if_none_match: str | None = Header(default=None)):
    """The testbed's uploaded logo. Public: no token, the catalogue's visitors load it."""
    logo = store.get_logo(slug)
    if logo is None:
        raise HTTPException(status_code=404, detail=f"Testbed '{slug}' has no uploaded logo")
    etag = f'"{logo["sha256"][:16]}"'
    headers = {
        "ETag": etag,
        "Cache-Control": "public, max-age=3600",
        "X-Content-Type-Options": "nosniff",
        # An image shown in a page; opened on its own it has nothing to load or run.
        "Content-Security-Policy": "default-src 'none'; sandbox",
        "Cross-Origin-Resource-Policy": "cross-origin",
    }
    if if_none_match == etag:
        return Response(status_code=304, headers=headers)
    return Response(content=logo["data"], media_type=logo["content_type"], headers=headers)


@router.get("/{slug}/assets", dependencies=[Depends(require_testbed_access)])
def list_assets(slug: str):
    _get(slug)
    assets = store.list_assets(slug)
    return {"assets": assets, "total": len(assets)}


@router.post("/{slug}/assets/discover")
def discover_assets(slug: str, claims: dict = Depends(require_testbed_access)):
    """Ask the testbed's connector, through the central connector, what it offers, and store each offered
    asset. These stored assets are what contract negotiation and transfers are started for later.

    A successful catalogue request also proves the connector is reachable, so it is recorded as the
    'connector' step; a failure is recorded there too and returned as an error.
    """
    tb = _get(slug)
    steps = dict(tb["steps"])
    try:
        offered = edc_consumer_client.fetch_catalog(tb["dsp_url"], _provider(tb))
        if _hosted(tb):  # the facility connector offers every testbed's and every distribution's asset
            offered = [a for a in offered if a["asset_id"] == upload_asset_id(tb)]
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
def find_transfers(slug: str, asset_id: str, claims: dict = Depends(require_testbed_access)):
    """Look on the central connector for this asset's contract and its transfers, and store what is found
    on the asset: the agreement (and the negotiation behind it) and the transfer worth tracking (a running
    one if there is one)."""
    tb = _get(slug)
    asset = _asset(slug, asset_id)

    agreement = negotiation = None
    try:
        agreements = edc_consumer_client.find_agreements(asset_id, _provider(tb))
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
def negotiate_asset(slug: str, asset_id: str, claims: dict = Depends(require_testbed_access)):
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
            tb["dsp_url"], _provider(tb), asset_id, asset["offer_id"])
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
def start_asset_transfer(slug: str, asset_id: str, claims: dict = Depends(require_testbed_access)):
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
        dsp_url=tb["dsp_url"], provider_id=_provider(tb), asset_id=asset_id,
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


@router.post("/{slug}/assets/{asset_id}/transfers/terminate")
def terminate_asset_transfer(slug: str, asset_id: str, claims: dict = Depends(require_testbed_access)):
    """End this asset's transfers that have not ended yet, on the central connector, so a new one can be started with
    the same contract (Start transfer): e.g. after the testbed's connector, or the facility connector, was redeployed
    with a fix. Waits up to ~15 s for them to end."""
    tb = _get(slug)
    _asset(slug, asset_id)
    transfers = edc_consumer_client.find_transfers(asset_id, tb["dsp_url"])
    live = {t["transfer_id"] for t in transfers if t["state"] not in edc_consumer_client.ENDED_STATES}
    if not live:
        asset = store.record_transfer(slug, asset_id, edc_consumer_client.best_transfer(transfers))
        return {"result": "not_running", "asset": asset, "terminated": [], "message": None}

    reason = f"Terminated from DataOps by {_actor(claims)}"
    refused = [t for t in sorted(live) if not edc_consumer_client.terminate_transfer(t, reason)]
    still = live
    deadline = time.monotonic() + 15  # the connector usually ends it within a few seconds
    while True:
        transfers = edc_consumer_client.find_transfers(asset_id, tb["dsp_url"])
        still = {t["transfer_id"] for t in transfers
                 if t["transfer_id"] in live and t["state"] not in edc_consumer_client.ENDED_STATES}
        if not still or time.monotonic() >= deadline:
            break
        time.sleep(1)
    asset = store.record_transfer(slug, asset_id, edc_consumer_client.best_transfer(transfers))
    ended = sorted(live - still)
    store.audit(slug, _actor(claims), "terminate-transfer",
                f"{asset_id}: ended {', '.join(ended) or 'none'}" + (f"; still running {', '.join(sorted(still))}" if still else ""))
    message = None
    if refused:
        message = (f"The central connector refused to terminate {', '.join(refused)}. Check its log; "
                   "Find transfer shows the current state.")
    elif still:
        message = "Termination was requested but has not finished yet. Use Find transfer to refresh."
    return {"result": "terminating" if still else "terminated", "asset": asset, "terminated": ended, "message": message}


@router.get("/{slug}/audit", dependencies=[Depends(require_testbed_access)])
def get_audit(slug: str):
    _get(slug)
    return {"entries": store.audit_log(slug)}


class MemberAdd(BaseModel):
    email: str = Field(description="E-mail address of an existing Keycloak user")


@router.get("/{slug}/members")
def list_members(slug: str, claims: dict = Depends(require_testbed_admin)):
    """The members of the testbed's Keycloak group: the people who can use this testbed in the UI."""
    _get(slug)
    keycloak_admin.require_configured()
    members = keycloak_admin.list_members(slug)
    return {"members": members, "total": len(members)}


@router.post("/{slug}/members", status_code=201)
def add_member(slug: str, body: MemberAdd, claims: dict = Depends(require_testbed_admin)):
    _get(slug)
    keycloak_admin.require_configured()
    email = body.email.strip()
    if not _EMAIL_RE.match(email):
        raise HTTPException(status_code=422, detail="Not a valid e-mail address")
    member = keycloak_admin.add_member(slug, email)
    store.audit(slug, _actor(claims), "add-member", email)
    return member


@router.delete("/{slug}/members/{user_id}")
def remove_member(slug: str, user_id: str, claims: dict = Depends(require_testbed_admin)):
    _get(slug)
    keycloak_admin.require_configured()
    keycloak_admin.remove_member(slug, user_id)
    store.audit(slug, _actor(claims), "remove-member", user_id)
    return {"removed": user_id}


class DatasetEdit(BaseModel):
    """Fields of a dataset to change; the ones left out are kept."""
    title: str | None = None
    description: str | None = None
    keywords: list[str] | None = None
    license: str | None = Field(default=None, description="Licence URL; empty removes it")
    access_level: Literal["public", "restricted", "internal"] | None = Field(
        default=None, description="piveau access level: public, restricted (signed-in users), internal (members only)")
    # The fields below are written as the asset monitor writes them from a metadata.json; empty removes one.
    produced_by: str | None = Field(default=None, description="The organisation's GAIA-X participant IRI (gax:producedBy)")
    publisher: str | None = None
    contact_email: str | None = None
    creator: "DatasetCreator | None" = None
    themes: list[str] | None = Field(default=None, description="EU data theme codes, e.g. TECH")
    language: str | None = Field(default=None, description="EU language code, e.g. ENG")
    spatial: str | None = None
    temporal_start: str | None = Field(default=None, description="YYYY-MM-DD; sent together with temporal_end")
    temporal_end: str | None = None
    version: str | None = None
    contains_pii: bool | None = None


class DatasetCreator(BaseModel):
    name: str = ""
    kind: Literal["Person", "Organization"] = "Person"
    email: str = ""
    orcid: str = ""
    affiliation: str = ""


DatasetEdit.model_rebuild()


class DatasetRecord(BaseModel):
    turtle: str = Field(description="The dataset's whole record as Turtle")


@router.get("/{slug}/datasets", dependencies=[Depends(require_testbed_access)])
def list_testbed_datasets(slug: str):
    """The datasets of the testbed's catalogue, private ones included (read with the API key), with what the edit
    form needs and the visibilities each can have."""
    tb = _get(slug)
    datasets = testbed_datasets.list_datasets(tb["catalogue_id"])
    access_control = piveau_access.enabled()
    return {"catalogue_id": tb["catalogue_id"], "access_control": access_control,
            "levels": list(testbed_datasets.allowed_levels(tb["catalogue_id"])) if access_control else [],
            "default_level": piveau_access.default_level(tb["catalogue_id"]) if access_control else None,
            "datasets": datasets, "total": len(datasets)}


@router.get("/{slug}/datasets/{dataset_id}", dependencies=[Depends(require_testbed_access)])
def get_testbed_dataset(slug: str, dataset_id: str):
    """One dataset of the testbed's catalogue, for the edit page, with the visibilities it can have."""
    tb = _get(slug)
    access_control = piveau_access.enabled()
    return {"catalogue_id": tb["catalogue_id"], "access_control": access_control,
            "levels": list(testbed_datasets.allowed_levels(tb["catalogue_id"])) if access_control else [],
            "default_level": piveau_access.default_level(tb["catalogue_id"]) if access_control else None,
            "dataset": testbed_datasets.summary(tb["catalogue_id"], dataset_id)}


@router.patch("/{slug}/datasets/{dataset_id}")
def edit_testbed_dataset(slug: str, dataset_id: str, body: DatasetEdit, claims: dict = Depends(require_testbed_access)):
    tb = _get(slug)
    changes = body.model_dump(exclude_unset=True)
    if not changes:
        raise HTTPException(status_code=422, detail="Nothing to change")
    result = testbed_datasets.update(tb["catalogue_id"], dataset_id, changes)
    store.audit(slug, _actor(claims), "edit-dataset", f"{dataset_id}: {', '.join(sorted(changes))}"[:200])
    return result


@router.get("/{slug}/datasets/{dataset_id}/record", dependencies=[Depends(require_testbed_admin)])
def get_testbed_dataset_record(slug: str, dataset_id: str):
    """The dataset's whole record as Turtle, for the platform administrators' advanced editor."""
    tb = _get(slug)
    return {"turtle": testbed_datasets.get_turtle(tb["catalogue_id"], dataset_id)}


@router.put("/{slug}/datasets/{dataset_id}/record")
def replace_testbed_dataset_record(slug: str, dataset_id: str, body: DatasetRecord,
                                   claims: dict = Depends(require_testbed_admin)):
    """Replace the dataset's whole record (platform administrators only). Its visibility is kept."""
    tb = _get(slug)
    result = testbed_datasets.replace_turtle(tb["catalogue_id"], dataset_id, body.turtle)
    store.audit(slug, _actor(claims), "edit-dataset-record", dataset_id[:200])
    return result


class CatalogueMemberAdd(BaseModel):
    email: str = Field(description="E-mail address of an existing Keycloak user")
    level: Literal["reader", "contributor", "publisher"] = "reader"


class CatalogueMemberLevel(BaseModel):
    level: Literal["reader", "contributor", "publisher"]


# Who can see and edit the datasets of the testbed's catalogue in the Data Space: the catalogue's piveau role groups
# in Keycloak. Managed by the testbed's own administrators as well as the testbed admins: it is their data.

@router.get("/{slug}/catalogue-members", dependencies=[Depends(require_testbed_access)])
def list_catalogue_members(slug: str):
    tb = _get(slug)
    keycloak_admin.require_configured()
    members = keycloak_admin.list_catalogue_members(tb["catalogue_id"])
    return {"catalogue_id": tb["catalogue_id"], "available": members is not None,
            "members": members or [], "total": len(members or [])}


@router.post("/{slug}/catalogue-members", status_code=201)
def add_catalogue_member(slug: str, body: CatalogueMemberAdd, claims: dict = Depends(require_testbed_access)):
    tb = _get(slug)
    keycloak_admin.require_configured()
    email = body.email.strip()
    if not _EMAIL_RE.match(email):
        raise HTTPException(status_code=422, detail="Not a valid e-mail address")
    member = keycloak_admin.set_catalogue_member(tb["catalogue_id"], body.level, email=email)
    store.audit(slug, _actor(claims), "catalogue-access", f"{email}: {body.level}")
    return member


@router.put("/{slug}/catalogue-members/{user_id}")
def set_catalogue_member_level(slug: str, user_id: str, body: CatalogueMemberLevel,
                               claims: dict = Depends(require_testbed_access)):
    tb = _get(slug)
    keycloak_admin.require_configured()
    member = keycloak_admin.set_catalogue_member(tb["catalogue_id"], body.level, user_id=user_id)
    store.audit(slug, _actor(claims), "catalogue-access", f"{member.get('email') or user_id}: {body.level}")
    return member


@router.delete("/{slug}/catalogue-members/{user_id}")
def remove_catalogue_member(slug: str, user_id: str, claims: dict = Depends(require_testbed_access)):
    tb = _get(slug)
    keycloak_admin.require_configured()
    keycloak_admin.remove_catalogue_member(tb["catalogue_id"], user_id)
    store.audit(slug, _actor(claims), "catalogue-access", f"{user_id}: removed")
    return {"removed": user_id}


HOSTED_STEPS = ("upload_bucket", "upload_key", "asset", "transfer")


def _upload_credentials(tb: dict, access_key: str, secret: str) -> dict:
    return {"endpoint": DATALAKE_PUBLIC_ENDPOINT_URL, "bucket": tb["upload_bucket"],
            "access_key": access_key, "secret_key": secret}


def _provision_hosted(slug: str, tb: dict, steps: dict, record, claims: dict) -> dict | None:
    """The steps of a testbed hosted by the facility connector, after the common ones. Returns the upload key when
    this run created it (its secret is shown once and not stored).

    The transfer is started here, not left for later: the connector's bucket source ignores files that are already
    in the bucket when the transfer starts, so it must be running before the testbed's first upload.
    """
    issued: dict | None = None
    record("upload_bucket", lambda: datalake_admin.ensure_bucket(tb["upload_bucket"]))

    if steps["upload_bucket"]["status"] != "ok":
        steps["upload_key"] = {"status": "skipped", "detail": "upload bucket not ready", "at": _now()}
    elif tb["upload_access_key"]:
        steps["upload_key"] = {"status": "ok", "detail": "exists", "at": _now()}
    else:
        def make_upload_key():
            nonlocal issued
            access_key, secret = datalake_admin.create_scoped_user(slug, tb["upload_bucket"], purpose="upload")
            store.set_upload_key(slug, access_key)
            issued = _upload_credentials(tb, access_key, secret)
            return "created"
        record("upload_key", make_upload_key)

    if steps["upload_bucket"]["status"] == "ok":
        record("asset", lambda: edc_client.register_upload_asset(
            upload_asset_id(tb), tb["upload_bucket"], f"{tb['name']} uploads"))
    else:
        steps["asset"] = {"status": "skipped", "detail": "upload bucket not ready", "at": _now()}

    if steps["asset"]["status"] != "ok" or steps["credentials"]["status"] != "ok":
        steps["transfer"] = {"status": "skipped", "detail": "needs the asset and the Data Lake key", "at": _now()}
    else:
        def start_transfer() -> str:
            # The facility connector negotiates with, and then transfers to, itself: the same calls as for a
            # testbed connector, through this router's own endpoints so the asset's record stays in step.
            store.update_testbed(slug, steps=steps)
            discover_assets(slug, claims)
            aid = upload_asset_id(tb)
            if not any(a["asset_id"] == aid and a["present"] for a in store.list_assets(slug)):
                raise HTTPException(status_code=502, detail=f"The facility connector does not offer '{aid}'")
            deal = negotiate_asset(slug, aid, claims)
            if deal["result"] not in ("agreed", "already_agreed"):
                raise HTTPException(status_code=502, detail=deal.get("message") or
                                    f"Contract negotiation is {deal['asset']['negotiation_state']}; re-run provisioning")
            started = start_asset_transfer(slug, aid, claims)
            return {"started": "started", "already_running": "running"}.get(started["result"],
                                                                         f"requested ({started['state']})")
        record("transfer", start_transfer)
        steps.update({k: v for k, v in store.get_testbed(slug)["steps"].items() if k == "connector"})
    return issued


@router.post("/{slug}/provision")
def provision(slug: str, claims: dict = Depends(require_testbed_admin)):
    """Run (or re-run) the central provisioning steps. Safe to repeat.

    Returns the one-time data-lake secret only if it was created by this call;
    it is stored encrypted for the transfer step and is never returned again.
    """
    tb = _get(slug)
    if tb["status"] in REQUEST_STATUSES:
        raise HTTPException(status_code=409, detail=f"Testbed '{slug}' is a request ({tb['status']}): approve it first")
    steps = dict(tb["steps"])
    issued: dict | None = None

    def record(name: str, fn):
        try:
            result = fn()
            steps[name] = {"status": "ok", "detail": result, "at": _now()}
        except HTTPException as e:
            steps[name] = {"status": "failed", "detail": e.detail, "at": _now()}

    record("bucket", lambda: datalake_admin.ensure_bucket(tb["bucket"]))
    def ensure_catalogue() -> str:
        private = tb["visibility"] == "private"
        result = piveau_catalogue_client.ensure_catalogue(
            tb["catalogue_id"], *catalogue_text(tb["name"]),
            tb["organisation"] or tb["name"], tb["logo_url"], private)
        # A catalogue's visibility is fixed when it is created: a testbed registered as private whose catalogue
        # already exists as public would publish everything it uploads, so that is a failed step, not 'exists'.
        if result == "exists" and private and piveau_access.catalogue_level(tb["catalogue_id"]) != "internal":
            raise HTTPException(status_code=409, detail=f"Catalogue '{tb['catalogue_id']}' already exists and is "
                                                        "public; a catalogue's visibility cannot be changed")
        # A catalogue that already existed keeps what it has; a saved logo is applied to it here. Without a
        # saved logo it is left alone, so a logo set directly in piveau is not wiped by provisioning.
        if result == "exists" and tb["logo_url"]:
            logo = piveau_catalogue_client.set_catalogue_logo(tb["catalogue_id"], tb["logo_url"])
            return "exists, logo " + ("already set" if logo == "unchanged" else "set")
        return result

    record("catalogue", ensure_catalogue)

    if keycloak_admin.configured():
        record("access", lambda: keycloak_admin.ensure_group(slug))
    else:
        steps["access"] = {"status": "skipped", "at": _now(),
                           "detail": "KEYCLOAK_ADMIN_CLIENT_ID/_SECRET not set: create the Keycloak group by hand"}

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

    issued_upload: dict | None = None
    if _hosted(tb):
        issued_upload = _provision_hosted(slug, tb, steps, record, claims)

    # `access` only counts when it was attempted: without a Keycloak admin client the group is made by hand
    required = ("bucket", "catalogue", "credentials") + (("access",) if keycloak_admin.configured() else ()) \
        + (HOSTED_STEPS if _hosted(tb) else ())
    all_ok = all(steps[s]["status"] == "ok" for s in required)
    updated = store.update_testbed(slug, steps=steps, status="provisioned" if all_ok else tb["status"])
    store.audit(slug, _actor(claims), "provision", "ok" if all_ok else "partial")
    return {
        "testbed": updated,
        "issued_credentials": issued,
        # A hosted testbed's upload key, returned only when this call created it: hand it to the testbed now.
        "issued_upload_credentials": issued_upload,
        # With MONITOR_API_TOKEN set the asset monitor asks for its buckets (routers/monitor.py); without it, by hand.
        "followups": [
            f"Add '{tb['bucket']}' to MONITOR_BUCKETS of the s3-asset-monitor service and restart it "
            "(or set MONITOR_API_TOKEN here and MONITOR_BUCKETS_TOKEN there, and it follows the registry)."
        ] if all_ok and not MONITOR_API_TOKEN else [],
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


@router.post("/{slug}/upload-credentials/rotate")
def rotate_upload_credentials(slug: str, claims: dict = Depends(require_testbed_access)):
    """A hosted testbed's new upload key, replacing the old one (which stops working). The secret is returned
    once and not stored: when it is lost, rotate again. The running transfer is unaffected (the facility connector
    reads the upload bucket with its own key). Open to the testbed's administrators: they are the ones uploading."""
    tb = _get(slug)
    if not _hosted(tb):
        raise HTTPException(status_code=409, detail="Only a testbed hosted by the facility connector has an upload key")
    if tb["steps"].get("upload_bucket", {}).get("status") != "ok":
        raise HTTPException(status_code=409, detail="The upload bucket is not ready. Run provisioning first.")
    old = tb["upload_access_key"]
    access_key, secret = datalake_admin.create_scoped_user(slug, tb["upload_bucket"], purpose="upload")
    store.set_upload_key(slug, access_key)
    if old:
        datalake_admin.remove_user(old)
    store.audit(slug, _actor(claims), "rotate-upload-key", access_key)
    return _upload_credentials(tb, access_key, secret)


@router.get("/{slug}/bundle")
def download_bundle(slug: str, claims: dict = Depends(require_testbed_access)):
    tb = _get(slug)
    if _hosted(tb):
        raise HTTPException(status_code=409, detail="This testbed is hosted by the facility connector: it has no "
                                                    "connector to deploy. It uploads with its upload key instead.")
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
    if _hosted(tb):
        preview["upload_bucket"] = tb["upload_bucket"]
        try:
            objects, truncated = datalake_admin.count_objects(tb["upload_bucket"])
            preview["upload_objects"], preview["upload_objects_truncated"] = objects, truncated
        except HTTPException as e:
            preview["upload_objects"], preview["upload_objects_error"] = None, e.detail
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

    if _hosted(tb):
        if tb["upload_access_key"]:
            try:
                datalake_admin.remove_user(tb["upload_access_key"])
                store.set_upload_key(slug, None)
                results["upload_key"] = {"status": "removed"}
                datalake_admin.remove_policy(slug, purpose="upload")
            except HTTPException as e:
                results["upload_key"] = {"status": "failed", "detail": e.detail}
                failures.append(f"Upload key: {e.detail}")
        # The facility connector would otherwise keep polling the upload bucket. Both are best effort: the
        # registry entry does not depend on the connector being reachable.
        aid = upload_asset_id(tb)
        running = [a["transfer_id"] for a in store.list_assets(slug)
                   if a["asset_id"] == aid and a["transfer_id"] and a["transfer_state"] == "STARTED"]
        ended = all(edc_consumer_client.terminate_transfer(t, f"testbed {slug} deregistered") for t in running)
        removed = asyncio.run(edc_client.delete_asset(aid))
        results["asset"] = {"status": removed["status"] if ended else "transfer still running",
                            **({"detail": removed["error"]} if "error" in removed else {})}

    for requested, name, step in (
        (delete_catalogue, "catalogue", lambda: piveau_catalogue_client.delete_catalogue(tb["catalogue_id"])),
        (delete_bucket, "bucket", lambda: datalake_admin.delete_bucket(tb["bucket"])),
        (delete_bucket and _hosted(tb), "upload_bucket", lambda: datalake_admin.delete_bucket(tb["upload_bucket"])),
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
    if delete_bucket and not MONITOR_API_TOKEN:
        followups.append(f"Remove '{tb['bucket']}' from MONITOR_BUCKETS of the s3-asset-monitor service and restart it.")
    if delete_bucket or delete_catalogue:
        followups.append("EDC assets that were registered on the central connector for the deleted files are not removed.")
    if _hosted(tb) and results.get("asset", {}).get("status") not in ("deleted", "not_found"):
        followups.append(f"The upload asset '{upload_asset_id(tb)}' on the facility connector could not be removed "
                         f"({results['asset']['status']}): end its transfer and delete the asset on the connector.")
    return {"slug": slug, "status": "deregistered", "results": results, "followups": followups}
