from __future__ import annotations

import json
import os
import time
import uuid
from datetime import datetime, timezone

import requests
from airflow.decorators import task
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.sdk import get_current_context

from dali.utils import (
    DATAOPS_S3_CONN_ID,
    DATASPACE_S3_CONN_ID,
    EDC_CONSUMER_URL,
    EDC_PROVIDER_PROTOCOL_URL,
)

EDC_POLL_INTERVAL = int(os.getenv("EDC_POLL_INTERVAL", "3"))
EDC_POLL_TIMEOUT  = int(os.getenv("EDC_POLL_TIMEOUT", "120"))


@task(multiple_outputs=True)
def download_dataset() -> dict:
    """Resolve the distribution's S3 object from its dali:assetId and download it.

    `asset_id` is what dataops-orchestrator names the uploaded object after
    (see piveau_dataset_client.py's add_distribution and
    routers/datasets.py's add_distribution endpoint) — the object's key is
    "{dataset_id}/{asset_id}.{ext}", but the extension isn't known up front,
    so it's resolved here via an S3 prefix listing rather than fetched from
    piveau (avoiding a round trip and a dependency on dcat:mediaType being
    set correctly). Returns {"content": ..., "asset_title": ...} — the
    basename (with extension) is needed downstream (see run_expectations)."""
    params = get_current_context()["params"]
    dataset_id = params["dataset_id"]
    catalogue_id = params["catalogue_id"]
    asset_id = params["asset_id"]

    hook = S3Hook(aws_conn_id=DATASPACE_S3_CONN_ID)

    conn = hook.get_connection(DATASPACE_S3_CONN_ID)
    print(f"[dali] conn_id={DATASPACE_S3_CONN_ID!r} login={conn.login!r} extra={conn.extra!r}")
    client = hook.get_conn()
    print(f"[dali] resolved endpoint_url={client.meta.endpoint_url!r} region={client.meta.region_name!r} "
          f"addressing_style={client.meta.config.s3.get('addressing_style') if client.meta.config.s3 else None!r}")

    prefix = f"{dataset_id}/{asset_id}."
    keys = hook.list_keys(bucket_name=catalogue_id, prefix=prefix) or []
    if not keys:
        raise FileNotFoundError(f"[dali] no object found under s3://{catalogue_id}/{prefix}*")
    if len(keys) > 1:
        print(f"[dali] multiple objects match prefix {prefix!r}: {keys} — using the first one")
    input_key = keys[0]
    asset_title = input_key.rsplit("/", 1)[-1]
    print(f"[dali] dataset_id={dataset_id} asset_id={asset_id} resolved asset_title={asset_title!r}")
    print(f"[dali] bucket={catalogue_id!r} key={input_key!r}")

    obj = hook.get_key(key=input_key, bucket_name=catalogue_id)
    content = obj.get()["Body"].read().decode("utf-8")
    return {"content": content, "asset_title": asset_title}


# Media types dataops-orchestrator registers on an EDC asset
# (piveau_dataset_client.resolve_media_type), mapped back to the extension
# dali.utils.detect_format keys off. Only the formats validation actually
# supports are listed — anything else falls through to the .csv default.
_EXT_BY_MEDIA_TYPE = {
    "text/csv":                 ".csv",
    "text/tab-separated-values": ".tsv",
    "application/jsonl":        ".jsonl",
    "application/x-ndjson":     ".jsonl",
    "application/json":         ".json",
}


def _local_part(key: str) -> str:
    """Local part of a JSON-LD key, dropping any prefix or IRI namespace:
    'edc:name', 'https://w3id.org/edc/v0.0.1/ns/name' and the dotted flat
    form 'edc.name' all reduce to 'name'. EDC returns asset properties in
    whichever of these shapes the response happened to be compacted to, so
    matching on the local part is the only reliable way to find one."""
    return key.split(":")[-1].split("/")[-1].split(".")[-1].lower()


def _property(entry: dict, name: str) -> str:
    """An asset property from a catalogue entry, matched by local part and
    unwrapped from a {"@value": ...} literal if it is expanded."""
    for key, value in entry.items():
        if _local_part(key) != name:
            continue
        if isinstance(value, dict):
            value = value.get("@value", "")
        if isinstance(value, str) and value.strip():
            return value.strip()
    return ""


def resolve_asset_title(entry: dict, asset_id: str) -> str:
    """The filename to treat this catalogue entry's distribution as.

    Downstream format detection keys off the extension (see
    dali.utils.detect_format), so this has to carry the real one rather than
    a hardcoded guess. In order of preference:
      1. the asset's `name` property — dataops-orchestrator registers it
         from the uploaded file's own filename (see edc_client.register_asset),
         so it is the authoritative title whenever it has an extension;
      2. an extension derived from the asset's `contenttype` property,
         appended to the asset_id;
      3. "{asset_id}.csv", the format the vast majority of distributions are.
    """
    name = _property(entry, "name")
    if name and os.path.splitext(name)[1]:
        print(f"[edc] asset_title {name!r} from the catalogue entry's name property")
        return name

    media_type = _property(entry, "contenttype").split(";")[0].strip().lower()
    ext = _EXT_BY_MEDIA_TYPE.get(media_type)
    if ext:
        print(f"[edc] asset_title from contenttype {media_type!r} -> {ext}")
        return f"{asset_id}{ext}"

    print(f"[edc] no usable name/contenttype on asset {asset_id!r} "
          f"(name={name!r}, contenttype={media_type!r}) — defaulting to .csv")
    return f"{asset_id}.csv"


@task(multiple_outputs=True)
def download_dataset_edc() -> dict:
    """
    Retrieve a distribution from a provider EDC connector into a fixed
    DataOps S3 staging location, then return its content.

    Flow:
        1. Request the provider's catalogue via the DataOps EDC consumer,
           looking the asset up by its dali:assetId
        2. Negotiate a contract for the matched offer
        3. Generate a presigned PUT URL for a freshly, randomly named
           object in the DataOps S3 staging bucket
        4. Initiate the transfer — the provider EDC PUTs directly to the
           presigned URL (no S3 credentials are shared with the provider)
        5. Poll until the transfer is complete
        6. Read the file back from the DataOps S3 staging bucket and return
           its content

    Required params:
        asset_id       The distribution's dali:assetId. This is also the EDC
                       asset's own @id: dataops-orchestrator registers each
                       uploaded distribution under exactly this identifier
                       (see edc_client.register_asset), so piveau and EDC
                       agree on one identifier per distribution.

    Optional params:
        provider_id    Connector ID asserted to the provider during
                       contract negotiation and transfer
                       (default: "daliprovider")

    Returns {"content": ..., "asset_title": ...} — the same shape as
    download_dataset above, so either can feed the validation chain (see
    dali.validation.validate_file_format). asset_title is resolved from the
    catalogue entry's own properties rather than guessed, because downstream
    format detection keys off its extension (see dali.utils.detect_format):
    a distribution can be csv, tsv, jsonl or json, and mislabelling it fails
    the format check before a single expectation runs.

    The staging destination is fixed in code, not derived from DAG params:
        destination_bucket   "6g-dali-dataops"
        destination_key      a randomly generated "{uuid4}{ext}" filename,
                              so concurrent runs never collide on the same
                              object

    The provider EDC connector's protocol (DSP) address is fixed via
    EDC_PROVIDER_PROTOCOL_URL (see dali.utils), not a DAG param — a
    triggering user should not be able to point this DAG at an arbitrary
    connector, and the consumer only ever needs the provider's protocol
    port, never its management or control ports.

    The DataOps EDC consumer connector's MANAGEMENT API (a separate service
    from the provider above) is likewise fixed via EDC_CONSUMER_URL (see
    dali.utils) — every catalog/negotiation/transfer call below is made
    against *our own* connector's management API; the provider is only
    ever addressed indirectly, via counterPartyAddress.

    The DataOps S3 connection is taken from the DATAOPS_S3_CONN_ID env var
    (see dali.utils), not from a DAG param.
    """
    params = get_current_context()["params"]

    provider_url = EDC_PROVIDER_PROTOCOL_URL
    mgmt         = f"{EDC_CONSUMER_URL}/management/v3"
    asset_id     = params["asset_id"]
    if not asset_id:
        raise ValueError("[edc] asset_id is required — it is the EDC asset's @id")

    # ── 1. Request the offer for the specific asset from the provider ─────────
    print(f"[edc] requesting offer for asset '{asset_id}' from {provider_url}")
    cat_resp = requests.post(
        f"{mgmt}/catalog/request",
        json={
            "@context": {"@vocab": "https://w3id.org/edc/v0.0.1/ns/"},
            "counterPartyAddress": f"{provider_url}/protocol",
            "protocol": "dataspace-protocol-http",
            "querySpec": {
                "filterExpression": [{
                    "operandLeft": "https://w3id.org/edc/v0.0.1/ns/id",
                    "operator": "=",
                    "operandRight": asset_id,
                }]
            },
        },
        timeout=30,
    )
    cat_resp.raise_for_status()
    catalog = cat_resp.json()

    datasets = catalog.get("dcat:dataset", [])
    if isinstance(datasets, dict):
        datasets = [datasets]
    if not datasets:
        raise RuntimeError(f"[edc] asset '{asset_id}' not found in provider catalogue")

    entry = datasets[0]
    asset_title = resolve_asset_title(entry, asset_id)

    offers = entry.get("odrl:hasPolicy", [])
    if isinstance(offers, dict):
        offers = [offers]
    if not offers:
        raise RuntimeError(f"[edc] no policy offer found for asset '{asset_id}'")

    offer = offers[0]
    offer_id = offer["@id"]
    print(f"[edc] found offer {offer_id} for asset {asset_id}")

    # ── 2. Initiate contract negotiation ─────────────────────────────────────
    provider_id = params.get("provider_id", "daliprovider")
    neg_resp = requests.post(
        f"{mgmt}/contractnegotiations",
        json={
            "@context": {
                "@vocab": "https://w3id.org/edc/v0.0.1/ns/",
                "odrl":   "http://www.w3.org/ns/odrl/2/",
            },
            "@type":              "ContractRequest",
            "counterPartyAddress": f"{provider_url}/protocol",
            "providerId":          provider_id,
            "protocol":            "dataspace-protocol-http",
            "policy": {
                "@id":              offer_id,
                "@type":            "http://www.w3.org/ns/odrl/2/Offer",
                "odrl:permission":  offer.get("odrl:permission", []),
                "odrl:prohibition": offer.get("odrl:prohibition", []),
                "odrl:obligation":  offer.get("odrl:obligation", []),
                "odrl:target":      {"@id": asset_id},
                "odrl:assigner":    {"@id": provider_id},
            },
        },
        timeout=30,
    )
    neg_resp.raise_for_status()
    neg_id = neg_resp.json()["@id"]
    print(f"[edc] negotiation started: {neg_id}")

    # ── 3. Poll until negotiation is FINALIZED ───────────────────────────────
    agreement_id = None
    deadline = time.time() + EDC_POLL_TIMEOUT
    while time.time() < deadline:
        state_resp = requests.get(f"{mgmt}/contractnegotiations/{neg_id}", timeout=10)
        state_resp.raise_for_status()
        state = state_resp.json()
        neg_state = state.get("state", state.get("edc:state", ""))
        print(f"[edc] negotiation state: {neg_state}")
        if neg_state == "FINALIZED":
            agreement_id = state.get("contractAgreementId") or state.get("edc:contractAgreementId")
            print(f"[edc] agreement: {agreement_id}")
            break
        if neg_state in ("TERMINATED", "ERROR"):
            raise RuntimeError(f"[edc] negotiation failed with state: {neg_state}")
        time.sleep(EDC_POLL_INTERVAL)
    else:
        raise TimeoutError(f"[edc] negotiation did not complete within {EDC_POLL_TIMEOUT}s")

    # ── 4. Generate a presigned PUT URL for the DataOps S3 destination ───────
    hook = S3Hook(aws_conn_id=DATAOPS_S3_CONN_ID)
    s3_client = hook.get_conn()

    destination_bucket = "6g-dali-dataops"
    # Staged under the asset's own extension, so nothing downstream has to
    # re-guess the format from a hardcoded suffix.
    ext = os.path.splitext(asset_title)[1] or ".csv"
    destination_key = f"{uuid.uuid4()}{ext}"

    presigned_put_url = s3_client.generate_presigned_url(
        "put_object",
        Params={"Bucket": destination_bucket, "Key": destination_key},
        ExpiresIn=EDC_POLL_TIMEOUT * 2,
    )
    print(f"[edc] presigned PUT URL generated for s3://{destination_bucket}/{destination_key}")

    # ── 5. Initiate data transfer — provider PUTs to our presigned URL ────────
    xfer_resp = requests.post(
        f"{mgmt}/transferprocesses",
        json={
            "@context":            {"@vocab": "https://w3id.org/edc/v0.0.1/ns/"},
            "@type":               "TransferRequest",
            "counterPartyAddress": f"{provider_url}/protocol",
            "connectorId":         provider_id,
            "protocol":            "dataspace-protocol-http",
            "contractId":          agreement_id,
            "assetId":             asset_id,
            "transferType":        "PresignedHttpData-PUSH",
            "dataDestination": {
                "type":    "PresignedHttpData",
                "baseUrl": presigned_put_url,
                "method":  "PUT",
            },
        },
        timeout=30,
    )
    xfer_resp.raise_for_status()
    xfer_id = xfer_resp.json()["@id"]
    print(f"[edc] transfer started: {xfer_id}")

    # ── 6. Poll until transfer is COMPLETED ──────────────────────────────────
    deadline = time.time() + EDC_POLL_TIMEOUT
    while time.time() < deadline:
        xstate_resp = requests.get(f"{mgmt}/transferprocesses/{xfer_id}", timeout=10)
        xstate_resp.raise_for_status()
        xfer_state = xstate_resp.json().get("state", xstate_resp.json().get("edc:state", ""))
        print(f"[edc] transfer state: {xfer_state}")
        if xfer_state == "COMPLETED":
            break
        if xfer_state in ("TERMINATED", "ERROR"):
            raise RuntimeError(f"[edc] transfer failed with state: {xfer_state}")
        time.sleep(EDC_POLL_INTERVAL)
    else:
        raise TimeoutError(f"[edc] transfer did not complete within {EDC_POLL_TIMEOUT}s")

    # ── 7. Retrieve the transferred file from DataOps S3 ─────────────────────
    print(f"[edc] retrieving {destination_key} from bucket {destination_bucket}")
    obj = hook.get_key(key=destination_key, bucket_name=destination_bucket)
    content = obj.get()["Body"].read().decode("utf-8")
    return {"content": content, "asset_title": asset_title}


@task
def upload_results(report: dict) -> str:
    params = get_current_context()["params"]
    catalogue_id = params["catalogue_id"]
    input_key    = report["input_key"]

    base = os.path.splitext(input_key)[0]
    ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    output_key = f"{base}_{ts}.gx"

    print(f"[dali] upload_results bucket={catalogue_id!r} output_key={output_key!r}")

    hook = S3Hook(aws_conn_id=DATASPACE_S3_CONN_ID)
    hook.load_string(
        string_data=json.dumps(report, indent=2),
        key=output_key,
        bucket_name=catalogue_id,
        replace=True,
    )
    return output_key


# The object-key suffix each of a processing run's artifacts is stored under
# (see dali.processing.run_dataops_pipeline for what produces them). Derived
# from the artifact's name rather than from its filename, which starts with the
# dataset's own stem and may itself contain underscores.
_ARTIFACT_SUFFIXES = {
    "input_csv":        "_raw.csv",
    "output_csv":       "_remediated.csv",
    "report_json":      "_report.json",
    "soft_cleaned_csv": "_soft_cleaned.csv",
}


@task
def upload_artifacts(pipeline: dict) -> dict:
    """Upload a processing run's artifacts next to the distribution they came
    from, and return {name: object key}.

    Keys follow the same convention as upload_results — the distribution's own
    prefix plus a run timestamp — so a processing run's outputs sit beside the
    validation reports for the same distribution and never collide with a
    concurrent run over a different one:

        <dataset_id>/<asset_id>_<timestamp>_raw.csv
        <dataset_id>/<asset_id>_<timestamp>_remediated.csv
        <dataset_id>/<asset_id>_<timestamp>_report.json
        <dataset_id>/<asset_id>_<timestamp>_soft_cleaned.csv

    Files are uploaded from disk (load_file) rather than read into memory: a
    remediated CSV can be far larger than the report, and the whole point of
    the scratch directory is that the frame never has to be held in an XCom.
    """
    params = get_current_context()["params"]
    catalogue_id = params["catalogue_id"]
    dataset_id   = params["dataset_id"]
    asset_id     = params["asset_id"]

    ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    prefix = f"{dataset_id}/{asset_id}_{ts}"

    hook = S3Hook(aws_conn_id=DATASPACE_S3_CONN_ID)
    uploaded: dict[str, str] = {}
    for name, path in (pipeline.get("artifacts") or {}).items():
        key = f"{prefix}{_ARTIFACT_SUFFIXES.get(name, os.path.splitext(path)[1])}"
        print(f"[dali] uploading {name} -> s3://{catalogue_id}/{key}")
        hook.load_file(
            filename=path,
            key=key,
            bucket_name=catalogue_id,
            replace=True,
            gzip=False,
        )
        uploaded[name] = key
    if not uploaded:
        print("[dali] the pipeline produced no artifacts to upload")
    return uploaded
