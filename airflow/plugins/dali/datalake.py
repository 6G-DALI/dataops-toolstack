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


@task
def download_dataset_edc() -> str:
    """
    Retrieve a dataset from the 6G-DALI provider EDC connector into a fixed
    DataOps S3 destination, then return its content as a string.

    Flow:
        1. Request the provider's catalogue via the DataOps EDC consumer,
           looking up the asset by "{dataset_id}/{asset_title}"
        2. Negotiate a contract for the matched offer
        3. Generate a presigned PUT URL for a freshly, randomly named
           object in the DataOps S3 destination bucket
        4. Initiate the transfer — the provider EDC PUTs directly to the
           presigned URL (no S3 credentials are shared with the provider)
        5. Poll until the transfer is complete
        6. Read the file back from the DataOps S3 destination and return
           its content

    Required params:
        dataset_id     Folder/prefix of the asset in the provider's catalogue
        asset_title    Filename of the target object; combined with
                       dataset_id as "{dataset_id}/{asset_title}" to form
                       the asset ID used for the catalogue lookup, contract
                       negotiation, and transfer request

    Optional params:
        provider_id    Connector ID asserted to the provider during
                       contract negotiation and transfer
                       (default: "daliprovider")

    The destination is fixed in code, not derived from DAG params:
        destination_bucket   "6g-dali-dataops"
        destination_key      a randomly generated "{uuid4}.csv" filename,
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
    print("download_dataset_edc()")
    params = get_current_context()["params"]

    # ── Step 1 & 2: catalogue request + contract negotiation ─────────────────
    provider_url = EDC_PROVIDER_PROTOCOL_URL
    dataset_id   = params["dataset_id"]
    mgmt         = f"{EDC_CONSUMER_URL}/management/v3"

    # ── Step 3: presigned PUT URL generation ─────────────────────────────────
    asset_title  = params["asset_title"]
    input_key    = f"{dataset_id}/{asset_title}"

    # ── 1. Request the offer for the specific asset from the provider ─────────
    print(f"[edc] requesting offer for asset '{dataset_id}' from {provider_url}")
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
                    "operandRight": input_key,
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
        raise RuntimeError(f"[edc] asset '{dataset_id}' not found in provider catalogue")

    offers = datasets[0].get("odrl:hasPolicy", [])
    if isinstance(offers, dict):
        offers = [offers]
    if not offers:
        raise RuntimeError(f"[edc] no policy offer found for asset '{dataset_id}'")

    offer = offers[0]
    offer_id = offer["@id"]
    print(f"[edc] found offer {offer_id} for asset {dataset_id}")

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
                "odrl:target":      {"@id": input_key},
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
    destination_key = f"{uuid.uuid4()}.csv"

    presigned_put_url = s3_client.generate_presigned_url(
        "put_object",
        Params={"Bucket": destination_bucket, "Key": destination_key},
        ExpiresIn=EDC_POLL_TIMEOUT * 2,
    )
    print(f"presigned_put_url: {presigned_put_url}")

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
            "assetId":             input_key,
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
    return obj.get()["Body"].read().decode("utf-8")


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
