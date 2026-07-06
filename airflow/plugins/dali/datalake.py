from __future__ import annotations

import json
import os
import time
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


@task
def download_dataset() -> str:
    params = get_current_context()["params"]
    hook = S3Hook(aws_conn_id=DATASPACE_S3_CONN_ID)

    conn = hook.get_connection(DATASPACE_S3_CONN_ID)
    print(f"[dali] conn_id={DATASPACE_S3_CONN_ID!r} login={conn.login!r} extra={conn.extra!r}")
    client = hook.get_conn()
    print(f"[dali] resolved endpoint_url={client.meta.endpoint_url!r} region={client.meta.region_name!r} "
          f"addressing_style={client.meta.config.s3.get('addressing_style') if client.meta.config.s3 else None!r}")

    input_key = f"{params['dataset_id']}/{params['asset_title']}"
    print(f"[dali] dataset_id={params['dataset_id']} asset_title={params['asset_title']}")
    print(f"[dali] bucket={params['catalogue_id']!r} key={input_key!r}")

    obj = hook.get_key(key=input_key, bucket_name=params["catalogue_id"])
    return obj.get()["Body"].read().decode("utf-8")


@task
def download_dataset_edc() -> str:
    """
    Retrieve a dataset from the 6G-DALI provider EDC connector into the DataOps
    S3 bucket, then return its content as a string.

    Flow:
        1. Request the provider's catalogue via the DataOps EDC consumer
        2. Negotiate a contract for the requested asset
        3. Generate a presigned PUT URL for the DataOps S3 destination
        4. Initiate the transfer — the provider EDC PUTs directly to the
           presigned URL (no S3 credentials are shared with the provider)
        5. Poll until the transfer is complete
        6. Read the file from the DataOps S3 and return its content

    Required params:
        dataset_id        Asset ID in the provider's catalogue
        asset_title       Filename of the target object
        catalogue_id      DataOps S3 bucket where the file will land

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
    catalogue_id = params["catalogue_id"]
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
    presigned_put_url = s3_client.generate_presigned_url(
        "put_object",
        Params={"Bucket": catalogue_id, "Key": input_key},
        ExpiresIn=EDC_POLL_TIMEOUT * 2,
    )
    print(f"presigned_put_url: {presigned_put_url}")

    print(f"[edc] presigned PUT URL generated for s3://{catalogue_id}/{input_key}")

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
    print(f"[edc] retrieving {input_key} from bucket {catalogue_id}")
    obj = hook.get_key(key=input_key, bucket_name=catalogue_id)
    return obj.get()["Body"].read().decode("utf-8")


@task
def upload_results(report: dict) -> str:
    params = get_current_context()["params"]
    catalogue_id = params["catalogue_id"]
    input_key    = f"{params['dataset_id']}/{params['asset_title']}"

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
