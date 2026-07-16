from __future__ import annotations

import json

from airflow.decorators import task
from airflow.sdk import get_current_context

import os

from dali.utils import (
    DALI_NS,
    PIVEAU_DATASETS_URL,
    dist_keys,
    extension_for_media_type,
    fetch_distribution_info,
    node_types,
)


@task
def resolve_asset_title() -> str:
    """Derive the distribution's S3 object filename from its dali:assetId and
    dcat:mediaType (fetched from piveau), instead of taking it as a DAG param
    — keeps it in sync with how dataops-orchestrator names the object at
    upload time (see piveau_dataset_client.py's FIRST_DISTRIBUTION_ID and
    routers/datasets.py's submit_dataset).

    distribution_id only locates the right dcat:Distribution node — it is
    piveau's own node identifier, not necessarily the same as dali:assetId,
    which is what actually identifies the file and is what gets prefixed
    with the extension here. Falls back to distribution_id when a
    distribution has no dali:assetId (e.g. older/foreign records)."""
    params = get_current_context()["params"]
    dataset_id      = params["dataset_id"]
    distribution_id = params.get("distribution_id", "")
    asset_id, media_type = fetch_distribution_info(dataset_id, distribution_id)
    ext = extension_for_media_type(media_type)
    basename = asset_id or distribution_id or "data"
    asset_title = f"{basename}.{ext}"
    print(f"[dali] resolved asset_title={asset_title!r} from asset_id={asset_id!r} "
          f"distribution_id={distribution_id!r} media_type={media_type!r}")
    return asset_title


@task
def publish_quality_to_piveau(report: dict) -> None:
    import requests as req
    params = get_current_context()["params"]
    dataset_id      = params["dataset_id"]
    catalogue_id    = params["catalogue_id"]
    distribution_id = params.get("distribution_id", "")
    api_key         = os.environ["PIVEAU_API_KEY"]

    base_url = f"{PIVEAU_DATASETS_URL}/{dataset_id}"
    qs       = f"?catalogue={catalogue_id}" if catalogue_id else ""
    headers  = {"X-API-Key": api_key, "Accept": "application/ld+json"}

    get_resp = req.get(f"{base_url}{qs}", headers=headers, timeout=15)
    if get_resp.status_code == 404:
        print(f"[dali] dataset {dataset_id} not found — skipping quality publish")
        return
    get_resp.raise_for_status()
    graph = get_resp.json()

    run_time = report["run_time"]

    nodes = graph.get("@graph", [])
    dist_candidates = [n for n in nodes if any("Distribution" in t for t in node_types(n))]

    if distribution_id:
        dist_node = next((n for n in dist_candidates if distribution_id in dist_keys(n)), None)
        if dist_node is None:
            print(f"[dali] dataset {dataset_id} has no dcat:Distribution node matching "
                  f"distribution_id={distribution_id!r} — skipping quality publish")
            return
    else:
        dist_node = dist_candidates[0] if dist_candidates else None
        if dist_node is None:
            print(f"[dali] dataset {dataset_id} has no dcat:Distribution node — skipping quality publish")
            return
        if len(dist_candidates) > 1:
            print(f"[dali] no distribution_id given and dataset {dataset_id} has "
                  f"{len(dist_candidates)} distributions — defaulting to the first one "
                  f"({dist_node.get('@id')!r}); pass distribution_id to target a specific one")
    dist_uri = dist_node["@id"]

    meas_refs  = []
    meas_nodes = []
    for r in report["results"]:
        exp_type = r["expectation_type"]
        col      = r.get("kwargs", {}).get("column", "")
        suffix   = f"{exp_type}_{col}" if col else exp_type
        meas_uri = f"{dist_uri}/quality/{suffix}"
        meas_refs.append({"@id": meas_uri})
        meas_nodes.append({
            "@id":                 meas_uri,
            "@type":               "dqv:QualityMeasurement",
            "dqv:isMeasurementOf": {"@id": f"{DALI_NS}{exp_type}"},
            "dqv:value":           {"@value": str(r["success"]).lower(), "@type": "xsd:boolean"},
            "dct:description":     json.dumps({
                **{k: v for k, v in r.get("kwargs", {}).items() if k != "batch_id"},
                **r.get("result", {}),
            }),
            "dct:date":            {"@value": run_time, "@type": "xsd:dateTime"},
        })

    nodes = [n for n in nodes if not str(n.get("@id", "")).startswith(f"{dist_uri}/quality/")]
    dist_node = next(n for n in nodes if n.get("@id") == dist_uri)
    for key in list(dist_node.keys()):
        if "hasQualityMeasurement" in key:
            del dist_node[key]

    if meas_refs:
        dist_node["dqv:hasQualityMeasurement"] = meas_refs
        nodes.extend(meas_nodes)

    graph["@graph"] = nodes

    ctx = graph.get("@context", {})
    if isinstance(ctx, dict):
        ctx.setdefault("dqv",  "http://www.w3.org/ns/dqv#")
        ctx.setdefault("dct",  "http://purl.org/dc/terms/")
        ctx.setdefault("dcat", "http://www.w3.org/ns/dcat#")
        ctx.setdefault("xsd",  "http://www.w3.org/2001/XMLSchema#")
        graph["@context"] = ctx

    print(f"[dali] piveau PUT: {len(graph.get('@graph', []))} nodes, {len(meas_refs)} quality measurements")

    put_resp = req.put(
        f"{base_url}{qs}",
        headers={**headers, "Content-Type": "application/ld+json"},
        data=json.dumps(graph),
        timeout=15,
    )
    put_resp.raise_for_status()
    print(f"[dali] quality published for {dataset_id} — HTTP {put_resp.status_code}")
