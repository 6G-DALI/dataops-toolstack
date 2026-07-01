from __future__ import annotations

import json

from airflow.decorators import task
from airflow.sdk import get_current_context

import os

from dali.utils import DALI_NS, PIVEAU_DATASETS_URL


@task
def publish_quality_to_piveau(report: dict) -> None:
    import requests as req
    params = get_current_context()["params"]
    dataset_id   = params["input_key"].split("/")[0]
    catalogue_id = params["catalogue_id"]
    api_key      = os.environ["PIVEAU_API_KEY"]

    base_url = f"{PIVEAU_DATASETS_URL}/{dataset_id}"
    qs       = f"?catalogue={catalogue_id}" if catalogue_id else ""
    headers  = {"X-API-Key": api_key, "Accept": "application/ld+json"}

    get_resp = req.get(f"{base_url}{qs}", headers=headers, timeout=15)
    if get_resp.status_code == 404:
        print(f"[dali] dataset {dataset_id} not found — skipping quality publish")
        return
    get_resp.raise_for_status()
    graph = get_resp.json()

    dataset_uri = base_url
    run_time    = report["run_time"]

    meas_refs  = []
    meas_nodes = []
    for r in report["results"]:
        exp_type = r["expectation_type"]
        col      = r.get("kwargs", {}).get("column", "")
        suffix   = f"{exp_type}_{col}" if col else exp_type
        meas_uri = f"{dataset_uri}/quality/{suffix}"
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

    nodes = graph.get("@graph", [])
    nodes = [n for n in nodes if not str(n.get("@id", "")).startswith(f"{dataset_uri}/quality/")]

    ds_node = next((n for n in nodes if n.get("@id") == dataset_uri), None)
    if ds_node is None:
        ds_node = {"@id": dataset_uri, "@type": "dcat:Dataset"}
        nodes.append(ds_node)
    for key in list(ds_node.keys()):
        if "hasQualityMeasurement" in key:
            del ds_node[key]

    if meas_refs:
        ds_node["dqv:hasQualityMeasurement"] = meas_refs
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
