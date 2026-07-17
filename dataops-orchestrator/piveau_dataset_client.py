"""
Builds and submits 6G-DALI MAP records (DCAT-AP + GAIA-X + CMT testbed-
context fields) to the piveau-hub Staging Catalogue, following the same PUT
{PIVEAU_HUB_URL}/datasets/{id}?catalogue={catalogue} pattern already used for
service registration (see piveau_service_client.py).

Submission is a two-step process, matching routers/datasets.py's two
endpoints:
  1. create_dataset   — PUT a Turtle document with just the dataset's own
                         metadata (identity/object/testbed context). No
                         distribution yet.
  2. add_distribution — GET the dataset's current JSON-LD graph, append a new
                         dcat:Distribution node (+ link it from the dataset),
                         and PUT the whole graph back. Can be called more
                         than once per dataset; each call gets the next
                         sequential distribution_id (count of existing
                         distributions + 1), so distribution numbering is
                         never hardcoded to a single fixed value.

`catalogue_id` doubles as both the piveau catalogue name and the Data Lake
S3 bucket the file was uploaded to, matching the convention every DataOps
DAG already assumes (see dali_dataspace_validate_dataset's `catalogue_id`
param).
"""

import json
import logging
import os
from datetime import datetime, timezone

import httpx
from fastapi import HTTPException

from dataset_models import DatasetIdentity, DatasetObject, DistributionMetrics, TestbedContext

log = logging.getLogger(__name__)

PIVEAU_HUB_URL         = os.getenv("PIVEAU_HUB_URL", "")
PIVEAU_API_KEY         = os.getenv("PIVEAU_API_KEY", "")
DSPACE_BASE            = os.getenv("DSPACE_BASE_URL", "https://dataspace.6gdali.eu")
PUBLISHER_NAME_DEFAULT = os.getenv("PUBLISHER_NAME", "6G-DALI")
DATASPACE_S3_ENDPOINT_URL = os.getenv("DATASPACE_S3_ENDPOINT_URL", "")
DALI_NS                = "https://dali-project.eu/ns#"

# Every distribution submitted through add_distribution is also registered
# as an EDC asset (see edc_client.register_asset) — this is the connector's
# own public base URL, used as dcat:accessURL (the negotiation entrypoint,
# distinct from EDC_PROVIDER_MANAGEMENT_URL's internal Management API and
# from the raw S3 object URL, which goes in dcat:downloadURL instead).
EDC_CONNECTOR_URL = os.getenv("EDC_CONNECTOR_URL", "http://edc.6gdali.sparkworks.net")

# Fallback for when the uploaded filename itself has no extension (see
# routers/datasets.py, which prefers the original filename's own extension —
# more reliable than content-type, since browsers/clients often send generic
# or wrong content-types for less common formats like JSON Lines).
EXTENSION_BY_MEDIA_TYPE = {
    "text/csv":                     "csv",
    "text/tab-separated-values":    "tsv",
    "application/json":             "json",
    "application/ld+json":          "jsonld",
    "application/jsonl":            "jsonl",
    "application/x-ndjson":         "jsonl",
    "application/x-jsonlines":      "jsonl",
    "text/plain":                   "txt",
    "application/xml":              "xml",
    "text/xml":                     "xml",
    "application/parquet":          "parquet",
    "application/octet-stream":     "bin",
}


def extension_for_media_type(media_type: str | None) -> str:
    return EXTENSION_BY_MEDIA_TYPE.get((media_type or "").lower().strip(), "dat")


# The canonical dcat:mediaType to register for an extension resolved from the
# uploaded filename — used when the browser/client's own content-type is
# missing or too generic to be worth recording as-is (see routers/datasets.py).
# Not just the reverse of EXTENSION_BY_MEDIA_TYPE: several content-types can
# map to the same extension there, so the canonical choice is spelled out
# explicitly here instead of derived.
CANONICAL_MEDIA_TYPE_BY_EXTENSION = {
    "csv":     "text/csv",
    "tsv":     "text/tab-separated-values",
    "json":    "application/json",
    "jsonld":  "application/ld+json",
    "jsonl":   "application/jsonl",
    "ndjson":  "application/x-ndjson",
    "txt":     "text/plain",
    "xml":     "application/xml",
    "parquet": "application/parquet",
}

# content-types too generic to be worth keeping as-is when a better guess
# (the uploaded filename's own extension) is available.
_GENERIC_MEDIA_TYPES = {"", "application/octet-stream", "binary/octet-stream"}


def resolve_media_type(content_type: str | None, ext: str) -> str | None:
    """Prefer the client-supplied content-type unless it's missing/generic,
    in which case fall back to the canonical media type for the resolved
    extension (which itself may come from the filename, not content-type —
    see routers/datasets.py)."""
    if content_type and content_type.lower().strip() not in _GENERIC_MEDIA_TYPES:
        return content_type
    return CANONICAL_MEDIA_TYPE_BY_EXTENSION.get(ext.lower(), content_type)


_ACCESS_RIGHTS = {
    "PUBLIC":     "http://publications.europa.eu/resource/authority/access-right/PUBLIC",
    "RESTRICTED": "http://publications.europa.eu/resource/authority/access-right/RESTRICTED",
    "NON_PUBLIC": "http://publications.europa.eu/resource/authority/access-right/NON_PUBLIC",
}


def _esc(s: str) -> str:
    return s.replace("\\", "\\\\").replace('"', '\\"')


def _dataset_uri(dataset_id: str) -> str:
    return f"{DSPACE_BASE}/set/data/{dataset_id}"


def _distribution_uri(asset_id: str) -> str:
    """piveau represents distributions as flat resources — /set/distribution/{id},
    not nested under the dataset's own URI — confirmed against production records
    (e.g. https://dspace.sparkworks.net/set/distribution/<uuid>)."""
    return f"{DSPACE_BASE}/set/distribution/{asset_id}"


def _require_piveau_config() -> None:
    if not PIVEAU_HUB_URL or not PIVEAU_API_KEY:
        raise HTTPException(status_code=503, detail="PIVEAU_HUB_URL or PIVEAU_API_KEY not configured")


def _testbed_context_block(tc: TestbedContext) -> list[str]:
    """Build the dali:testbedContext blank-node lines, skipping unset fields."""
    fields = [
        ("dali:underlayPlatform",       tc.underlay_platform,       "uri"),
        ("dali:environment",            tc.environment,             "str"),
        ("dali:networkDomain",          tc.network_domain,          "str"),
        ("dali:ran3gppRelease",         tc.ran_3gpp_release,        "str"),
        ("dali:ranNewRadioType",        tc.ran_new_radio_type,      "str"),
        ("dali:ranSplit",               tc.ran_split,               "str"),
        ("dali:ranFocusedTechnology",   tc.ran_focused_technology,  "str"),
        ("dali:ranCoverageType",        tc.ran_coverage_type,       "str"),
        ("dali:ranFrequencyBand",       tc.ran_frequency_band,      "str"),
        ("dali:ranBandwidthMHz",        tc.ran_bandwidth_mhz,       "num"),
        ("dali:ranMaxEndDevices",       tc.ran_max_end_devices,     "num"),
        ("dali:ranMobilityModel",       tc.ran_mobility_model,      "str"),
        ("dali:coreRelease",            tc.core_release,            "str"),
        ("dali:coreSolution",           tc.core_solution,           "str"),
        ("dali:transportType",          tc.transport_type,          "str"),
        ("dali:computeOrchestratorType", tc.compute_orchestrator_type, "str"),
        ("dali:computeGpuUse",          tc.compute_gpu_use,         "bool"),
        ("dali:computeVirtualizationType", tc.compute_virtualization_type, "str"),
        ("dali:computeInfrastructureType", tc.compute_infrastructure_type, "str"),
        ("dali:trafficOrigin",          tc.traffic_origin,          "str"),
        ("dali:trafficPattern",         tc.traffic_pattern,         "str"),
        ("dali:sliceType",              tc.slice_type,              "str"),
        ("dali:referencePlane",         tc.reference_plane,         "str"),
        ("dali:relatedVertical",        tc.related_vertical,        "str"),
        ("dali:observationPointHorizontal", tc.observation_point_horizontal, "str"),
        ("dali:observationPointVertical",   tc.observation_point_vertical,   "str"),
    ]
    lines = []
    for pred, value, kind in fields:
        if value is None or value == "":
            continue
        if kind == "uri":
            lines.append(f"        {pred} <{value}> ;")
        elif kind == "bool":
            lines.append(f'        {pred} {str(value).lower()} ;')
        elif kind == "num":
            num = int(value) if float(value).is_integer() else value
            lines.append(f"        {pred} {num} ;")
        else:
            lines.append(f'        {pred} "{_esc(str(value))}" ;')
    for fam in tc.measurement_family:
        lines.append(f'        dali:measurementFamily "{_esc(fam)}" ;')
    for tool in tc.measurement_tool:
        lines.append(f'        dali:measurementTool "{_esc(tool)}" ;')
    return lines


def build_dataset_turtle(dataset_id: str, ident: DatasetIdentity, obj: DatasetObject, testbed_context: TestbedContext) -> str:
    """Dataset-only Turtle — no dcat:Distribution, no dcat:distribution link.
    Distributions are added afterwards, one at a time, via add_distribution."""
    uri = _dataset_uri(dataset_id)
    now = datetime.now(timezone.utc).strftime("%Y-%m-%d")

    access_rights_uri = _ACCESS_RIGHTS.get(obj.access_rights, _ACCESS_RIGHTS["PUBLIC"])

    lines = [
        "@prefix rdf:    <http://www.w3.org/1999/02/22-rdf-syntax-ns#> .",
        "@prefix rdfs:   <http://www.w3.org/2000/01/rdf-schema#> .",
        "@prefix dcat:   <http://www.w3.org/ns/dcat#> .",
        "@prefix dct:    <http://purl.org/dc/terms/> .",
        "@prefix adms:   <http://www.w3.org/ns/adms#> .",
        "@prefix foaf:   <http://xmlns.com/foaf/0.1/> .",
        "@prefix vcard:  <http://www.w3.org/2006/vcard/ns#> .",
        "@prefix gax:    <https://registry.lab.gaia-x.eu/v1/api/trusted-shape-registry/v1/shapes/jsonld/trustframework#> .",
        "@prefix schema: <https://schema.org/> .",
        "@prefix dali:   <https://dali-project.eu/ns#> .",
        "@prefix xsd:    <http://www.w3.org/2001/XMLSchema#> .",
        "",
        f"<{uri}>",
        "    rdf:type                dcat:Dataset, gax:DataResource ;",
        f'    dct:title               "{_esc(ident.title)}"@en ;',
        f'    dct:description         "{_esc(ident.description)}"@en ;',
        f'    dct:identifier          "{_esc(dataset_id)}" ;',
        f'    dct:issued              "{now}"^^xsd:date ;',
        f"    dct:accessRights        <{access_rights_uri}> ;",
        f"    dct:license             <{obj.license}> ;",
        f'    dali:snsProjectName     "{_esc(ident.sns_project_name)}" ;',
        f'    dali:gdprCompliant      {str(obj.gdpr_compliant).lower()} ;',
        f'    dali:fairCompliant      {str(obj.fair_compliant).lower()} ;',
        f'    gax:containsPII         {str(obj.contains_pii).lower()} ;',
        f'    dct:conformsTo          <https://www.go-fair.org/fair-principles/> ;',
    ]

    if obj.produced_by:
        lines.append(f"    gax:producedBy          <{obj.produced_by}> ;")

    if ident.publisher_name:
        lines.append(f'    dct:publisher           [ rdf:type foaf:Organization ; foaf:name "{_esc(ident.publisher_name)}" ] ;')
    if ident.contact_email:
        lines.append(f'    dcat:contactPoint       [ rdf:type vcard:Organization ; vcard:hasEmail <mailto:{ident.contact_email}> ] ;')
    for c in ident.contributors:
        lines.append(f'    dct:contributor         [ rdf:type foaf:Agent ; foaf:name "{_esc(c)}" ] ;')
    for kw in ident.keywords:
        lines.append(f'    dcat:keyword            "{_esc(kw)}"@en ;')
    for pub in ident.related_publications:
        lines.append(f"    dct:relation            <{pub}> ;")
    if ident.language:
        lines.append(f"    dct:language            <http://publications.europa.eu/resource/authority/language/{ident.language}> ;")
    if ident.spatial:
        lines.append(f'    dct:spatial             "{_esc(ident.spatial)}" ;')
    if ident.temporal_start and ident.temporal_end:
        lines.append(
            f"    dct:temporal            [ rdf:type dct:PeriodOfTime ; "
            f'dcat:startDate "{ident.temporal_start}"^^xsd:date ; dcat:endDate "{ident.temporal_end}"^^xsd:date ] ;'
        )
    if ident.version:
        lines.append(f'    adms:version            "{_esc(ident.version)}" ;')

    tc_lines = _testbed_context_block(testbed_context)
    if tc_lines:
        lines.append("    dali:testbedContext     [")
        lines.append("        rdf:type dali:TestbedContext ;")
        lines.extend(tc_lines)
        lines.append("    ] ;")

    # close the dataset resource
    lines[-1] = lines[-1].rstrip(" ;") + " ."

    return "\n".join(lines)


async def create_dataset(
    dataset_id: str, catalogue_id: str, ident: DatasetIdentity, obj: DatasetObject, testbed_context: TestbedContext
) -> dict:
    """Step 1: register the dataset's own metadata in piveau. No file, no
    distribution yet — call add_distribution afterwards for that."""
    _require_piveau_config()

    turtle = build_dataset_turtle(dataset_id, ident, obj, testbed_context)
    log.info("[piveau] dataset Turtle for %s:\n%s", dataset_id, turtle)

    url = f"{PIVEAU_HUB_URL}/datasets/{dataset_id}?catalogue={catalogue_id}"
    headers = {"X-API-Key": PIVEAU_API_KEY, "Content-Type": "text/turtle"}
    try:
        async with httpx.AsyncClient(timeout=15) as client:
            r = await client.put(url, content=turtle.encode(), headers=headers)
        r.raise_for_status()
    except httpx.HTTPStatusError as e:
        raise HTTPException(
            status_code=e.response.status_code,
            detail=f"piveau error for {dataset_id}: {e.response.status_code} {e.response.text[:500]}",
        )
    except httpx.RequestError as e:
        raise HTTPException(status_code=502, detail=f"Could not reach piveau at {PIVEAU_HUB_URL}: {e}")

    return {"dataset_id": dataset_id, "dataset_uri": _dataset_uri(dataset_id), "status": "created", "piveau_url": url}


async def _fetch_dataset_graph(dataset_id: str, catalogue_id: str) -> dict:
    _require_piveau_config()
    url = f"{PIVEAU_HUB_URL}/datasets/{dataset_id}?catalogue={catalogue_id}"
    headers = {"X-API-Key": PIVEAU_API_KEY, "Accept": "application/ld+json"}
    try:
        async with httpx.AsyncClient(timeout=15) as client:
            r = await client.get(url, headers=headers)
        r.raise_for_status()
    except httpx.HTTPStatusError as e:
        raise HTTPException(
            status_code=e.response.status_code,
            detail=f"piveau error for {dataset_id}: {e.response.status_code} {e.response.text[:500]}",
        )
    except httpx.RequestError as e:
        raise HTTPException(status_code=502, detail=f"Could not reach piveau at {PIVEAU_HUB_URL}: {e}")
    return r.json()


def _node_types(node: dict) -> list[str]:
    t = node.get("@type", [])
    return t if isinstance(t, list) else [t]


def _scalar(val) -> str:
    if isinstance(val, list):
        val = val[0] if val else ""
    if isinstance(val, dict):
        return val.get("@value", "")
    return str(val) if val else ""


def _asset_id_of(node: dict) -> str:
    # piveau's own Turtle/JSON-LD serialization doesn't necessarily reuse the
    # "dali:" prefix we submitted under — it may come back keyed by the full
    # IRI instead (confirmed against a real record). Check both forms.
    return _scalar(node.get("dali:assetId") or node.get(f"{DALI_NS}assetId"))


def _count_distributions(graph: dict) -> int:
    nodes = graph.get("@graph", [])
    return sum(1 for n in nodes if any("Distribution" in t for t in _node_types(n)))


async def next_distribution_id(dataset_id: str, catalogue_id: str) -> str:
    """The distribution_id the *next* add_distribution call on this dataset
    will get — needed by the caller up front, since the S3 object must be
    uploaded under "{this_id}.{ext}" before add_distribution itself runs."""
    graph = await _fetch_dataset_graph(dataset_id, catalogue_id)
    return str(_count_distributions(graph) + 1)


async def add_distribution(
    dataset_id: str, catalogue_id: str, distribution_id: str, asset_id: str,
    distribution_url: str, original_filename: str | None, media_type: str | None,
    metrics: DistributionMetrics,
) -> dict:
    """Step 2: append a new dcat:Distribution to an existing dataset's piveau
    record — fetch the current JSON-LD graph, add the node (+ link it from
    the dataset), PUT the whole graph back. Safe to call more than once per
    dataset; get `distribution_id` from next_distribution_id first so the
    caller can name the S3 object to match before this runs.

    `asset_id` is a UUID generated by the caller (routers/datasets.py) — it,
    not `distribution_id`, is written as dali:assetId and is what the S3
    object is named after. `distribution_id` only numbers/locates this node
    within the dataset's graph."""
    graph = await _fetch_dataset_graph(dataset_id, catalogue_id)
    nodes = graph.get("@graph", [])

    # Match by rdf:type, not by comparing @id to a locally-computed
    # _dataset_uri(dataset_id) — piveau-hub-repo canonicalizes the dataset's
    # own resource URI itself on PUT, so an exact-URI comparison here
    # silently found no match against production records, which meant the
    # new distribution was appended as an orphan node with no
    # dcat:distribution link back to the dataset (confirmed against a real
    # dataset record: its existing distributions never showed up as a match).
    ds_node = next((n for n in nodes if any("Dataset" in t for t in _node_types(n))), None)

    # piveau represents distributions as flat resources (.../set/distribution/{id}),
    # not nested under the dataset's own URI — also confirmed against production
    # records, which is why dist_uri is built from _distribution_uri, independent
    # of whatever the dataset's own (possibly rewritten) @id turned out to be.
    dist_uri = _distribution_uri(asset_id)

    dist_node: dict = {
        "@id": dist_uri,
        "@type": "dcat:Distribution",
        "dct:title": {"@value": original_filename or f"Distribution {distribution_id}"},
        # accessURL points at the EDC connector's negotiation entrypoint
        # (this distribution is registered there under dali:assetId — see
        # edc_client.register_asset), not the raw file — that's downloadURL.
        "dcat:accessURL": {"@id": EDC_CONNECTOR_URL},
        "dcat:downloadURL": {"@id": distribution_url},
        # Identifies the underlying file — a UUID generated at upload time
        # (routers/datasets.py), independent of distribution_id (which only
        # locates this node within the dataset's graph). This, not
        # distribution_id, is what the validate DAG's download_dataset task
        # matches against an S3 prefix listing to find the object (see
        # dali/datalake.py). Must match the object's actual uploaded
        # basename (routers/datasets.py).
        "dali:assetId": asset_id,
        # Every distribution submitted through this endpoint is also
        # registered as an EDC asset under this same asset_id (see
        # edc_client.register_asset, called from routers/datasets.py before
        # this) — connectorType flags that to consumers (e.g. the vanilla
        # frontend's download-button logic), matching the "dspaceconnector"
        # value used elsewhere for EDC-served distributions.
        "dali:connectorType": "dspaceconnector",
    }
    if media_type:
        dist_node["dcat:mediaType"] = media_type
    # Measured variables/technique describe this distribution's file
    # specifically, not the dataset as a whole (see MAP §5.3.E/§5.6).
    if metrics.variable_measured:
        dist_node["schema:variableMeasured"] = list(metrics.variable_measured)
    if metrics.measurement_technique:
        dist_node["schema:measurementTechnique"] = {"@value": metrics.measurement_technique, "@language": "en"}

    nodes.append(dist_node)

    if ds_node is not None:
        existing = ds_node.get("dcat:distribution")
        refs = [] if existing is None else (existing if isinstance(existing, list) else [existing])
        refs.append({"@id": dist_uri})
        ds_node["dcat:distribution"] = refs
    else:
        log.warning("[piveau] no dcat:Dataset node found in the fetched graph for %s — "
                    "dcat:distribution link not added, only the distribution node itself", dataset_id)

    graph["@graph"] = nodes

    ctx = graph.get("@context", {})
    if isinstance(ctx, dict):
        ctx.setdefault("dct",    "http://purl.org/dc/terms/")
        ctx.setdefault("dcat",   "http://www.w3.org/ns/dcat#")
        ctx.setdefault("dali",   "https://dali-project.eu/ns#")
        ctx.setdefault("schema", "https://schema.org/")
        graph["@context"] = ctx

    url = f"{PIVEAU_HUB_URL}/datasets/{dataset_id}?catalogue={catalogue_id}"
    headers = {"X-API-Key": PIVEAU_API_KEY, "Content-Type": "application/ld+json"}
    try:
        async with httpx.AsyncClient(timeout=15) as client:
            r = await client.put(url, content=json.dumps(graph).encode(), headers=headers)
        r.raise_for_status()
    except httpx.HTTPStatusError as e:
        raise HTTPException(
            status_code=e.response.status_code,
            detail=f"piveau error for {dataset_id}: {e.response.status_code} {e.response.text[:500]}",
        )
    except httpx.RequestError as e:
        raise HTTPException(status_code=502, detail=f"Could not reach piveau at {PIVEAU_HUB_URL}: {e}")

    # piveau mints its own canonical @id for the distribution on write,
    # discarding whatever @id we PUT (confirmed against a real record: our
    # submitted URI only survives as dct:identifier, while @id becomes a new
    # piveau-assigned UUID under /set/distribution/). Callers (e.g. the
    # validate DAG trigger) need *that* real id to ever find this node again,
    # so re-fetch the graph and locate the node by its dali:assetId — a plain
    # literal, unlike @id/dct:identifier, so it survives untouched — then
    # read off piveau's actual assigned id.
    refetched = await _fetch_dataset_graph(dataset_id, catalogue_id)
    real_node = next(
        (
            n for n in refetched.get("@graph", [])
            if any("Distribution" in t for t in _node_types(n)) and _asset_id_of(n) == asset_id
        ),
        None,
    )
    if real_node is not None:
        real_uri = real_node.get("@id") or dist_uri
        piveau_distribution_id = real_uri.rstrip("/").rsplit("/", 1)[-1]
    else:
        log.warning("[piveau] could not find the just-added distribution (asset_id=%s) in the "
                    "re-fetched graph for %s — falling back to the locally-guessed id", asset_id, dataset_id)
        real_uri = dist_uri
        piveau_distribution_id = distribution_id

    return {
        "dataset_id":       dataset_id,
        "distribution_id":  piveau_distribution_id,
        "distribution_uri": real_uri,
        "status":           "submitted",
        "piveau_url":       url,
    }
