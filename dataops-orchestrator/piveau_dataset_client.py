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

import asyncio
import json
import logging
import os
from datetime import datetime, timezone

import httpx
from fastapi import HTTPException
from rdflib import Graph, URIRef

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

# dct:format on a dcat:Distribution must be an IRI from the EU file-type
# authority vocabulary (dali:DistributionShape requires sh:nodeKind sh:IRI),
# not a media-type string — that's dcat:mediaType's job. JSON Lines has no
# dedicated authority entry, so it maps to JSON, matching how the reference
# records catalogue the IMEC .jsonl scenario files.
_FILE_TYPE_BASE = "http://publications.europa.eu/resource/authority/file-type"
FILE_TYPE_IRI_BY_EXTENSION = {
    "csv":     f"{_FILE_TYPE_BASE}/CSV",
    "tsv":     f"{_FILE_TYPE_BASE}/TSV",
    "json":    f"{_FILE_TYPE_BASE}/JSON",
    "jsonl":   f"{_FILE_TYPE_BASE}/JSON",
    "ndjson":  f"{_FILE_TYPE_BASE}/JSON",
    "jsonld":  f"{_FILE_TYPE_BASE}/JSON_LD",
    "txt":     f"{_FILE_TYPE_BASE}/TXT",
    "xml":     f"{_FILE_TYPE_BASE}/XML",
    "parquet": f"{_FILE_TYPE_BASE}/PARQUET",
    "tar":     f"{_FILE_TYPE_BASE}/TAR",
    "zip":     f"{_FILE_TYPE_BASE}/ZIP",
    "gz":      f"{_FILE_TYPE_BASE}/GZIP",
}


def file_type_iri(ext: str) -> str | None:
    """The EU file-type authority IRI for a resolved extension, or None when
    the extension isn't in the vocabulary — in which case dct:format is left
    off rather than filled with a non-authority IRI (a Warning, not a
    Violation, per dali:DistributionShape)."""
    return FILE_TYPE_IRI_BY_EXTENSION.get(ext.lower().strip())


def resolve_media_type(content_type: str | None, ext: str) -> str | None:
    """Prefer the client-supplied content-type unless it's missing/generic,
    in which case fall back to the canonical media type for the resolved
    extension (which itself may come from the filename, not content-type —
    see routers/datasets.py)."""
    if content_type and content_type.lower().strip() not in _GENERIC_MEDIA_TYPES:
        return content_type
    return CANONICAL_MEDIA_TYPE_BY_EXTENSION.get(ext.lower(), content_type)


# --- Sentinels for the RDF-first submission path (POST /datasets/rdf) --------
# A client that builds its own Turtle cannot know either identifier: dataset_id
# is minted here, and DSPACE_BASE is server configuration. So it emits these two
# placeholders and create_dataset_from_turtle substitutes them. Keeping the
# minting server-side matters for more than tidiness — a client-chosen
# dataset_id would let a submitter PUT over an existing dataset's record, since
# piveau writes are keyed by id.
#
# These must stay byte-identical to the constants in dataops-ui/src/map/datasetTurtle.ts.
DATASET_URI_SENTINEL = "urn:6gdali:dataset:self"   # the dataset's subject IRI
DATASET_ID_SENTINEL  = "urn:6gdali:dataset:id"     # the bare id, as dct:identifier

# The same idea for a submitted distribution (POST /datasets/{id}/distributions/rdf).
# A client can describe the distribution but cannot know any of these: the asset
# id is minted here, the object URL only exists after the upload, and the
# connector URL is server configuration. The subject and the dali:assetId literal
# need separate tokens because one resolves to a full IRI and the other to the
# bare UUID.
DISTRIBUTION_URI_SENTINEL = "urn:6gdali:distribution:self"
ASSET_ID_SENTINEL         = "urn:6gdali:distribution:asset-id"
DOWNLOAD_URL_SENTINEL     = "urn:6gdali:datalake:object-url"
ACCESS_URL_SENTINEL       = "urn:6gdali:edc:connector-url"

# Compact terms for serializing a submitted distribution back to JSON-LD, so the
# node merges into the dataset record in the same shape add_distribution writes.
_JSONLD_CONTEXT = {
    "rdf":    "http://www.w3.org/1999/02/22-rdf-syntax-ns#",
    "rdfs":   "http://www.w3.org/2000/01/rdf-schema#",
    "dcat":   "http://www.w3.org/ns/dcat#",
    "dct":    "http://purl.org/dc/terms/",
    "adms":   "http://www.w3.org/ns/adms#",
    "foaf":   "http://xmlns.com/foaf/0.1/",
    "vcard":  "http://www.w3.org/2006/vcard/ns#",
    "schema": "https://schema.org/",
    "spdx":   "http://spdx.org/rdf/terms#",
    "dali":   DALI_NS,
    "gax":    "https://registry.lab.gaia-x.eu/v1/api/trusted-shape-registry/v1/shapes/jsonld/trustframework#",
}


_ACCESS_RIGHTS = {
    "PUBLIC":     "http://publications.europa.eu/resource/authority/access-right/PUBLIC",
    "RESTRICTED": "http://publications.europa.eu/resource/authority/access-right/RESTRICTED",
    "NON_PUBLIC": "http://publications.europa.eu/resource/authority/access-right/NON_PUBLIC",
}


def _esc(s: str) -> str:
    """Escape a value for a Turtle single-quoted string literal. Newlines and
    tabs need escaping as much as quotes do: dct:description is submitted from a
    textarea, and a raw newline inside "..." is a Turtle syntax error, so an
    unescaped one made piveau reject the whole record."""
    return (
        s.replace("\\", "\\\\")
        .replace('"', '\\"')
        .replace("\r", "\\r")
        .replace("\n", "\\n")
        .replace("\t", "\\t")
    )


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
    # Repeatable testbed properties — emitted one triple per value rather than
    # as a single joined literal, so a multi-band or multi-tool setup stays
    # queryable per value. Blank entries are skipped: the UI's append-able
    # lists can submit an unfilled row, which would otherwise be published as
    # an empty literal.
    repeatable = [
        ("dali:ranFrequencyBand",  tc.ran_frequency_band),
        ("dali:measurementFamily", tc.measurement_family),
        ("dali:measurementTool",   tc.measurement_tool),
    ]
    for pred, values in repeatable:
        for value in values:
            if not value or not value.strip():
                continue
            lines.append(f'        {pred} "{_esc(value.strip())}" ;')
    return lines


def build_dataset_turtle(dataset_id: str, ident: DatasetIdentity, obj: DatasetObject, testbed_context: TestbedContext) -> str:
    """Dataset-only Turtle — no dcat:Distribution, no dcat:distribution link.
    Distributions are added afterwards, one at a time, via add_distribution."""
    uri = _dataset_uri(dataset_id)
    # dct:issued is the dataset's *first publication* date. For a harvested
    # record that's the upstream one (e.g. the Zenodo record's), so prefer the
    # submitted value and only fall back to today when none was given.
    issued = ident.issued or datetime.now(timezone.utc).strftime("%Y-%m-%d")

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
        f'    dct:issued              "{issued}"^^xsd:date ;',
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
    for creator in ident.creators:
        if not creator.name:
            continue
        # foaf:Person for named researchers, foaf:Organization when the
        # institution itself is credited — the MAP uses both. ORCID goes in
        # schema:identifier as an IRI (matching the reference records), so a
        # bare "0000-0002-..." is expanded to its resolvable orcid.org form.
        node_type = "foaf:Organization" if creator.kind == "Organization" else "foaf:Person"
        parts = [f"rdf:type {node_type}", f'foaf:name "{_esc(creator.name)}"']
        if creator.orcid:
            orcid = creator.orcid.strip()
            if not orcid.startswith("http"):
                orcid = f"https://orcid.org/{orcid}"
            parts.append(f"schema:identifier <{orcid}>")
        if creator.affiliation:
            parts.append(f'schema:affiliation "{_esc(creator.affiliation)}"')
        lines.append(f'    dct:creator             [ {" ; ".join(parts)} ] ;')
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


async def _put_dataset_turtle(dataset_id: str, catalogue_id: str, turtle: str) -> dict:
    """PUT a complete Turtle dataset record to piveau. Shared by both submission
    paths — the JSON one (create_dataset, which builds the Turtle here) and the
    RDF one (create_dataset_from_turtle, which receives it already built)."""
    _require_piveau_config()

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


async def create_dataset(
    dataset_id: str, catalogue_id: str, ident: DatasetIdentity, obj: DatasetObject, testbed_context: TestbedContext
) -> dict:
    """Step 1: register the dataset's own metadata in piveau. No file, no
    distribution yet — call add_distribution afterwards for that."""
    turtle = build_dataset_turtle(dataset_id, ident, obj, testbed_context)
    return await _put_dataset_turtle(dataset_id, catalogue_id, turtle)


def resolve_sentinels(turtle: str, dataset_id: str) -> str:
    """Replace the client-side placeholders with the identifiers this server
    owns (see DATASET_URI_SENTINEL). Plain substitution rather than an RDF
    rewrite: the orchestrator has no RDF library, and piveau is the parser —
    keeping this a passthrough means the graph piveau stores is byte-for-byte
    the graph the submitter was shown, apart from these two tokens."""
    return (
        turtle
        .replace(DATASET_URI_SENTINEL, _dataset_uri(dataset_id))
        .replace(DATASET_ID_SENTINEL, dataset_id)
    )


async def create_dataset_from_turtle(dataset_id: str, catalogue_id: str, turtle: str) -> dict:
    """Step 1, RDF-first variant: register a dataset from a Turtle record the
    client built itself (see POST /datasets/rdf), rather than from the JSON
    field groups build_dataset_turtle renders.

    The record must reference the dataset by DATASET_URI_SENTINEL — without it
    the graph would describe some other subject, and piveau would happily store
    a record that no dataset_id resolves to."""
    if not turtle or not turtle.strip():
        raise HTTPException(status_code=422, detail="Empty Turtle body")
    if DATASET_URI_SENTINEL not in turtle:
        raise HTTPException(
            status_code=422,
            detail=(
                f"Turtle body must identify the dataset by the sentinel IRI "
                f"<{DATASET_URI_SENTINEL}>, which is replaced with the "
                f"server-minted dataset URI."
            ),
        )
    return await _put_dataset_turtle(dataset_id, catalogue_id, resolve_sentinels(turtle, dataset_id))


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


async def _fetch_dataset_graph_with_dataset(dataset_id: str, catalogue_id: str) -> dict:
    """Fetch a dataset's graph, waiting briefly for its dcat:Dataset node to be
    there.

    piveau-hub acknowledges a write before the record is necessarily readable
    back, so a distribution added immediately after the dataset was created could
    fetch a document with no dataset node in it — and then be appended to nothing
    and silently lost. Retries a few times with a short backoff, and logs the
    document's actual shape so a persistent failure is diagnosable rather than
    just absent."""
    delays = (0.0, 0.5, 1.0, 2.0)
    doc: dict = {}
    for attempt, delay in enumerate(delays, start=1):
        if delay:
            await asyncio.sleep(delay)
        doc = await _fetch_dataset_graph(dataset_id, catalogue_id)
        nodes = _graph_nodes(doc)
        if _find_dataset_node(nodes) is not None:
            if attempt > 1:
                log.info("[piveau] dataset %s became readable on attempt %d", dataset_id, attempt)
            return doc
        log.warning(
            "[piveau] dataset %s not readable yet (attempt %d/%d) — top-level keys=%s, "
            "%d node(s), types=%s",
            dataset_id, attempt, len(delays), sorted(doc.keys()), len(nodes),
            [t for n in nodes for t in _node_types(n)],
        )
    return doc


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


def _iri_of(val) -> str:
    """The IRI out of a JSON-LD value that may be a list, an {"@id": ...}
    node reference, or a bare string."""
    if isinstance(val, list):
        val = val[0] if val else ""
    if isinstance(val, dict):
        return val.get("@id", "")
    return str(val) if val else ""


def _dataset_license(ds_node: dict | None) -> str:
    """The dataset's own dct:license IRI, to be inherited by a distribution
    that doesn't carry one of its own. dali:DistributionShape makes
    dct:license a *Violation*-severity mandatory IRI, and the submission UI
    only ever collects a single dataset-level license, so every distribution
    is published under it. Keyed by either the compact term or the full IRI,
    since piveau doesn't guarantee which form it serializes back (same reason
    as _asset_id_of)."""
    if not ds_node:
        return ""
    return _iri_of(ds_node.get("dct:license") or ds_node.get("http://purl.org/dc/terms/license"))


def _graph_nodes(doc: dict) -> list[dict]:
    """The node list of a fetched piveau record, whichever shape it came back in.

    piveau does not always wrap a record in @graph. A single-subject document —
    exactly what a freshly created dataset is, before it has any distributions —
    can come back as a flat JSON-LD node with its properties at the top level.
    Reading doc["@graph"] then yields nothing, which is what left the first
    distribution both unlinked and unstored ("no dcat:Dataset node found in the
    fetched graph")."""
    graph = doc.get("@graph")
    if isinstance(graph, list):
        return graph
    if isinstance(graph, dict):
        return [graph]
    node = {k: v for k, v in doc.items() if k != "@context"}
    return [node] if node else []


def _with_graph_nodes(doc: dict, nodes: list[dict]) -> dict:
    """Rebuild a fetched record around a new node list.

    Rebuilt rather than mutated so a flat document's top-level properties are
    dropped: leaving them alongside a new @graph would describe the same subject
    twice, once inline and once in the graph."""
    rebuilt: dict = {"@graph": nodes}
    ctx = doc.get("@context")
    if ctx is not None:
        rebuilt["@context"] = ctx
    return rebuilt


def _merge_context(doc: dict, terms: dict) -> None:
    """Ensure `terms` are bound in the document's @context.

    A fetched @context is usually an object, but JSON-LD also allows a string
    (a remote context) or an array. In those cases the terms are appended as an
    extra context rather than skipped — previously a non-object context meant
    the compact keys on an appended node had nothing to resolve against."""
    ctx = doc.get("@context")
    if isinstance(ctx, dict):
        for prefix, iri in terms.items():
            ctx.setdefault(prefix, iri)
        doc["@context"] = ctx
    elif ctx is None:
        doc["@context"] = dict(terms)
    elif isinstance(ctx, list):
        if not any(isinstance(entry, dict) and terms.keys() <= entry.keys() for entry in ctx):
            doc["@context"] = [*ctx, dict(terms)]
    else:
        doc["@context"] = [ctx, dict(terms)]


def _find_dataset_node(nodes: list[dict]) -> dict | None:
    return next((n for n in nodes if any("Dataset" in t for t in _node_types(n))), None)


def _count_distributions(graph: dict) -> int:
    return sum(1 for n in _graph_nodes(graph) if any("Distribution" in t for t in _node_types(n)))


async def next_distribution_id(dataset_id: str, catalogue_id: str) -> str:
    """The distribution_id the *next* add_distribution call on this dataset
    will get — needed by the caller up front, since the S3 object must be
    uploaded under "{this_id}.{ext}" before add_distribution itself runs."""
    graph = await _fetch_dataset_graph(dataset_id, catalogue_id)
    return str(_count_distributions(graph) + 1)


async def add_distribution(
    dataset_id: str, catalogue_id: str, distribution_id: str, asset_id: str,
    distribution_url: str, original_filename: str | None, media_type: str | None,
    metrics: DistributionMetrics, byte_size: int | None = None, ext: str | None = None,
) -> dict:
    """Step 2: append a new dcat:Distribution to an existing dataset's piveau
    record — fetch the current JSON-LD graph, add the node (+ link it from
    the dataset), PUT the whole graph back. Safe to call more than once per
    dataset; get `distribution_id` from next_distribution_id first so the
    caller can name the S3 object to match before this runs.

    `asset_id` is a UUID generated by the caller (routers/datasets.py) — it,
    not `distribution_id`, is written as dali:assetId and is what the S3
    object is named after. `distribution_id` only numbers/locates this node
    within the dataset's graph.

    `byte_size` is the uploaded file's size in bytes and `ext` its resolved
    extension — both are known only to the caller, and both are needed to
    satisfy dali:DistributionShape (dcat:byteSize, dct:format)."""
    graph = await _fetch_dataset_graph_with_dataset(dataset_id, catalogue_id)
    nodes = _graph_nodes(graph)

    # Match by rdf:type, not by comparing @id to a locally-computed
    # _dataset_uri(dataset_id) — piveau-hub-repo canonicalizes the dataset's
    # own resource URI itself on PUT, so an exact-URI comparison here
    # silently found no match against production records, which meant the
    # new distribution was appended as an orphan node with no
    # dcat:distribution link back to the dataset (confirmed against a real
    # dataset record: its existing distributions never showed up as a match).
    ds_node = _find_dataset_node(nodes)

    # piveau represents distributions as flat resources (.../set/distribution/{id}),
    # not nested under the dataset's own URI — also confirmed against production
    # records, which is why dist_uri is built from _distribution_uri, independent
    # of whatever the dataset's own (possibly rewritten) @id turned out to be.
    dist_uri = _distribution_uri(asset_id)

    dist_node: dict = {
        "@id": dist_uri,
        "@type": "dcat:Distribution",
        # Language-tagged: dali:DistributionShape requires dct:title to be an
        # rdf:langString, so a plain literal here is a Violation.
        "dct:title": {
            "@value": original_filename or f"Distribution {distribution_id}",
            "@language": "en",
        },
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

    # dct:license is mandatory on a distribution at Violation severity, and the
    # submission flow only collects one license, at dataset level — so inherit
    # it. Without this every distribution registered through the UI fails SHACL
    # validation, independently of which dataset it belongs to.
    license_iri = _dataset_license(ds_node)
    if license_iri:
        dist_node["dct:license"] = {"@id": license_iri}
    else:
        log.warning("[piveau] dataset %s has no readable dct:license — the new distribution "
                    "(asset_id=%s) will fail dali:DistributionShape's mandatory dct:license",
                    dataset_id, asset_id)

    # dct:format (an EU file-type authority IRI, distinct from dcat:mediaType)
    # and dcat:byteSize — both recommended by dali:DistributionShape. byteSize
    # is explicitly typed: the shape constrains it to xsd:nonNegativeInteger,
    # which a bare JSON number would not satisfy.
    format_iri = file_type_iri(ext) if ext else None
    if format_iri:
        dist_node["dct:format"] = {"@id": format_iri}
    if byte_size is not None:
        dist_node["dcat:byteSize"] = {
            "@value": str(byte_size),
            "@type": "http://www.w3.org/2001/XMLSchema#nonNegativeInteger",
        }

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

    graph = _with_graph_nodes(graph, nodes)
    _merge_context(graph, _JSONLD_CONTEXT)

    return await _put_dataset_graph(dataset_id, catalogue_id, graph, asset_id, distribution_id)


async def _put_dataset_graph(
    dataset_id: str, catalogue_id: str, graph: dict, asset_id: str, fallback_distribution_id: str = "1"
) -> dict:
    """PUT a dataset's whole JSON-LD graph back to piveau and report the id piveau
    actually assigned the distribution identified by `asset_id`. Shared by both
    ways of adding a distribution — the metadata-built one (add_distribution) and
    the client-submitted one (add_distribution_from_turtle)."""
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
            n for n in _graph_nodes(refetched)
            if any("Distribution" in t for t in _node_types(n)) and _asset_id_of(n) == asset_id
        ),
        None,
    )
    dist_uri = _distribution_uri(asset_id)
    if real_node is not None:
        real_uri = real_node.get("@id") or dist_uri
        piveau_distribution_id = real_uri.rstrip("/").rsplit("/", 1)[-1]
    else:
        log.warning("[piveau] could not find the just-added distribution (asset_id=%s) in the "
                    "re-fetched graph for %s — falling back to the locally-guessed id", asset_id, dataset_id)
        real_uri = dist_uri
        piveau_distribution_id = fallback_distribution_id

    return {
        "dataset_id":       dataset_id,
        "distribution_id":  piveau_distribution_id,
        "distribution_uri": real_uri,
        "status":           "submitted",
        "piveau_url":       url,
    }


def resolve_distribution_sentinels(
    turtle: str, asset_id: str, distribution_url: str, access_url: str
) -> str:
    """Replace a submitted distribution's placeholders with the values only this
    server can supply. Same passthrough approach as resolve_sentinels: the graph
    stored is the one the submitter was shown, apart from these four tokens."""
    return (
        turtle
        .replace(DISTRIBUTION_URI_SENTINEL, _distribution_uri(asset_id))
        .replace(DOWNLOAD_URL_SENTINEL, distribution_url)
        .replace(ACCESS_URL_SENTINEL, access_url)
        # Last: the asset-id token is a substring of nothing else, but resolving
        # it after the subject keeps the ordering obvious.
        .replace(ASSET_ID_SENTINEL, asset_id)
    )


def distribution_nodes_from_turtle(turtle: str, dist_uri: str) -> list[dict]:
    """Parse a submitted distribution document into JSON-LD node dicts, ready to
    append to the dataset's existing record.

    Only the distribution itself and its blank nodes (e.g. an spdx:Checksum) are
    kept. Anything else a client sends — statements about the dataset, about some
    other dataset, about a different distribution — is dropped rather than
    merged: this endpoint's contract is "describe THIS distribution", and without
    the filter a submitter could rewrite arbitrary parts of the catalogue by
    including extra subjects in the body.
    """
    try:
        graph = Graph().parse(data=turtle, format="turtle")
    # Deliberately broad: this is untrusted input, and rdflib surfaces malformed
    # Turtle as any of BadSyntax, ParserError, ValueError or a pyparsing error
    # depending on how it fails. All of them mean the same thing to the caller.
    except Exception as e:  # noqa: BLE001
        raise HTTPException(status_code=422, detail=f"Body is not valid Turtle: {e}")

    if (URIRef(dist_uri), None, None) not in graph:
        raise HTTPException(
            status_code=422,
            detail=(
                f"Turtle body must describe the distribution as "
                f"<{DISTRIBUTION_URI_SENTINEL}>, which is replaced with the "
                f"server-assigned distribution URI."
            ),
        )

    try:
        doc = json.loads(graph.serialize(format="json-ld", context=_JSONLD_CONTEXT, auto_compact=True))
    # Broad for the same reason, but a 500 rather than a 422: the Turtle already
    # parsed, so a failure here is ours, not the submitter's.
    except Exception as e:  # noqa: BLE001
        raise HTTPException(status_code=500, detail=f"Could not serialize submitted distribution: {e}")

    nodes = doc.get("@graph") or ([doc] if "@id" in doc or "@type" in doc else [])
    # Blank nodes carry no stable @id, so they are kept by virtue of not being a
    # named subject other than the distribution.
    kept, dropped = [], []
    for node in nodes:
        node_id = node.get("@id", "")
        if node_id == dist_uri or not node_id or node_id.startswith("_:"):
            node.pop("@context", None)
            kept.append(node)
        else:
            dropped.append(node_id)
    if dropped:
        log.warning("[piveau] ignoring %d non-distribution subject(s) in a submitted "
                    "distribution document: %s", len(dropped), dropped)
    if not kept:
        raise HTTPException(status_code=422, detail="No distribution statements found in the body")
    return kept


async def add_distribution_from_turtle(
    dataset_id: str, catalogue_id: str, asset_id: str, turtle: str,
    distribution_url: str, access_url: str,
) -> dict:
    """Step 2, RDF-first variant: append a client-supplied dcat:Distribution to
    an existing dataset's piveau record, instead of building the node here from
    the upload's metadata (see add_distribution).

    The write path is the same one add_distribution uses — fetch the dataset's
    JSON-LD graph, append the node, link it from the dataset, PUT the whole graph
    back — because that is what has been proven against production records. Only
    the node's *contents* come from the submitter.

    Returns the usual result plus `turtle`: the document as actually stored, with
    the sentinels resolved, so the caller can show the final graph rather than
    the placeholder one it sent.
    """
    if not turtle or not turtle.strip():
        raise HTTPException(status_code=422, detail="Empty Turtle body")
    if DISTRIBUTION_URI_SENTINEL not in turtle:
        raise HTTPException(
            status_code=422,
            detail=f"Turtle body must identify the distribution by <{DISTRIBUTION_URI_SENTINEL}>",
        )

    resolved = resolve_distribution_sentinels(turtle, asset_id, distribution_url, access_url)
    dist_uri = _distribution_uri(asset_id)
    new_nodes = distribution_nodes_from_turtle(resolved, dist_uri)

    graph = await _fetch_dataset_graph_with_dataset(dataset_id, catalogue_id)
    nodes = _graph_nodes(graph)
    ds_node = _find_dataset_node(nodes)

    nodes = [*nodes, *new_nodes]

    # The dataset's side of the link. The submitted body describes only the
    # distribution (its own dcat:distribution statement, if any, was filtered
    # out above), so the link is added here exactly as add_distribution does.
    if ds_node is not None:
        existing = ds_node.get("dcat:distribution")
        refs = [] if existing is None else (existing if isinstance(existing, list) else [existing])
        refs.append({"@id": dist_uri})
        ds_node["dcat:distribution"] = refs
    else:
        log.warning("[piveau] no dcat:Dataset node found in the fetched graph for %s — "
                    "dcat:distribution link not added, only the distribution node itself", dataset_id)

    graph = _with_graph_nodes(graph, nodes)
    _merge_context(graph, _JSONLD_CONTEXT)

    piveau_result = await _put_dataset_graph(dataset_id, catalogue_id, graph, asset_id)
    piveau_result["turtle"] = resolved
    return piveau_result


async def list_asset_ids(dataset_id: str, catalogue_id: str) -> list[str]:
    """The dali:assetId of every distribution currently on this dataset —
    used by routers/datasets.py's dataset-delete endpoint to know which S3
    objects and EDC assets to clean up *before* the piveau record (which is
    the only place this list is readable from) is deleted."""
    graph = await _fetch_dataset_graph(dataset_id, catalogue_id)
    nodes = _graph_nodes(graph)
    return [
        asset_id for n in nodes
        if any("Distribution" in t for t in _node_types(n)) and (asset_id := _asset_id_of(n))
    ]


async def delete_distribution(dataset_id: str, catalogue_id: str, asset_id: str) -> dict:
    """Remove one dcat:Distribution node (matched by dali:assetId, same
    stable-handle convention as add_distribution/publish_quality_to_piveau)
    from a dataset's piveau graph: unlinks it from the dataset's own
    dcat:distribution list, deletes the node itself and any
    dqv:QualityMeasurement nodes attached to it (see
    dali.dataspace.publish_quality_to_piveau, which creates those under
    "{dist_uri}/quality/..."), then PUTs the graph back."""
    graph = await _fetch_dataset_graph(dataset_id, catalogue_id)
    nodes = _graph_nodes(graph)

    dist_node = next(
        (n for n in nodes if any("Distribution" in t for t in _node_types(n)) and _asset_id_of(n) == asset_id),
        None,
    )
    if dist_node is None:
        return {"dataset_id": dataset_id, "asset_id": asset_id, "status": "not_found"}

    dist_uri = dist_node["@id"]
    nodes = [
        n for n in nodes
        if n.get("@id") != dist_uri and not str(n.get("@id", "")).startswith(f"{dist_uri}/quality/")
    ]

    ds_node = _find_dataset_node(nodes)
    if ds_node is not None:
        existing = ds_node.get("dcat:distribution")
        refs = [] if existing is None else (existing if isinstance(existing, list) else [existing])
        refs = [r for r in refs if r.get("@id") != dist_uri]
        if refs:
            ds_node["dcat:distribution"] = refs
        else:
            ds_node.pop("dcat:distribution", None)

    graph = _with_graph_nodes(graph, nodes)
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

    return {"dataset_id": dataset_id, "asset_id": asset_id, "distribution_uri": dist_uri, "status": "deleted"}


async def delete_dataset(dataset_id: str, catalogue_id: str) -> dict:
    """DELETE the whole dataset record from piveau — its distribution nodes
    live inside the same record (see add_distribution), so this removes them
    too. Mirrors piveau_service_client.deregister_service's DELETE pattern."""
    _require_piveau_config()
    url = f"{PIVEAU_HUB_URL}/datasets/{dataset_id}?catalogue={catalogue_id}"
    headers = {"X-API-Key": PIVEAU_API_KEY}
    try:
        async with httpx.AsyncClient(timeout=15) as client:
            r = await client.delete(url, headers=headers)
        r.raise_for_status()
    except httpx.HTTPStatusError as e:
        if e.response.status_code == 404:
            return {"dataset_id": dataset_id, "status": "not_found"}
        raise HTTPException(
            status_code=e.response.status_code,
            detail=f"piveau error for {dataset_id}: {e.response.status_code} {e.response.text[:500]}",
        )
    except httpx.RequestError as e:
        raise HTTPException(status_code=502, detail=f"Could not reach piveau at {PIVEAU_HUB_URL}: {e}")
    return {"dataset_id": dataset_id, "status": "deleted"}
