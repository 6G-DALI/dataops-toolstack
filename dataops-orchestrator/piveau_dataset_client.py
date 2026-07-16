"""
Builds and submits a full 6G-DALI MAP dataset record (DCAT-AP + GAIA-X +
CMT testbed-context fields) to the piveau-hub Staging Catalogue, following
the same PUT {PIVEAU_HUB_URL}/datasets/{id}?catalogue={catalogue} pattern
already used for service registration (see piveau_service_client.py).

`catalogue_id` doubles as both the piveau catalogue name and the Data Lake
S3 bucket the file was uploaded to, matching the convention every DataOps
DAG already assumes (see dali_dataspace_validate_dataset's `catalogue_id`
param).
"""

import logging
import os
from datetime import datetime, timezone

import httpx
from fastapi import HTTPException

from dataset_models import DatasetSubmission

log = logging.getLogger(__name__)

PIVEAU_HUB_URL         = os.getenv("PIVEAU_HUB_URL", "")
PIVEAU_API_KEY         = os.getenv("PIVEAU_API_KEY", "")
DSPACE_BASE            = os.getenv("DSPACE_BASE_URL", "https://dataspace.6gdali.eu")
PUBLISHER_NAME_DEFAULT = os.getenv("PUBLISHER_NAME", "6G-DALI")
DATASPACE_S3_ENDPOINT_URL = os.getenv("DATASPACE_S3_ENDPOINT_URL", "")

# Every submission creates exactly one distribution, always numbered "1" —
# this is also the distribution_id the validate DAG is triggered with right
# after submission (see routers/datasets.py:submit_dataset), so the two stay
# in sync without hardcoding "1" in two places.
FIRST_DISTRIBUTION_ID = "1"

# Kept in sync with dali/utils.py's EXTENSION_BY_MEDIA_TYPE on the Airflow
# side: the validate DAG resolves a distribution's S3 object filename as
# "{distribution_id}.{ext}" from its dcat:mediaType rather than taking a
# filename param, so the object must be *uploaded* under that same name here.
EXTENSION_BY_MEDIA_TYPE = {
    "text/csv":                     "csv",
    "text/tab-separated-values":    "tsv",
    "application/json":             "json",
    "application/ld+json":          "jsonld",
    "text/plain":                   "txt",
    "application/xml":              "xml",
    "text/xml":                     "xml",
    "application/parquet":          "parquet",
    "application/octet-stream":     "bin",
}


def extension_for_media_type(media_type: str | None) -> str:
    return EXTENSION_BY_MEDIA_TYPE.get((media_type or "").lower().strip(), "dat")

_ACCESS_RIGHTS = {
    "PUBLIC":     "http://publications.europa.eu/resource/authority/access-right/PUBLIC",
    "RESTRICTED": "http://publications.europa.eu/resource/authority/access-right/RESTRICTED",
    "NON_PUBLIC": "http://publications.europa.eu/resource/authority/access-right/NON_PUBLIC",
}


def _esc(s: str) -> str:
    return s.replace("\\", "\\\\").replace('"', '\\"')


def _dataset_uri(dataset_id: str) -> str:
    return f"{DSPACE_BASE}/set/data/{dataset_id}"


def _testbed_context_block(tc) -> list[str]:
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
    return lines


def build_turtle(dataset_id: str, sub: DatasetSubmission, distribution_url: str | None, media_type: str | None) -> str:
    uri = _dataset_uri(dataset_id)
    ident = sub.identity
    obj = sub.object
    metrics = sub.metrics
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

    tc_lines = _testbed_context_block(sub.testbed_context)
    if metrics.observation_point_horizontal:
        tc_lines.append(f'        dali:observationPointHorizontal "{_esc(metrics.observation_point_horizontal)}" ;')
    if metrics.observation_point_vertical:
        tc_lines.append(f'        dali:observationPointVertical "{_esc(metrics.observation_point_vertical)}" ;')
    for fam in metrics.measurement_family:
        tc_lines.append(f'        dali:measurementFamily "{_esc(fam)}" ;')
    for tool in metrics.measurement_tool:
        tc_lines.append(f'        dali:measurementTool "{_esc(tool)}" ;')

    if tc_lines:
        lines.append("    dali:testbedContext     [")
        lines.append("        rdf:type dali:TestbedContext ;")
        lines.extend(tc_lines)
        lines.append("    ] ;")

    if distribution_url:
        dist_uri = f"{uri}/distribution/{FIRST_DISTRIBUTION_ID}"
        lines.append(f"    dcat:distribution       <{dist_uri}> ;")

    # close the dataset resource
    lines[-1] = lines[-1].rstrip(" ;") + " ."

    if distribution_url:
        lines += [
            "",
            f"<{dist_uri}>",
            "    rdf:type       dcat:Distribution ;",
            f'    dct:title      "{_esc(ident.title)} - distribution"@en ;',
            f"    dcat:accessURL <{distribution_url}> ;",
            # Identifies the underlying file — this, not the distribution's own
            # URI/distribution_id, is what the validate DAG's resolve_asset_title
            # prefixes with the file extension to find the S3 object (see
            # dali/dataspace.py). Matches FIRST_DISTRIBUTION_ID since that's also
            # the object's actual basename at upload time (routers/datasets.py).
            f'    dali:assetId   "{FIRST_DISTRIBUTION_ID}" ;',
        ]
        if media_type:
            lines.append(f'    dcat:mediaType "{media_type}" ;')
        # Measured variables/technique describe this distribution's file
        # specifically, not the dataset as a whole (see MAP §5.3.E/§5.6) —
        # placed here rather than on the dataset resource above.
        for var in metrics.variable_measured:
            lines.append(f'    schema:variableMeasured "{_esc(var)}" ;')
        if metrics.measurement_technique:
            lines.append(f'    schema:measurementTechnique "{_esc(metrics.measurement_technique)}"@en ;')
        lines[-1] = lines[-1].rstrip(" ;") + " ."

    turtle = "\n".join(lines)
    return turtle


async def submit_dataset(
    dataset_id: str, catalogue_id: str, sub: DatasetSubmission, distribution_url: str | None, media_type: str | None
) -> dict:
    if not PIVEAU_HUB_URL or not PIVEAU_API_KEY:
        raise HTTPException(status_code=503, detail="PIVEAU_HUB_URL or PIVEAU_API_KEY not configured")

    turtle = build_turtle(dataset_id, sub, distribution_url, media_type)
    log.info("[piveau] Turtle for dataset %s:\n%s", dataset_id, turtle)

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

    return {"dataset_id": dataset_id, "dataset_uri": _dataset_uri(dataset_id), "status": "submitted", "piveau_url": url}