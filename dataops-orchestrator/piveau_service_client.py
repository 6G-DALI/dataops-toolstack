"""
Registers and manages dcat:DataService records in piveau-hub.
Each reusable dali task is described as a dcat:DataService and PUT
to the catalogue so it can be discovered and searched.
"""

import logging
import os

import httpx
from fastapi import HTTPException

log = logging.getLogger(__name__)

PIVEAU_HUB_URL    = os.getenv("PIVEAU_HUB_URL", "")
PIVEAU_API_KEY    = os.getenv("PIVEAU_API_KEY", "")
PIVEAU_CATALOGUE  = os.getenv("PIVEAU_SERVICES_CATALOGUE", "6g-dali-services")
DSPACE_BASE       = os.getenv("DSPACE_BASE_URL", "https://dataspace.6gdali.eu")
CODE_REPO         = os.getenv("CODE_REPOSITORY_URL", "")
PUBLISHER_NAME    = os.getenv("PUBLISHER_NAME", "6G-DALI")
ORCHESTRATOR_URL  = os.getenv("ORCHESTRATOR_URL", "http://localhost:8000")

_DALI_NS    = "https://dali-project.eu/ns#"
_MIT        = "https://opensource.org/licenses/MIT"
_RESTRICTED = "http://publications.europa.eu/resource/authority/access-right/RESTRICTED"


def _service_uri(service_id: str) -> str:
    return f"{DSPACE_BASE}/set/service/{service_id}"


def _build_turtle(service_id: str, meta: dict) -> str:
    uri = _service_uri(service_id)
    title = meta["title"].replace('"', '\\"')
    description = meta["description"].replace('"', '\\"')
    service_type = meta["service_type"]
    framework = meta.get("framework") or ""
    input_fmt = meta.get("input_format") or ""
    output_fmt = meta.get("output_format") or ""
    module = meta.get("module", "")
    function = meta.get("function", "")

    endpoint_url = f"{ORCHESTRATOR_URL}/services/{service_id}/register"

    lines = [
        "@prefix rdf:    <http://www.w3.org/1999/02/22-rdf-syntax-ns#> .",
        "@prefix dcat:   <http://www.w3.org/ns/dcat#> .",
        "@prefix dct:    <http://purl.org/dc/terms/> .",
        "@prefix foaf:   <http://xmlns.com/foaf/0.1/> .",
        "@prefix dali:   <https://dali-project.eu/ns#> .",
        "@prefix adms:   <http://www.w3.org/ns/adms#> .",
        "@prefix schema: <https://schema.org/> .",
        "@prefix xsd:    <http://www.w3.org/2001/XMLSchema#> .",
        "",
        f"<{uri}>",
        "    rdf:type             dcat:Dataset , dcat:DataService ;",
        f'    dct:title            "{title}"@en ;',
        f'    dct:description      "{description}"@en ;',
        f"    dcat:endpointURL     <{endpoint_url}> ;",
        f'    dct:license          <{_MIT}> ;',
        f'    dct:accessRights     <{_RESTRICTED}> ;',
        f'    dali:serviceType     "{service_type}" ;',
    ]

    if input_fmt:
        lines.append(f'    dali:inputFormat     "{input_fmt}" ;')
    if output_fmt:
        lines.append(f'    dali:outputFormat    "{output_fmt}" ;')
    if framework:
        lines.append(f'    dali:framework       "{framework}" ;')
    if module and function:
        lines.append(f'    dali:taskModule      "{module}" ;')
        lines.append(f'    dali:taskFunction    "{function}" ;')

    lines.append(f'    dct:publisher        [ rdf:type foaf:Organization ; foaf:name "{PUBLISHER_NAME}" ]')
    if CODE_REPO:
        lines[-1] += " ;"
        lines.append(f"    schema:codeRepository <{CODE_REPO}> .")
    else:
        lines[-1] += " ."

    turtle = "\n".join(lines)
    print(f"[piveau] Turtle for {service_id}:\n{turtle}")
    return turtle


async def register_service(service_id: str, meta: dict) -> dict:
    """PUT a single dcat:DataService record into piveau-hub."""
    if not PIVEAU_HUB_URL or not PIVEAU_API_KEY:
        raise HTTPException(status_code=503, detail="PIVEAU_HUB_URL or PIVEAU_API_KEY not configured")
    turtle = _build_turtle(service_id, meta)
    url = f"{PIVEAU_HUB_URL}/datasets/{service_id}?catalogue={PIVEAU_CATALOGUE}"
    headers = {
        "X-API-Key":    PIVEAU_API_KEY,
        "Content-Type": "text/turtle",
    }
    try:
        async with httpx.AsyncClient(timeout=15) as client:
            r = await client.put(url, content=turtle.encode(), headers=headers)
        r.raise_for_status()
    except httpx.HTTPStatusError as e:
        print(f"[piveau] {e.response.status_code} body: {e.response.text!r}")
        raise HTTPException(status_code=e.response.status_code,
                            detail=f"piveau error for {service_id}: {e.response.status_code} {e.response.text[:500]}")
    except httpx.RequestError as e:
        raise HTTPException(status_code=502, detail=f"Could not reach piveau at {PIVEAU_HUB_URL}: {e}")
    return {"service_id": service_id, "status": "registered", "piveau_url": url}


async def deregister_service(service_id: str) -> dict:
    """DELETE a dcat:DataService record from piveau-hub."""
    if not PIVEAU_HUB_URL or not PIVEAU_API_KEY:
        raise HTTPException(status_code=503, detail="PIVEAU_HUB_URL or PIVEAU_API_KEY not configured")
    url = f"{PIVEAU_HUB_URL}/datasets/{service_id}?catalogue={PIVEAU_CATALOGUE}"
    headers = {"X-API-Key": PIVEAU_API_KEY}
    try:
        async with httpx.AsyncClient(timeout=15) as client:
            r = await client.delete(url, headers=headers)
        r.raise_for_status()
    except httpx.HTTPStatusError as e:
        print(f"[piveau] {e.response.status_code} body: {e.response.text!r}")
        raise HTTPException(status_code=e.response.status_code,
                            detail=f"piveau error for {service_id}: {e.response.status_code} {e.response.text[:500]}")
    except httpx.RequestError as e:
        raise HTTPException(status_code=502, detail=f"Could not reach piveau at {PIVEAU_HUB_URL}: {e}")
    return {"service_id": service_id, "status": "deregistered"}
