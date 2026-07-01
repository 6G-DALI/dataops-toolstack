import importlib.util
import os
import sys

import asyncio
from fastapi import APIRouter, HTTPException

import piveau_service_client as psc

router = APIRouter(prefix="/services", tags=["Services"])

PLUGINS_PATH = os.getenv("AIRFLOW_PLUGINS_FOLDER", "/opt/airflow/plugins")


def _load_registry() -> dict:
    """Import dali.registry from the plugins folder at runtime."""
    registry_path = os.path.join(PLUGINS_PATH, "dali", "registry.py")
    if not os.path.exists(registry_path):
        raise HTTPException(status_code=503, detail=f"Registry not found at {registry_path}")
    spec = importlib.util.spec_from_file_location("dali.registry", registry_path)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod.SERVICES


@router.post("/debug-ping")
async def debug_ping():
    """Send a minimal Turtle to piveau to test connectivity and auth."""
    import httpx
    minimal_turtle = (
        "@prefix dcat: <http://www.w3.org/ns/dcat#> .\n"
        "@prefix dct:  <http://purl.org/dc/terms/> .\n"
        f"<{psc.DSPACE_BASE}/set/service/debug-ping>\n"
        '    a dcat:Dataset , dcat:DataService ;\n'
        '    dct:title "Debug Ping"@en ;\n'
        f'    dcat:endpointURL <{psc.ORCHESTRATOR_URL}/ping> .\n'
    )
    url = f"{psc.PIVEAU_HUB_URL}/datasets/debug-ping?catalogue={psc.PIVEAU_CATALOGUE}"
    headers = {"X-API-Key": psc.PIVEAU_API_KEY, "Content-Type": "text/turtle"}
    print(f"[debug] PUT {url}")
    print(f"[debug] Headers: { {k: v if k != 'X-API-Key' else v[:4] + '...' for k, v in headers.items()} }")
    print(f"[debug] Turtle:\n{minimal_turtle}")
    try:
        async with httpx.AsyncClient(timeout=15) as client:
            r = await client.put(url, content=minimal_turtle.encode(), headers=headers)
        print(f"[debug] Response {r.status_code}: {r.text!r}")
        return {"status": r.status_code, "body": r.text}
    except Exception as e:
        return {"error": str(e)}


@router.get("")
async def list_services():
    """List all available reusable dali task services from the plugin registry."""
    services = _load_registry()
    return {
        "services": [
            {"service_id": sid, **meta}
            for sid, meta in services.items()
        ],
        "total": len(services),
    }


@router.get("/{service_id}")
async def get_service(service_id: str):
    """Get metadata for a specific service."""
    services = _load_registry()
    if service_id not in services:
        raise HTTPException(status_code=404, detail=f"Service '{service_id}' not found")
    return {"service_id": service_id, **services[service_id]}


@router.post("/register")
async def register_all_services():
    """Register all dali task services as dcat:DataService records in piveau."""
    services = _load_registry()
    results = await asyncio.gather(
        *[psc.register_service(sid, meta) for sid, meta in services.items()],
        return_exceptions=True,
    )
    response = []
    for sid, result in zip(services.keys(), results):
        if isinstance(result, Exception):
            response.append({"service_id": sid, "status": "error", "detail": str(result)})
        else:
            response.append(result)
    return {"results": response}


@router.post("/{service_id}/register")
async def register_service(service_id: str):
    """Register a single dali task service as a dcat:DataService record in piveau."""
    services = _load_registry()
    if service_id not in services:
        raise HTTPException(status_code=404, detail=f"Service '{service_id}' not found")
    return await psc.register_service(service_id, services[service_id])


@router.delete("/{service_id}/register")
async def deregister_service(service_id: str):
    """Remove a dcat:DataService record from piveau."""
    services = _load_registry()
    if service_id not in services:
        raise HTTPException(status_code=404, detail=f"Service '{service_id}' not found")
    return await psc.deregister_service(service_id)