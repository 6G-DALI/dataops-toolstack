"""Creates a testbed's catalogue in piveau-hub-repo (the catalogue id is the bucket name)."""

import httpx
from fastapi import HTTPException

import piveau_dataset_client as pdc


def _turtle(catalogue_id: str, title: str, description: str, publisher: str) -> str:
    esc = pdc._esc
    return (
        "@prefix dcat: <http://www.w3.org/ns/dcat#> .\n"
        "@prefix dct: <http://purl.org/dc/terms/> .\n"
        "@prefix foaf: <http://xmlns.com/foaf/0.1/> .\n\n"
        f"<{pdc.DSPACE_BASE}/set/catalogue/{catalogue_id}>\n"
        "    a dcat:Catalog ;\n"
        f'    dct:title "{esc(title)}"@en ;\n'
        f'    dct:description "{esc(description)}"@en ;\n'
        f'    dct:publisher [ a foaf:Agent ; foaf:name "{esc(publisher)}" ] .\n'
    )


def ensure_catalogue(catalogue_id: str, title: str, description: str, publisher: str) -> str:
    """Create the catalogue if absent. Returns 'created' or 'exists'."""
    pdc._require_piveau_config()
    base = pdc.PIVEAU_HUB_URL.rstrip("/")
    headers = {"X-API-Key": pdc.PIVEAU_API_KEY}
    try:
        with httpx.Client(timeout=15) as c:
            if c.get(f"{base}/catalogues/{catalogue_id}", headers=headers).status_code == 200:
                return "exists"
            r = c.put(
                f"{base}/catalogues/{catalogue_id}",
                content=_turtle(catalogue_id, title, description, publisher).encode(),
                headers={**headers, "Content-Type": "text/turtle"},
            )
            r.raise_for_status()
    except httpx.HTTPStatusError as e:
        raise HTTPException(
            status_code=502,
            detail=f"piveau refused catalogue '{catalogue_id}': {e.response.status_code} {e.response.text[:300]}",
        )
    except httpx.RequestError as e:
        raise HTTPException(status_code=502, detail=f"Could not reach piveau at {pdc.PIVEAU_HUB_URL}: {e}")
    return "created"
