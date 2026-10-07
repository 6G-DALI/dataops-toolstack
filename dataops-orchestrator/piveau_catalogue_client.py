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


def count_datasets(catalogue_id: str) -> int:
    """How many datasets the search index holds for the catalogue."""
    import piveau_client as pc
    try:
        r = httpx.get(f"{pc.PIVEAU_URL.rstrip('/')}/search",
                      params={"filter": "dataset", "catalog": catalogue_id, "limit": 1}, timeout=15)
        r.raise_for_status()
        return int(r.json().get("result", {}).get("count", 0))
    except (httpx.HTTPError, ValueError) as e:
        raise HTTPException(status_code=502, detail=f"Could not count the datasets of '{catalogue_id}' in piveau: {e}")


def delete_catalogue(catalogue_id: str) -> dict:
    """Delete the catalogue from piveau-hub-repo. piveau removes the datasets inside it with it."""
    pdc._require_piveau_config()
    url = f"{pdc.PIVEAU_HUB_URL.rstrip('/')}/catalogues/{catalogue_id}"
    try:
        r = httpx.delete(url, headers={"X-API-Key": pdc.PIVEAU_API_KEY}, timeout=60)
    except httpx.RequestError as e:
        raise HTTPException(status_code=502, detail=f"Could not reach piveau at {pdc.PIVEAU_HUB_URL}: {e}")
    if r.status_code == 404:
        return {"status": "not_found"}
    if r.status_code >= 400:
        raise HTTPException(status_code=502, detail=f"piveau refused to delete catalogue '{catalogue_id}': {r.status_code} {r.text[:300]}")
    return {"status": "deleted"}


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
