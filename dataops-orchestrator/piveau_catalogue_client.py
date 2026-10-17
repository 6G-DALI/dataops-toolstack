"""Creates a testbed's catalogue in piveau-hub-repo (the catalogue id is the bucket name)."""

import re

import httpx
from fastapi import HTTPException
from rdflib import Graph, URIRef
from rdflib.namespace import DCAT, FOAF, RDF

import piveau_dataset_client as pdc

# A testbed's logo is the catalogue's foaf:logo: a plain image URL, as on the KUL and ISI catalogues.
# It is written into Turtle as <iri>, so anything that could close or escape the IRI is refused.
_LOGO_URL_RE = re.compile(r'^https?://[^\s<>"{}|\\^`]+$')


def clean_logo_url(value: str | None) -> str | None:
    """The trimmed logo URL, None for blank (no logo). Raises ValueError for anything that is not an http(s) URL."""
    url = (value or "").strip()
    if not url:
        return None
    if not _LOGO_URL_RE.match(url):
        raise ValueError("logo_url must be an http(s) URL to an image, e.g. https://example.org/logo.png")
    return url


def _turtle(catalogue_id: str, title: str, description: str, publisher: str, logo_url: str | None = None) -> str:
    esc = pdc._esc
    logo = f"    foaf:logo <{logo_url}> ;\n" if logo_url else ""
    return (
        "@prefix dcat: <http://www.w3.org/ns/dcat#> .\n"
        "@prefix dct: <http://purl.org/dc/terms/> .\n"
        "@prefix foaf: <http://xmlns.com/foaf/0.1/> .\n\n"
        f"<{pdc.DSPACE_BASE}/set/catalogue/{catalogue_id}>\n"
        "    a dcat:Catalog ;\n"
        f'    dct:title "{esc(title)}"@en ;\n'
        f'    dct:description "{esc(description)}"@en ;\n'
        f"{logo}"
        f'    dct:publisher [ a foaf:Agent ; foaf:name "{esc(publisher)}" ] .\n'
    )


def with_logo(turtle: str, logo_url: str | None) -> str:
    """The catalogue's Turtle with its foaf:logo set to `logo_url` (removed when None). Everything else, including
    the publisher and any other properties piveau holds, is kept as it is."""
    g = Graph()
    g.parse(data=turtle, format="turtle")
    catalogues = list(g.subjects(RDF.type, DCAT.Catalog))
    if len(catalogues) != 1:
        raise ValueError(f"expected one dcat:Catalog in the catalogue record, found {len(catalogues)}")
    node = catalogues[0]
    g.remove((node, FOAF.logo, None))
    if logo_url:
        g.add((node, FOAF.logo, URIRef(logo_url)))
    return g.serialize(format="turtle")


def _logos(turtle: str) -> set[str]:
    g = Graph()
    g.parse(data=turtle, format="turtle")
    return {str(o) for o in g.objects(None, FOAF.logo)}


def set_catalogue_logo(catalogue_id: str, logo_url: str | None) -> str:
    """Set (or, with None, remove) the logo of an existing catalogue. Reads the catalogue from piveau, changes only
    foaf:logo and writes it back; the datasets in the catalogue are not touched. Returns 'set', 'removed' or
    'unchanged'."""
    pdc._require_piveau_config()
    base = pdc.PIVEAU_HUB_URL.rstrip("/")
    headers = {"X-API-Key": pdc.PIVEAU_API_KEY}
    try:
        with httpx.Client(timeout=15) as c:
            r = c.get(f"{base}/catalogues/{catalogue_id}", headers={**headers, "Accept": "text/turtle"})
            if r.status_code == 404:
                raise HTTPException(status_code=404,
                                    detail=f"Catalogue '{catalogue_id}' does not exist in piveau yet: run provisioning first")
            r.raise_for_status()
            try:
                if _logos(r.text) == ({logo_url} if logo_url else set()):
                    return "unchanged"
                updated = with_logo(r.text, logo_url)
            except ValueError as e:  # rdflib's parse errors are ValueErrors too
                raise HTTPException(status_code=502, detail=f"Could not update catalogue '{catalogue_id}': {e}")
            r = c.put(f"{base}/catalogues/{catalogue_id}", content=updated.encode(),
                      headers={**headers, "Content-Type": "text/turtle"})
            r.raise_for_status()
    except httpx.HTTPStatusError as e:
        raise HTTPException(
            status_code=502,
            detail=f"piveau refused the logo for catalogue '{catalogue_id}': {e.response.status_code} {e.response.text[:300]}",
        )
    except httpx.RequestError as e:
        raise HTTPException(status_code=502, detail=f"Could not reach piveau at {pdc.PIVEAU_HUB_URL}: {e}")
    return "set" if logo_url else "removed"


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


def ensure_catalogue(catalogue_id: str, title: str, description: str, publisher: str,
                     logo_url: str | None = None) -> str:
    """Create the catalogue if absent, with `logo_url` as its logo. Returns 'created' or 'exists'. An existing
    catalogue is left as it is: use set_catalogue_logo to change its logo."""
    pdc._require_piveau_config()
    base = pdc.PIVEAU_HUB_URL.rstrip("/")
    headers = {"X-API-Key": pdc.PIVEAU_API_KEY}
    try:
        with httpx.Client(timeout=15) as c:
            if c.get(f"{base}/catalogues/{catalogue_id}", headers=headers).status_code == 200:
                return "exists"
            r = c.put(
                f"{base}/catalogues/{catalogue_id}",
                content=_turtle(catalogue_id, title, description, publisher, logo_url).encode(),
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
