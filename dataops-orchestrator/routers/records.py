"""
A catalogue record's RDF, for the catalogue's "Linked data" view (Turtle, JSON-LD, RDF/XML, N-Triples).

The catalogue used to link to piveau's <id>.ttl / .jsonld files directly. A link opened in a tab carries no
Authorization header, so any record that is not public answered 403. The catalogue now asks here instead, with the
user's token when signed in. The record is read from piveau as the caller (or anonymously), never with the
orchestrator's API key, so piveau's access control decides who sees what.
"""

from urllib.parse import quote

import httpx
from fastapi import APIRouter, HTTPException, Query, Request, Response

from piveau_dataset_client import PIVEAU_HUB_URL

router = APIRouter(prefix="/records", tags=["Records"])

# The serialisations piveau's hub-repo can negotiate, by the name the catalogue uses for them.
FORMATS = {
    "ttl": "text/turtle",
    "jsonld": "application/ld+json",
    "rdf": "application/rdf+xml",
    "nt": "application/n-triples",
    "n3": "text/n3",
}


@router.get("/{dataset_id}")
async def get_record(
    dataset_id: str,
    request: Request,
    format: str = Query("ttl", description="ttl, jsonld, rdf, nt or n3"),
    catalogue: str | None = Query(None, description="The record's catalogue, when known"),
):
    """The dataset's RDF in the requested serialisation, as the caller may see it.

    Works signed in or not. 404 when the record does not exist or is not visible to the caller (piveau's 403 is not
    passed on, so a hidden record is not told apart from a missing one), 401 when piveau rejects the token, 422 for an
    unknown format.
    """
    accept = FORMATS.get(format)
    if accept is None:
        raise HTTPException(status_code=422, detail=f"Unknown format; use one of {', '.join(FORMATS)}")
    if not PIVEAU_HUB_URL:
        raise HTTPException(status_code=503, detail="PIVEAU_HUB_URL not configured")

    headers = {"Accept": accept}
    authorization = request.headers.get("authorization") or ""
    if authorization.lower().startswith("bearer "):
        headers["Authorization"] = authorization
    url = f"{PIVEAU_HUB_URL.rstrip('/')}/datasets/{quote(dataset_id, safe='')}"
    try:
        async with httpx.AsyncClient(timeout=20) as client:
            r = await client.get(url, params={"catalogue": catalogue} if catalogue else None, headers=headers)
    except httpx.RequestError as e:
        raise HTTPException(status_code=502, detail=f"Could not reach the catalogue: {e}")
    if r.status_code == 401 and "Authorization" in headers:
        raise HTTPException(status_code=401, detail="The catalogue did not accept your sign-in; sign in again")
    if r.status_code in (401, 403, 404):
        raise HTTPException(status_code=404, detail="Record not found, or not visible to you")
    if r.status_code >= 400:
        raise HTTPException(status_code=502, detail=f"The catalogue answered {r.status_code}")
    # Never cached by a shared proxy: what comes back depends on who asked.
    return Response(content=r.content, media_type=r.headers.get("content-type", accept).split(";")[0],
                    headers={"Cache-Control": "private, no-store"})
