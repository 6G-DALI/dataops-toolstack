"""
The datasets of a testbed's catalogue, for the testbed page: list them, and edit their main fields and visibility.

Reads and writes go to piveau-hub-repo with the API key, so the testbed's administrators see every dataset of their
catalogue, private ones included. An edit is a read-modify-write of the dataset's whole graph (Turtle): piveau replaces
a dataset on a PUT, so sending only the changed fields would drop its distributions. The visibility is piveau's
access level (access control v2), read from the dataset's catalogue record and set with the PUT's `accessLevel`
parameter; dct:accessRights is kept in step with it.

Only the catalogue is changed. A dataset uploaded by a testbed keeps its metadata.json in the bucket as the testbed
shared it, so re-registering the dataset from it (the asset monitor's status page) undoes edits made here.
"""

import re

import httpx
from fastapi import HTTPException
from rdflib import Graph, Literal, URIRef
from rdflib.namespace import DCAT, DCTERMS, RDF

import piveau_access
import piveau_dataset_client as pdc

LEVELS = piveau_access.LEVELS
_RIGHT_OF_LEVEL = piveau_access.RIGHT_OF_LEVEL
# The record's piveau:accessLevel, prefixed or as a full IRI (piveau does not promise which it serializes).
_LEVEL_RE = re.compile(r"(?:piveau:|ns/voc#)accessLevel>?\s+(?:piveau:|<https://piveau\.eu/ns/voc#)(\w+)")
_URL_RE = re.compile(r"^https?://\S+$")


def _hub() -> str:
    pdc._require_piveau_config()
    return pdc.PIVEAU_HUB_URL.rstrip("/")


def _call(method: str, path: str, *, accept: str = "text/turtle", body: str | None = None, params: dict | None = None):
    headers = {"X-API-Key": pdc.PIVEAU_API_KEY, "Accept": accept}
    if body is not None:
        headers["Content-Type"] = "text/turtle"
    try:
        return httpx.request(method, f"{_hub()}{path}", params=params, headers=headers,
                             content=body.encode() if body is not None else None, timeout=30)
    except httpx.HTTPError as e:
        raise HTTPException(status_code=502, detail=f"Could not reach piveau: {e}")


def _graph(catalogue_id: str, dataset_id: str) -> tuple[Graph, URIRef]:
    r = _call("GET", f"/datasets/{dataset_id}", params={"catalogue": catalogue_id})
    if r.status_code == 404:
        raise HTTPException(status_code=404, detail=f"Dataset '{dataset_id}' is not in catalogue '{catalogue_id}'")
    if r.status_code != 200:
        raise HTTPException(status_code=502, detail=f"piveau answered {r.status_code} for dataset '{dataset_id}': {r.text[:200]}")
    g = Graph().parse(data=r.text, format="turtle")
    subject = next(g.subjects(RDF.type, DCAT.Dataset), None)
    if subject is None:
        raise HTTPException(status_code=502, detail=f"piveau returned no dcat:Dataset for '{dataset_id}'")
    return g, subject


def _level(catalogue_id: str, dataset_id: str) -> str | None:
    """The dataset's piveau access level, from its catalogue record; None without access control (or unreadable)."""
    r = _call("GET", f"/datasets/{dataset_id}/record", params={"catalogue": catalogue_id})
    m = _LEVEL_RE.search(r.text) if r.status_code == 200 else None
    return m.group(1) if m else None


def _text(g: Graph, s: URIRef, p: URIRef) -> str:
    values = list(g.objects(s, p))
    english = [v for v in values if isinstance(v, Literal) and v.language in ("en", None)]
    return str((english or values or [""])[0])


def summary(catalogue_id: str, dataset_id: str) -> dict:
    g, s = _graph(catalogue_id, dataset_id)
    rights = g.value(s, DCTERMS.accessRights)
    licence = g.value(s, DCTERMS.license)
    return {
        "id": dataset_id,
        "title": _text(g, s, DCTERMS.title) or dataset_id,
        "description": _text(g, s, DCTERMS.description),
        "keywords": sorted({str(k) for k in g.objects(s, DCAT.keyword)}, key=str.lower),
        "license": str(licence) if licence is not None else "",
        "access_rights": str(rights).rsplit("/", 1)[-1] if rights is not None else None,
        "access_level": _level(catalogue_id, dataset_id),
        "distributions": len(list(g.objects(s, DCAT.distribution))),
        "modified": str(g.value(s, DCTERMS.modified) or g.value(s, DCTERMS.issued) or ""),
    }


def list_datasets(catalogue_id: str) -> list[dict]:
    r = _call("GET", "/datasets", accept="application/json",
              params={"catalogue": catalogue_id, "valueType": "identifiers", "limit": 1000})
    if r.status_code != 200:
        raise HTTPException(status_code=502, detail=f"Could not list catalogue '{catalogue_id}': {r.status_code} {r.text[:200]}")
    out = []
    for dataset_id in r.json():
        try:
            out.append(summary(catalogue_id, dataset_id))
        except HTTPException as e:  # one unreadable record must not hide the others
            out.append({"id": dataset_id, "title": dataset_id, "error": e.detail})
    return sorted(out, key=lambda d: (d.get("modified") or ""), reverse=True)


def allowed_levels(catalogue_id: str) -> tuple[str, ...]:
    """Every level, in any catalogue: new datasets are internal (piveau_access.default_level), and publishing one,
    to signed-in users or to everyone, is an administrator's decision made here."""
    return LEVELS


def update(catalogue_id: str, dataset_id: str, changes: dict) -> dict:
    """Apply `changes` (title, description, keywords, license, access_level; absent ones are kept) to the dataset in
    the catalogue. Returns the dataset as it is now."""
    level = changes.get("access_level")
    if level is not None:
        if not piveau_access.enabled():
            raise HTTPException(status_code=409, detail="Visibility needs piveau access control (PIVEAU_ACCESS_CONTROL)")
        if level not in allowed_levels(catalogue_id):
            raise HTTPException(status_code=422, detail=f"Unknown visibility '{level}'")
    if "title" in changes and not (changes["title"] or "").strip():
        raise HTTPException(status_code=422, detail="The title cannot be empty")
    if changes.get("license") and not _URL_RE.match(changes["license"].strip()):
        raise HTTPException(status_code=422, detail="The licence must be a URL, e.g. https://creativecommons.org/licenses/by/4.0/")

    g, s = _graph(catalogue_id, dataset_id)
    if "title" in changes:
        g.set((s, DCTERMS.title, Literal(changes["title"].strip(), lang="en")))
    if "description" in changes:
        g.set((s, DCTERMS.description, Literal((changes["description"] or "").strip(), lang="en")))
    if "keywords" in changes:
        g.remove((s, DCAT.keyword, None))
        for k in dict.fromkeys(k.strip() for k in changes["keywords"] or [] if k and k.strip()):
            g.add((s, DCAT.keyword, Literal(k, lang="en")))
    if "license" in changes:
        g.remove((s, DCTERMS.license, None))
        if (changes["license"] or "").strip():
            g.add((s, DCTERMS.license, URIRef(changes["license"].strip())))
    params = {"catalogue": catalogue_id}
    if level is not None:
        g.set((s, DCTERMS.accessRights, URIRef(piveau_access.ACCESS_RIGHT + _RIGHT_OF_LEVEL[level])))
        params.update(accessLevel=level, publicationStatus="published")

    r = _call("PUT", f"/datasets/{dataset_id}", body=g.serialize(format="turtle"), params=params)
    if r.status_code not in (200, 201, 204, 304):
        raise HTTPException(status_code=502, detail=f"piveau refused the edit of '{dataset_id}': {r.status_code} {r.text[:300]}")

    return summary(catalogue_id, dataset_id)
