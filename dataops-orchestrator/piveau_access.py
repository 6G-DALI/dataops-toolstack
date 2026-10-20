"""
piveau access control v2: the access level a dataset is written with, and the caller's token for reads.

A catalogue is public or private (dct:accessRights PUBLIC or NON_PUBLIC, fixed when it is created). Every new dataset,
in any catalogue, is internal (NON_PUBLIC: members of its catalogue only) until an administrator publishes it from the
DataOps edit page (testbed_datasets). What the record itself states (dct:accessRights, from the testbed's
metadata.json or the submission form) is not used, and is rewritten to match the level. A dataset that already
exists keeps its level (it may have been published on purpose): it is re-PUT without accessLevel, which piveau leaves
alone. piveau does not apply the catalogue's level by itself: a new dataset PUT without accessLevel would be public.

Everything here is inert while PIVEAU_ACCESS_CONTROL is off (piveau 3.3 has no access levels).
"""

import asyncio
import contextvars

import httpx
from fastapi import HTTPException, Request
import re

from rdflib import Graph, URIRef
from rdflib.namespace import DCAT, DCTERMS, RDF

import config

ACCESS_RIGHT = "http://publications.europa.eu/resource/authority/access-right/"
_LEVEL_BY_RIGHT = {"PUBLIC": "public", "RESTRICTED": "restricted", "NON_PUBLIC": "internal"}
_ORDER = {"public": 0, "restricted": 1, "internal": 2}


def enabled() -> bool:
    return config.PIVEAU_ACCESS_CONTROL


def level_of(right: str | None) -> str:
    """The level for an access-right code or IRI. No statement is public; an unknown code is treated as non-public,
    so a typo never publishes a dataset."""
    if not right or not str(right).strip():
        return "public"
    return _LEVEL_BY_RIGHT.get(str(right).rsplit("/", 1)[-1].strip().upper(), "internal")


def stricter(*levels: str) -> str:
    return max(levels, key=_ORDER.__getitem__)


def catalogue_rights(private: bool) -> str:
    return ACCESS_RIGHT + ("NON_PUBLIC" if private else "PUBLIC")


def rights_in(turtle: str) -> str | None:
    """dct:accessRights of the dataset or catalogue a Turtle record describes, None without one (or when the record
    does not parse: piveau then rejects it anyway)."""
    g = Graph()
    try:
        g.parse(data=turtle, format="turtle")
    except Exception:  # rdflib raises a variety of parser errors
        return None
    for kind in (DCAT.Dataset, DCAT.Catalog):
        for s in g.subjects(RDF.type, kind):
            right = g.value(s, DCTERMS.accessRights)
            if right is not None:
                return str(right)
    return None


# A catalogue's visibility is fixed when it is created, so its level is cached for the life of the process.
_catalogue_levels: dict[str, str] = {}


def catalogue_level(catalogue_id: str) -> str:
    """The level every dataset of the catalogue gets at least. Raises 502 when piveau cannot tell, so a dataset is
    never written as public into a catalogue that might be private."""
    if catalogue_id in _catalogue_levels:
        return _catalogue_levels[catalogue_id]
    import piveau_dataset_client as pdc
    pdc._require_piveau_config()
    try:
        r = httpx.get(f"{pdc.PIVEAU_HUB_URL.rstrip('/')}/catalogues/{catalogue_id}", timeout=15,
                      headers={"X-API-Key": pdc.PIVEAU_API_KEY, "Accept": "text/turtle"})
        r.raise_for_status()
    except httpx.HTTPError as e:
        raise HTTPException(status_code=502, detail=f"Could not read catalogue '{catalogue_id}' from piveau to choose "
                                                    f"the dataset's access level: {e}")
    level = level_of(rights_in(r.text))
    _catalogue_levels[catalogue_id] = level
    return level


LEVELS = ("public", "restricted", "internal")
RIGHT_OF_LEVEL = {"public": "PUBLIC", "restricted": "RESTRICTED", "internal": "NON_PUBLIC"}


DEFAULT_LEVEL = "internal"


def default_level(catalogue_id: str) -> str:
    """The level a new dataset gets: internal, in any catalogue, until an administrator publishes it."""
    return DEFAULT_LEVEL


def current_level(catalogue_id: str, dataset_id: str) -> str | None:
    """A dataset's piveau access level, from its catalogue record; None when the dataset does not exist yet. Raises 502
    when piveau cannot tell, so an existing dataset is never re-leveled by mistake."""
    import piveau_dataset_client as pdc
    pdc._require_piveau_config()
    try:
        r = httpx.get(f"{pdc.PIVEAU_HUB_URL.rstrip('/')}/datasets/{dataset_id}/record", params={"catalogue": catalogue_id},
                      headers={"X-API-Key": pdc.PIVEAU_API_KEY, "Accept": "text/turtle"}, timeout=15)
    except httpx.HTTPError as e:
        raise HTTPException(status_code=502, detail=f"Could not read the record of '{dataset_id}' from piveau: {e}")
    if r.status_code == 404:
        return None
    m = _LEVEL_RE.search(r.text) if r.status_code == 200 else None
    if not m:
        raise HTTPException(status_code=502, detail=f"piveau did not say the access level of '{dataset_id}' ({r.status_code})")
    return m.group(1)


# The record's piveau:accessLevel, prefixed or as a full IRI (piveau does not promise which it serializes).
_LEVEL_RE = re.compile(r"(?:piveau:|ns/voc#)accessLevel>?\s+(?:piveau:|<https://piveau\.eu/ns/voc#)(\w+)")


def with_rights(turtle: str, level: str) -> str:
    """The record with its dct:accessRights set to match `level`, so readers (and the catalogue UI's badges) see the
    level piveau enforces. Unchanged when it does not parse (piveau then rejects it anyway)."""
    g = Graph()
    try:
        g.parse(data=turtle, format="turtle")
    except Exception:
        return turtle
    subjects = [s for kind in (DCAT.Dataset, DCAT.DataService) for s in g.subjects(RDF.type, kind)]
    if not subjects:
        return turtle
    for s in subjects:
        g.set((s, DCTERMS.accessRights, URIRef(ACCESS_RIGHT + RIGHT_OF_LEVEL[level])))
    return g.serialize(format="turtle")


async def prepare_write(catalogue_id: str, dataset_id: str, turtle: str) -> tuple[str, dict]:
    """The Turtle to PUT and its query parameters. A new dataset gets its catalogue's default level; an existing one
    keeps its level (no accessLevel sent). Either way dct:accessRights is made to match. Unchanged and no parameters
    while access control is off."""
    if not enabled():
        return turtle, {}
    current = await asyncio.to_thread(current_level, catalogue_id, dataset_id)
    level = current or await asyncio.to_thread(default_level, catalogue_id)
    params = {} if current else {"accessLevel": level, "publicationStatus": "published"}
    return with_rights(turtle, level), params


# --- reads with the caller's token --------------------------------------------------------------------------------
# The orchestrator's dataset listings are open endpoints, so they must never read piveau with more rights than the
# caller has. With access control on they forward the caller's own bearer token (from the DataOps UI, same realm as
# piveau) and piveau filters; without a token they read as anonymous and see public data only.
_caller_auth: contextvars.ContextVar[str | None] = contextvars.ContextVar("piveau_caller_auth", default=None)


async def forward_caller_token(request: Request) -> None:
    """Router dependency: remember the request's bearer token for the piveau reads made while serving it."""
    if enabled():
        auth = request.headers.get("authorization") or ""
        _caller_auth.set(auth if auth.lower().startswith("bearer ") else None)


def read_headers() -> dict:
    auth = _caller_auth.get() if enabled() else None
    return {"Authorization": auth} if auth else {}
