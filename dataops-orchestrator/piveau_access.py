"""
piveau access control v2: the access level a dataset is written with, and the caller's token for reads.

A catalogue is public or private (dct:accessRights PUBLIC or NON_PUBLIC, fixed when it is created). A dataset is
written with the stricter of its catalogue's level and its own dct:accessRights. piveau does not apply the
catalogue's level by itself: a new dataset PUT without accessLevel is public even in a private catalogue. A re-PUT
without accessLevel keeps the level the dataset has, so only the writes that create datasets need it. The mapping
is the OOP Data Lab's: PUBLIC -> public, RESTRICTED -> restricted (hidden from anonymous visitors), NON_PUBLIC ->
internal (members of the catalogue only).

Everything here is inert while PIVEAU_ACCESS_CONTROL is off (piveau 3.3 has no access levels).
"""

import asyncio
import contextvars

import httpx
from fastapi import HTTPException, Request
from rdflib import Graph
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


async def write_params(catalogue_id: str, turtle: str, floor: str | None = None) -> dict:
    """Query parameters for a PUT that creates a dataset: its access level, the stricter of its catalogue's, its own
    dct:accessRights and `floor`. Empty while access control is off."""
    if not enabled():
        return {}
    levels = [await asyncio.to_thread(catalogue_level, catalogue_id), floor or level_of(rights_in(turtle))]
    return {"accessLevel": stricter(*levels), "publicationStatus": "published"}


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
