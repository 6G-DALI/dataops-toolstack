"""
The datasets of a testbed's catalogue, for the testbed page: list them, and edit their main fields and visibility.

Reads and writes go to piveau-hub-repo with the API key, so the testbed's administrators see every dataset of their
catalogue, private ones included. An edit is a read-modify-write of the dataset's whole graph (Turtle): piveau replaces
a dataset on a PUT, so sending only the changed fields would drop its distributions. The visibility is piveau's
access level (access control v2), read from the dataset's catalogue record and set with the PUT's `accessLevel`
parameter; dct:accessRights is kept in step with it.

The fields are the ones the asset monitor writes from a metadata.json (s3-asset-monitor DcatTurtleBuilder), in the
same shape, so an edited record reads like a registered one. Testbed administrators of the platform can also replace
the whole record as Turtle (replace_turtle), for what the form does not cover.

Only the catalogue is changed. A dataset uploaded by a testbed keeps its metadata.json in the bucket as the testbed
shared it, so re-registering the dataset from it (the asset monitor's status page) undoes edits made here.
"""

import re

import httpx
from fastapi import HTTPException
from rdflib import BNode, Graph, Literal, Namespace, URIRef
from rdflib.namespace import DCAT, DCTERMS, FOAF, PROV, RDF, XSD

import piveau_access
import piveau_dataset_client as pdc

LEVELS = piveau_access.LEVELS
_RIGHT_OF_LEVEL = piveau_access.RIGHT_OF_LEVEL
# The record's piveau:accessLevel, prefixed or as a full IRI (piveau does not promise which it serializes).
_LEVEL_RE = re.compile(r"(?:piveau:|ns/voc#)accessLevel>?\s+(?:piveau:|<https://piveau\.eu/ns/voc#)(\w+)")
_URL_RE = re.compile(r"^https?://\S+$")
_EMAIL_RE = re.compile(r"^[^@\s]+@[^@\s]+\.[^@\s]+$")
_DATE_RE = re.compile(r"^\d{4}-\d{2}-\d{2}$")
_CODE_RE = re.compile(r"^[A-Z_]{2,12}$")

# The asset monitor's GAIA-X namespace; records made elsewhere may use another one, so terms are matched by local name.
GAX = Namespace("https://registry.lab.gaia-x.eu/v1/api/trusted-shape-registry/v1/shapes/jsonld/trustframework#")
ADMS = Namespace("http://www.w3.org/ns/adms#")
SCHEMA = Namespace("https://schema.org/")
VCARD = Namespace("http://www.w3.org/2006/vcard/ns#")
DALI = Namespace("https://dali-project.eu/ns#")
THEME = "http://publications.europa.eu/resource/authority/data-theme/"
LANGUAGE = "http://publications.europa.eu/resource/authority/language/"
CREATOR_KINDS = ("Person", "Organization")


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


def _by_local_name(g: Graph, s: URIRef, name: str, default: URIRef) -> list[URIRef]:
    """The predicates of `s` ending in `name` (gax:producedBy comes in more than one GAIA-X namespace), or `default`."""
    found = [p for p in set(g.predicates(s, None)) if re.split(r"[#/]", str(p))[-1] == name]
    return found or [default]


def _drop(g: Graph, s, p) -> None:
    """Remove s p *, and the blank nodes it pointed to with everything under them."""
    for o in list(g.objects(s, p)):
        g.remove((s, p, o))
        if isinstance(o, BNode) and not any(True for _ in g.subjects(None, o)):
            for p2 in set(g.predicates(o, None)):
                _drop(g, o, p2)


def _mailto(v) -> str:
    return str(v).removeprefix("mailto:") if v is not None else ""


def _node_name(g: Graph, node) -> str:
    if node is None:
        return ""
    name = g.value(node, FOAF.name) or g.value(node, VCARD.fn)
    return str(name) if name is not None else (str(node) if isinstance(node, URIRef) else "")


def _details(g: Graph, s: URIRef) -> dict:
    """The fields of the edit form beyond the main ones, read as the asset monitor writes them."""
    produced = [o for p in _by_local_name(g, s, "producedBy", GAX.producedBy) for o in g.objects(s, p)]
    pii = [o for p in _by_local_name(g, s, "containsPII", GAX.containsPII) for o in g.objects(s, p)]
    contact = g.value(s, DCAT.contactPoint)
    creator = g.value(s, DCTERMS.creator)
    period = g.value(s, DCTERMS.temporal)
    language = g.value(s, DCTERMS.language)
    spatial = g.value(s, DCTERMS.spatial)
    kinds = {str(t).rsplit("/", 1)[-1] for t in g.objects(creator, RDF.type)} if creator is not None else set()
    return {
        "produced_by": str(produced[0]) if produced else "",
        "publisher": _node_name(g, g.value(s, DCTERMS.publisher)),
        "contact_email": _mailto(g.value(contact, VCARD.hasEmail)) if contact is not None else "",
        "creator": {
            "name": _node_name(g, creator),
            "kind": "Organization" if "Organization" in kinds else "Person",
            "email": _mailto(g.value(creator, FOAF.mbox)) if creator is not None else "",
            "orcid": str(g.value(creator, SCHEMA.identifier) or "") if creator is not None else "",
            "affiliation": str(g.value(creator, SCHEMA.affiliation) or "") if creator is not None else "",
        },
        "themes": sorted(str(t).removeprefix(THEME) for t in g.objects(s, DCAT.theme)),
        "language": str(language).removeprefix(LANGUAGE) if language is not None else "",
        "spatial": _node_name(g, spatial) if isinstance(spatial, BNode) else str(spatial or ""),
        "temporal_start": str(g.value(period, DCAT.startDate) or "") if period is not None else "",
        "temporal_end": str(g.value(period, DCAT.endDate) or "") if period is not None else "",
        "version": str(g.value(s, ADMS.version) or ""),
        "contains_pii": (str(pii[0]).lower() == "true") if pii else None,
    }


def summary(catalogue_id: str, dataset_id: str) -> dict:
    g, s = _graph(catalogue_id, dataset_id)
    rights = g.value(s, DCTERMS.accessRights)
    licence = g.value(s, DCTERMS.license)
    return {
        **_details(g, s),
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
    """Apply `changes` (title, description, keywords, license, access_level and the fields of _apply_details; absent
    ones are kept) to the dataset in the catalogue. Returns the dataset as it is now."""
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
    _check_details(changes)

    g, s = _graph(catalogue_id, dataset_id)
    _apply_details(g, s, changes)
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


def _bad(detail: str):
    raise HTTPException(status_code=422, detail=detail)


def _theme_iri(code: str) -> str:
    code = code.strip()
    if code.startswith(THEME):
        return code
    if not _CODE_RE.match(code.upper()):
        _bad(f"Unknown theme '{code}': use an EU data theme code such as TECH or ECON")
    return THEME + code.upper()


def _check_details(changes: dict) -> None:
    """Refuse values the form fields cannot hold, before anything is read or written."""
    def text(k):
        return (changes.get(k) or "").strip()
    if text("produced_by") and not _URL_RE.match(text("produced_by")):
        _bad("Produced by must be the organisation's participant IRI, e.g. https://dataspace.6gdali.eu/participant/eurecom")
    if text("contact_email") and not _EMAIL_RE.match(text("contact_email")):
        _bad("The contact e-mail is not an e-mail address")
    if text("language") and not _CODE_RE.match(text("language").upper()):
        _bad("The language must be an EU language code, e.g. ENG")
    for t in changes.get("themes") or []:
        _theme_iri(t)
    if ("temporal_start" in changes) != ("temporal_end" in changes):
        _bad("Send the start and the end of the period together")
    start, end = text("temporal_start"), text("temporal_end")
    if bool(start) != bool(end):
        _bad("The period needs both a start and an end date, or neither")
    if start and not (_DATE_RE.match(start) and _DATE_RE.match(end)):
        _bad("Period dates are YYYY-MM-DD")
    if start and start > end:
        _bad("The period ends before it starts")
    creator = changes.get("creator")
    if creator is not None:
        if (creator.get("kind") or "Person") not in CREATOR_KINDS:
            _bad("The creator is a Person or an Organization")
        email = (creator.get("email") or "").strip()
        if email and not _EMAIL_RE.match(email):
            _bad("The creator's e-mail is not an e-mail address")
        if not (creator.get("name") or "").strip() and any((creator.get(k) or "").strip() for k in ("email", "orcid", "affiliation")):
            _bad("The creator needs a name")


def _apply_details(g: Graph, s: URIRef, changes: dict) -> None:
    """Write the changed form fields in the shape the asset monitor uses; an empty value removes the field."""
    def text(k):
        return (changes.get(k) or "").strip()

    if "produced_by" in changes:
        old = {o for p in _by_local_name(g, s, "producedBy", GAX.producedBy) for o in g.objects(s, p)}
        for p in _by_local_name(g, s, "producedBy", GAX.producedBy):
            g.remove((s, p, None))
            if text("produced_by"):
                g.add((s, p, URIRef(text("produced_by"))))
        # prov:wasAttributedTo carries the same IRI; a named organisation (blank node) is left as it is.
        for o in old:
            g.remove((s, PROV.wasAttributedTo, o))
        if text("produced_by"):
            g.add((s, PROV.wasAttributedTo, URIRef(text("produced_by"))))
    if "publisher" in changes:
        _drop(g, s, DCTERMS.publisher)
        if text("publisher"):
            n = BNode()
            g.add((s, DCTERMS.publisher, n))
            g.add((n, RDF.type, FOAF.Organization))
            g.add((n, FOAF.name, Literal(text("publisher"))))
    if "contact_email" in changes:
        _drop(g, s, DCAT.contactPoint)
        if text("contact_email"):
            n = BNode()
            g.add((s, DCAT.contactPoint, n))
            g.add((n, RDF.type, VCARD.Organization))
            g.add((n, VCARD.hasEmail, URIRef("mailto:" + text("contact_email"))))
    if "creator" in changes:
        _drop(g, s, DCTERMS.creator)
        c = {k: (v or "").strip() for k, v in (changes["creator"] or {}).items()}
        if c.get("name"):
            n = BNode()
            g.add((s, DCTERMS.creator, n))
            g.add((n, RDF.type, FOAF.Organization if c.get("kind") == "Organization" else FOAF.Person))
            g.add((n, FOAF.name, Literal(c["name"])))
            if c.get("email"):
                g.add((n, FOAF.mbox, URIRef("mailto:" + c["email"])))
            if c.get("orcid"):
                g.add((n, SCHEMA.identifier, URIRef(c["orcid"] if c["orcid"].startswith("http") else "https://orcid.org/" + c["orcid"])))
            if c.get("affiliation"):
                g.add((n, SCHEMA.affiliation, Literal(c["affiliation"])))
    if "themes" in changes:
        g.remove((s, DCAT.theme, None))
        for t in dict.fromkeys(_theme_iri(t) for t in changes["themes"] or [] if t and t.strip()):
            g.add((s, DCAT.theme, URIRef(t)))
    if "language" in changes:
        g.remove((s, DCTERMS.language, None))
        if text("language"):
            g.add((s, DCTERMS.language, URIRef(LANGUAGE + text("language").upper())))
    if "spatial" in changes:
        _drop(g, s, DCTERMS.spatial)
        if text("spatial"):
            g.add((s, DCTERMS.spatial, Literal(text("spatial"))))
    if "temporal_start" in changes:
        _drop(g, s, DCTERMS.temporal)
        if text("temporal_start"):
            n = BNode()
            g.add((s, DCTERMS.temporal, n))
            g.add((n, RDF.type, DCTERMS.PeriodOfTime))
            g.add((n, DCAT.startDate, Literal(text("temporal_start"), datatype=XSD.date)))
            g.add((n, DCAT.endDate, Literal(text("temporal_end"), datatype=XSD.date)))
    if "version" in changes:
        g.remove((s, ADMS.version, None))
        if text("version"):
            g.add((s, ADMS.version, Literal(text("version"))))
    if "contains_pii" in changes and changes["contains_pii"] is not None:
        for p in _by_local_name(g, s, "containsPII", GAX.containsPII):
            g.set((s, p, Literal(bool(changes["contains_pii"]))))


# --- the whole record, for platform administrators ----------------------------------------------------------------

def _distribution_keys(g: Graph, s: URIRef) -> dict[str, str]:
    """The dataset's distributions by what identifies a file across PUTs (piveau re-mints distribution URIs): its
    asset id, else its access or download URL. Valued by its title, for messages."""
    out = {}
    for d in g.objects(s, DCAT.distribution):
        key = g.value(d, DALI.assetId) or g.value(d, DCAT.accessURL) or g.value(d, DCAT.downloadURL) or d
        out[str(key)] = _text(g, d, DCTERMS.title) or str(key)
    return out


def get_turtle(catalogue_id: str, dataset_id: str) -> str:
    g, _ = _graph(catalogue_id, dataset_id)
    return g.serialize(format="turtle")


def replace_turtle(catalogue_id: str, dataset_id: str, turtle: str) -> dict:
    """Replace the dataset's record with `turtle`. Refused when it does not parse, does not describe this dataset
    (one dcat:Dataset, same IRI), has no title, or loses a file. The visibility is not changed here: the PUT carries
    no accessLevel and dct:accessRights is set back to match the level piveau enforces."""
    try:
        new = Graph().parse(data=turtle, format="turtle")
    except Exception as e:  # rdflib raises a variety of parser errors
        _bad(f"The Turtle does not parse: {e}")
    subjects = list(new.subjects(RDF.type, DCAT.Dataset))
    if len(subjects) != 1:
        _bad(f"The record must describe exactly one dcat:Dataset (found {len(subjects)})")
    old, s = _graph(catalogue_id, dataset_id)
    if subjects[0] != s:
        _bad(f"The dataset's IRI cannot change: it is <{s}>")
    if not _text(new, s, DCTERMS.title).strip():
        _bad("The dataset needs a dct:title")
    lost = {k: t for k, t in _distribution_keys(old, s).items() if k not in _distribution_keys(new, s)}
    if lost:
        _bad("The record drops file(s) " + ", ".join(sorted(lost.values())) + ": keep their dcat:distribution nodes")

    level = _level(catalogue_id, dataset_id)
    body = piveau_access.with_rights(new.serialize(format="turtle"), level) if level else new.serialize(format="turtle")
    r = _call("PUT", f"/datasets/{dataset_id}", body=body, params={"catalogue": catalogue_id})
    if r.status_code not in (200, 201, 204, 304):
        raise HTTPException(status_code=502, detail=f"piveau refused the record of '{dataset_id}': {r.status_code} {r.text[:300]}")
    return summary(catalogue_id, dataset_id)
