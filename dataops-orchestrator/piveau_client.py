"""
Fetches datasets from a piveau-hub-search instance and normalises them
to the flat shapes used by the orchestrator API.

Two levels are exposed: `fetch_datasets` returns one entry per dataset,
`fetch_distributions` returns one entry per distribution of a single
dataset (mirroring the Catalogue -> Dataset -> Distribution drill-down
used by the DataOps UI's DAG trigger picker).
"""

import asyncio
import os

import httpx

PIVEAU_URL = os.getenv("PIVEAU_URL", "https://search.dspace.sparkworks.net")
DSPACE_URL = os.getenv("DSPACE_URL", "https://dspace.sparkworks.net/datasets")
_DSPACE_CATALOGUE_BASE = os.getenv("DSPACE_CATALOGUE_URL", "https://catalogue.dspace.sparkworks.net")
_SEARCH_PATH = "/search"

# Full IRIs (dali: prefix not in @context)
_SNS_PROJECT_NAME = "https://dali-project.eu/ns#snsProjectName"
_ASSET_ID         = "https://dali-project.eu/ns#assetId"
_CONNECTOR_TYPE   = "https://dali-project.eu/ns#connectorType"

# Prefixed keys (schema: and dct: are in @context)
_VARIABLE_MEASURED = "schema:variableMeasured"
_DCT_TITLE         = "dct:title"
_DCT_TYPE          = "@type"


def _title(obj: dict) -> str:
    if not obj:
        return ""
    return obj.get("en") or obj.get("de") or next(iter(obj.values()), "")


def _dist_uri(dist: dict) -> str:
    urls = dist.get("access_url") or []
    return urls[0] if urls else ""


def _scalar(val) -> str:
    if isinstance(val, list):
        val = val[0] if val else ""
    if isinstance(val, dict):
        return val.get("@value", "")
    return str(val) if val else ""


def _string_list(val) -> list[str]:
    if not isinstance(val, list):
        val = [val]
    return [_scalar(v) for v in val if v]


def _node_types(node: dict) -> list[str]:
    t = node.get(_DCT_TYPE, [])
    return t if isinstance(t, list) else [t]


def _dist_keys(*iris: str) -> list[str]:
    """Return all non-empty key variants for a distribution IRI (full IRI + bare UUID)."""
    keys = []
    for iri in iris:
        if iri:
            keys.append(iri)
            uuid = iri.rstrip("/").rsplit("/", 1)[-1]
            if uuid != iri:
                keys.append(uuid)
    return keys


async def _fetch_dataset_detail(
    client: httpx.AsyncClient, dataset_id: str
) -> tuple[str, list[str], dict[str, dict], dict]:
    """Fetch the full JSON-LD record.

    Returns (snsProjectName, variableMeasured, dist_details, raw) where
    dist_details maps distribution UUID -> {asset_id, asset_title}.
    """
    try:
        r = await client.get(f"{DSPACE_URL}/{dataset_id}", timeout=10)
        r.raise_for_status()
        graph = r.json()
        nodes = graph.get("@graph", [])

        sns = ""
        variable_measured: list[str] = []
        dist_details: dict[str, dict] = {}

        for node in nodes:
            types = _node_types(node)

            # Dataset node — carries snsProjectName and variableMeasured
            if "dcat:Dataset" in types or any("Dataset" in t for t in types):
                if _SNS_PROJECT_NAME in node:
                    sns = _scalar(node[_SNS_PROJECT_NAME])
                if _VARIABLE_MEASURED in node:
                    variable_measured = _string_list(node[_VARIABLE_MEASURED])

            # Distribution node — carries assetId and title per distribution.
            # Index by every possible key so we match whatever piveau returns as dist id.
            if "dcat:Distribution" in types:
                detail = {
                    "asset_id":    _scalar(node.get(_ASSET_ID, "")),
                    "asset_title": _scalar(node.get(_DCT_TITLE, "")),
                }
                node_id = node.get("@id", "")
                for key in _dist_keys(node_id, _scalar(node.get("dct:identifier", ""))):
                    dist_details[key] = detail
        return sns, variable_measured, dist_details, graph
    except Exception as exc:
        print(f"[piveau] detail fetch failed for {dataset_id}: {exc}")
    return "", [], {}, {}


def _normalize_distribution(
    ds: dict, dist: dict, sns: str,
    variable_measured: list[str], dist_details: dict[str, dict], raw: dict
) -> dict:
    fmt = (dist.get("format") or {}).get("label", "")
    dist_id = dist.get("id", "")
    detail = {}
    for key in _dist_keys(dist_id):
        detail = dist_details.get(key, {})
        if detail:
            break
    if not detail:
        print(f"[piveau] no detail match for dist_id={dist_id!r}, available keys={list(dist_details.keys())}")
    catalog = ds.get("catalog") or {}
    catalog_id = catalog.get("id", "")
    asset_title = detail.get("asset_title", "")
    return {
        "id": f"piveau_{ds['id']}_{dist_id}",
        "name": _title(ds.get("title")),
        "uri": _dist_uri(dist),
        "sns_project_name": sns,
        "asset_id":         detail.get("asset_id", ""),
        "asset_title":      asset_title,
        "dataset_id":       ds["id"],
        "input_key":        f"{ds['id']}/{asset_title}" if asset_title else "",
        "variable_measured": variable_measured,
        "catalog_id":       catalog_id,
        "catalog_title":    _title(catalog.get("title")),
        "catalog_url":      f"{_DSPACE_CATALOGUE_BASE}/catalogues/{catalog_id}?locale=en" if catalog_id else "",
        "extra": {
            "format":      fmt,
            "description": _title(dist.get("description") or ds.get("description")),
            "source":      "piveau",
            "publisher":   (ds.get("publisher") or {}).get("name", ""),
            "license":     (dist.get("license") or {}).get("label", ""),
        },
        "consuming_dags": [],
        "producing_dags": [],
        "created_at": ds.get("issued"),
        "updated_at": ds.get("modified"),
        "raw": raw,
    }


def _fallback_distribution(ds: dict, sns: str, variable_measured: list[str], raw: dict) -> dict:
    """A dataset-as-distribution stand-in, used when a dataset has no distributions."""
    landing = ds.get("landing_page") or []
    uri = landing[0].get("resource", "") if landing else ds.get("resource", ds.get("id", ""))
    catalog = ds.get("catalog") or {}
    catalog_id = catalog.get("id", "")
    return {
        "id":               f"piveau_{ds['id']}",
        "name":             _title(ds.get("title")),
        "uri":              uri,
        "sns_project_name": sns,
        "asset_id":         "",
        "asset_title":      "",
        "dataset_id":       ds["id"],
        "input_key":        "",
        "variable_measured": variable_measured,
        "catalog_id":       catalog_id,
        "catalog_title":    _title(catalog.get("title")),
        "catalog_url":      f"{_DSPACE_CATALOGUE_BASE}/catalogues/{catalog_id}?locale=en" if catalog_id else "",
        "extra": {
            "format":      "",
            "description": _title(ds.get("description")),
            "source":      "piveau",
            "publisher":   (ds.get("publisher") or {}).get("name", ""),
            "license":     "",
        },
        "consuming_dags": [],
        "producing_dags": [],
        "created_at": ds.get("issued"),
        "updated_at": ds.get("modified"),
        "raw": raw,
    }


def _expand(
    ds: dict, sns: str,
    variable_measured: list[str], dist_details: dict[str, dict], raw: dict
) -> list[dict]:
    """Return one entry per distribution of `ds`; fall back to one entry if none."""
    dists = ds.get("distributions") or []
    if dists:
        return [
            _normalize_distribution(ds, dist, sns, variable_measured, dist_details, raw)
            for dist in dists
        ]
    return [_fallback_distribution(ds, sns, variable_measured, raw)]


def _normalize_dataset(ds: dict, sns: str, variable_measured: list[str], raw: dict) -> dict:
    """One entry per dataset — distribution details are fetched separately, see fetch_distributions."""
    catalog = ds.get("catalog") or {}
    catalog_id = catalog.get("id", "")
    dists = ds.get("distributions") or []
    formats = sorted({
        fmt for d in dists
        if (fmt := (d.get("format") or {}).get("label", ""))
    })
    return {
        "id":                 ds["id"],
        "name":               _title(ds.get("title")),
        "sns_project_name":   sns,
        "dataset_id":         ds["id"],
        "distribution_count": len(dists),
        "variable_measured":  variable_measured,
        "catalog_id":         catalog_id,
        "catalog_title":      _title(catalog.get("title")),
        "catalog_url":        f"{_DSPACE_CATALOGUE_BASE}/catalogues/{catalog_id}?locale=en" if catalog_id else "",
        "extra": {
            "formats":     formats,
            "description": _title(ds.get("description")),
            "source":      "piveau",
            "publisher":   (ds.get("publisher") or {}).get("name", ""),
        },
        "consuming_dags": [],
        "producing_dags": [],
        "created_at": ds.get("issued"),
        "updated_at": ds.get("modified"),
        "raw": raw,
    }


async def _search_datasets(catalogue_id: str | None = None, limit: int = 100) -> list[tuple[dict, tuple]]:
    """Query piveau's dataset search index and fetch each result's JSON-LD detail.

    Returns a list of (search_result, (sns, variable_measured, dist_details, raw))
    pairs — the shared groundwork for both fetch_datasets and fetch_distributions.
    """
    params = {"index": "dataset", "limit": limit}
    if catalogue_id:
        params["catalog"] = catalogue_id
    async with httpx.AsyncClient(timeout=10) as client:
        r = await client.get(f"{PIVEAU_URL}{_SEARCH_PATH}", params=params)
        r.raise_for_status()
        data = r.json()
        results = [ds for ds in data.get("result", {}).get("results", [])
                   if ds.get("index") == "dataset"]

        details = await asyncio.gather(
            *[_fetch_dataset_detail(client, ds["id"]) for ds in results]
        )
        return list(zip(results, details))


async def fetch_datasets(catalogue_id: str | None = None, limit: int = 100) -> list[dict]:
    """Fetch datasets from piveau — one entry per dataset.

    When `catalogue_id` is given, only datasets in that catalogue are
    requested from piveau — this both scopes the result to a single
    catalogue and avoids that catalogue's datasets being crowded out by
    unrelated ones under the shared `limit`.
    """
    try:
        pairs = await _search_datasets(catalogue_id, limit)
        return [
            _normalize_dataset(ds, sns, variable_measured, raw)
            for ds, (sns, variable_measured, dist_details, raw) in pairs
        ]
    except Exception as exc:
        print(f"[piveau] fetch_datasets failed: {exc}")
        return []


async def fetch_distributions(
    dataset_id: str, catalogue_id: str | None = None, limit: int = 100
) -> list[dict]:
    """Fetch the distributions of a single dataset, one entry each.

    `catalogue_id` narrows the underlying search when known (the caller
    already picked a catalogue in the two-step Catalogue -> Dataset flow),
    but isn't required to find the dataset.
    """
    try:
        pairs = await _search_datasets(catalogue_id, limit)
        for ds, (sns, variable_measured, dist_details, raw) in pairs:
            if str(ds.get("id")) == str(dataset_id):
                return _expand(ds, sns, variable_measured, dist_details, raw)
        return []
    except Exception as exc:
        print(f"[piveau] fetch_distributions failed: {exc}")
        return []


async def fetch_catalogues(limit: int = 100) -> list[dict]:
    """Fetch the list of catalogues known to piveau (id + title)."""
    params = {"index": "catalogue", "limit": limit}
    try:
        async with httpx.AsyncClient(timeout=10) as client:
            r = await client.get(f"{PIVEAU_URL}{_SEARCH_PATH}", params=params)
            r.raise_for_status()
            data = r.json()
            results = [c for c in data.get("result", {}).get("results", [])
                       if c.get("index") == "catalogue"]
            return [
                {"id": c.get("id", ""), "title": _title(c.get("title")) or c.get("id", "")}
                for c in results
            ]
    except Exception as exc:
        print(f"[piveau] fetch_catalogues failed: {exc}")
        return []