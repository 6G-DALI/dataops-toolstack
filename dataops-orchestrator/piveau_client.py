"""
Fetches datasets from a piveau-hub-search instance and normalises them
to the flat dataset shape used by the orchestrator API.
One entry is produced per distribution.
"""

import asyncio
import httpx

PIVEAU_URL = "https://search.dspace.sparkworks.net"
DSPACE_URL = "https://dspace.sparkworks.net/datasets"
_SEARCH_PATH = "/search"
_SNS_PROJECT_NAME = "https://dali-project.eu/ns#snsProjectName"


def _title(obj: dict) -> str:
    """Return the best available title string from a language-keyed dict."""
    if not obj:
        return ""
    return obj.get("en") or obj.get("de") or next(iter(obj.values()), "")


def _dist_uri(dist: dict) -> str:
    """Return the access URL from a distribution."""
    urls = dist.get("access_url") or []
    return urls[0] if urls else ""


async def _fetch_sns_project_name(client: httpx.AsyncClient, dataset_id: str) -> str:
    """Fetch the dali:snsProjectName for a dataset from the DSpace JSON-LD endpoint."""
    try:
        r = await client.get(f"{DSPACE_URL}/{dataset_id}", timeout=10)
        r.raise_for_status()
        graph = r.json()
        # Response is {"@graph": [...], "@context": {...}}
        nodes = graph.get("@graph", [])
        for node in nodes:
            if _SNS_PROJECT_NAME in node:
                val = node[_SNS_PROJECT_NAME]
                # Value is a plain string: "6G-DALI"
                if isinstance(val, str):
                    return val
                if isinstance(val, list):
                    val = val[0] if val else ""
                if isinstance(val, dict):
                    return val.get("@value", "")
                return str(val)
    except Exception:
        pass
    return ""


def _normalize_distribution(ds: dict, dist: dict, sns_project_name: str) -> dict:
    fmt = (dist.get("format") or {}).get("label", "")
    return {
        "id": f"piveau_{ds['id']}_{dist.get('id', '')}",
        "name": _title(ds.get("title")),
        "uri": _dist_uri(dist),
        "sns_project_name": sns_project_name,
        "extra": {
            "format": fmt,
            "description": _title(dist.get("description") or ds.get("description")),
            "source": "piveau",
            "publisher": (ds.get("publisher") or {}).get("name", ""),
            "license": (dist.get("license") or {}).get("label", ""),
        },
        "consuming_dags": [],
        "producing_dags": [],
        "created_at": ds.get("issued"),
        "updated_at": ds.get("modified"),
    }


def _expand(ds: dict, sns_project_name: str) -> list[dict]:
    """Return one entry per distribution; fall back to one entry if none."""
    dists = ds.get("distributions") or []
    if dists:
        return [_normalize_distribution(ds, dist, sns_project_name) for dist in dists]
    landing = ds.get("landing_page") or []
    uri = landing[0].get("resource", "") if landing else ds.get("resource", ds.get("id", ""))
    return [{
        "id": f"piveau_{ds['id']}",
        "name": _title(ds.get("title")),
        "uri": uri,
        "sns_project_name": sns_project_name,
        "extra": {
            "format": "",
            "description": _title(ds.get("description")),
            "source": "piveau",
            "publisher": (ds.get("publisher") or {}).get("name", ""),
            "license": "",
        },
        "consuming_dags": [],
        "producing_dags": [],
        "created_at": ds.get("issued"),
        "updated_at": ds.get("modified"),
    }]


async def fetch_datasets(limit: int = 100) -> list[dict]:
    """Fetch datasets from piveau and return one entry per distribution."""
    params = {"index": "dataset", "limit": limit}
    try:
        async with httpx.AsyncClient(timeout=10) as client:
            r = await client.get(f"{PIVEAU_URL}{_SEARCH_PATH}", params=params)
            r.raise_for_status()
            data = r.json()
            results = [ds for ds in data.get("result", {}).get("results", [])
                       if ds.get("index") == "dataset"]

            # Fetch snsProjectName for all datasets concurrently
            sns_names = await asyncio.gather(
                *[_fetch_sns_project_name(client, ds["id"]) for ds in results]
            )

            entries = []
            for ds, sns in zip(results, sns_names):
                entries.extend(_expand(ds, sns))
            return entries
    except Exception:
        return []
