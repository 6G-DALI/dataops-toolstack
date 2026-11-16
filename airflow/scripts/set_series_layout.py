"""
Declare a narrow distribution's series layout in piveau: dali:seriesHierarchy, dali:seriesSeparator,
dali:ignoredColumn, dali:valueColumn.

The DataOps DAG reads these from the distribution (dali.utils.series_layout_of) and, when they are there, unwraps the
file into one column per series before processing and wraps the result back (dali.unwrap).

    PIVEAU_API_KEY=... python scripts/set_series_layout.py DATASET_ID ASSET_ID \
        --hierarchy cell_id,ue_id,metric_id --ignore source --ignore gNB_id \
        --catalogue 6g-dali-staging-eur            # prints what would change
    ... --apply                                    # writes it

Without --apply nothing is written. The distribution is matched by its dali:assetId. The three properties are replaced
(not merged), so re-running with a new list sets exactly that list; --clear removes them. Everything else on the graph
is sent back as it was read, the way the quality publisher does.
"""

from __future__ import annotations

import argparse
import json
import os
import sys

import requests

NS = "https://dali-project.eu/ns#"
PROPS = ("seriesHierarchy", "seriesSeparator", "ignoredColumn", "valueColumn")


def _asset(node: dict) -> str:
    raw = node.get(f"{NS}assetId") or node.get("dali:assetId") or ""
    return str(raw.get("@value", "") if isinstance(raw, dict) else raw)


def apply_layout(graph: dict, asset_id: str, hierarchy: str | list[str] | None, ignore: list[str],
                 value_col: str | None, separator: str | None = None) -> dict:
    """Set (or, hierarchy None, remove) the layout on the distribution with this asset id. Returns {before, after}."""
    nodes = graph.get("@graph", [])
    dists = [n for n in nodes if "dcat:Distribution" in (n["@type"] if isinstance(n.get("@type"), list) else [n.get("@type")])]
    node = next((n for n in dists if _asset(n) == asset_id), None)
    if node is None:
        raise SystemExit(f"no distribution with dali:assetId {asset_id!r} (found {[_asset(n) for n in dists]})")
    keys = {f"{NS}{p}": p for p in PROPS} | {f"dali:{p}": p for p in PROPS}
    before = {keys[k]: node[k] for k in node if k in keys}
    for k in list(node):
        if k in keys:
            del node[k]
    if hierarchy:
        node[f"{NS}seriesHierarchy"] = hierarchy            # the fields in order, comma-separated; a list = several
        if separator:
            node[f"{NS}seriesSeparator"] = separator
        if ignore:
            node[f"{NS}ignoredColumn"] = list(ignore)
        if value_col:
            node[f"{NS}valueColumn"] = value_col
    after = {keys[k]: node[k] for k in node if k in keys}
    return {"before": before, "after": after}


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("dataset_id")
    ap.add_argument("asset_id", help="the distribution's dali:assetId")
    ap.add_argument("--hierarchy", action="append",
                    help="the id columns in order, comma-separated, the last the metric: cell_id,ue_id,metric_id. "
                         "Repeat for several hierarchies (cell-level rows, UE-level rows)")
    ap.add_argument("--separator", help="what joins the parts of a series name; default _ (not written when omitted)")
    ap.add_argument("--ignore", action="append", default=[], help="a column of the narrow file with a single value, e.g. source; repeatable")
    ap.add_argument("--value-col", default="value")
    ap.add_argument("--clear", action="store_true", help="remove the three properties")
    ap.add_argument("--catalogue", default="", help="catalogue id (required with --apply)")
    ap.add_argument("--hub", default=os.environ.get("PIVEAU_DATASETS_URL", "https://dataspace.6gdali.eu/datasets"))
    ap.add_argument("--apply", action="store_true", help="write to piveau; without it, only print the change")
    args = ap.parse_args()

    if not args.hierarchy and not args.clear:
        ap.error("give --hierarchy, or --clear")
    key = os.environ.get("PIVEAU_API_KEY", "").strip()
    if not key:
        sys.exit("PIVEAU_API_KEY is not set")
    qs = f"?catalogue={args.catalogue}" if args.catalogue else ""
    url = f"{args.hub.rstrip('/')}/{args.dataset_id}{qs}"
    headers = {"X-API-Key": key, "Accept": "application/ld+json"}

    resp = requests.get(url, headers=headers, timeout=20)
    resp.raise_for_status()
    graph = resp.json()
    hierarchy = None if args.clear else (args.hierarchy[0] if len(args.hierarchy) == 1 else args.hierarchy)
    change = apply_layout(graph, args.asset_id, hierarchy, args.ignore, args.value_col, args.separator)
    print("before:", json.dumps(change["before"], indent=2))
    print("after: ", json.dumps(change["after"], indent=2))
    if not args.apply:
        print("\n(dry run: nothing written; add --apply)")
        return
    if not args.catalogue:
        sys.exit("--apply needs --catalogue")
    put = requests.put(url, headers={**headers, "Content-Type": "application/ld+json"}, data=json.dumps(graph), timeout=20)
    put.raise_for_status()
    print(f"written: HTTP {put.status_code}")


if __name__ == "__main__":
    main()
