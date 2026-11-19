"""
Declare a narrow or wide-panel distribution's series layout in piveau: dali:seriesHierarchy, dali:seriesSeparator,
dali:ignoredColumn, dali:valueColumn, and for a panel dali:measureColumn, dali:timestampColumn, dali:carryColumn.

The DataOps DAG reads these from the distribution (dali.utils.series_layout_of) and, when they are there, unwraps the
file into one column per series before processing and wraps the result back (dali.unwrap).

    PIVEAU_API_KEY=... python scripts/set_series_layout.py DATASET_ID ASSET_ID \
        --hierarchy cell_id,ue_id,metric_id --ignore source --ignore gNB_id \
        --catalogue 6g-dali-staging-eur            # prints what would change
    ... --apply                                    # writes it

A panel (several entities on their own clocks, one column per measure) names its measure columns; the hierarchy
ends in a metric field that is not a column of the file:

    python scripts/set_series_layout.py DATASET_ID ASSET_ID --hierarchy entity_id,metric_id \
        --measure barometer --measure pm10 --measure latitude --timestamp-col measured_at --carry name

The layout can instead be read from a dataset's metadata.json, from its "series_layout" object: one entry per data
file, keyed by file name, with hierarchy, separator, value_column, measure_columns, timestamp_column, carry_columns and
ignored_columns. A flag given on the command line replaces the same item of the entry:

    python scripts/set_series_layout.py DATASET_ID ASSET_ID --from-metadata 06_UL_and_DL_Throughput_Dataset/metadata.json \
        --file 1_throughput_ul_dl.csv --catalogue 6g-dali-adroit6g

Without --apply nothing is written. The distribution is matched by its dali:assetId. The properties are replaced
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
PROPS = ("seriesHierarchy", "seriesSeparator", "ignoredColumn", "valueColumn", "measureColumn", "timestampColumn",
         "carryColumn")


def _asset(node: dict) -> str:
    raw = node.get(f"{NS}assetId") or node.get("dali:assetId") or ""
    return str(raw.get("@value", "") if isinstance(raw, dict) else raw)


def apply_layout(graph: dict, asset_id: str, hierarchy: str | list[str] | None, ignore: list[str],
                 value_col: str | None, separator: str | None = None, measure: list[str] | None = None,
                 timestamp_col: str | None = None, carry: list[str] | None = None) -> dict:
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
        if value_col and not measure:
            node[f"{NS}valueColumn"] = value_col                # a panel has no value column
        if measure:
            node[f"{NS}measureColumn"] = list(measure)
        if timestamp_col:
            node[f"{NS}timestampColumn"] = timestamp_col
        if carry:
            node[f"{NS}carryColumn"] = list(carry)
    after = {keys[k]: node[k] for k in node if k in keys}
    return {"before": before, "after": after}


def layout_from_metadata(path: str, file: str | None = None) -> dict:
    """Read one file's entry of the "series_layout" object of a dataset's metadata.json (keyed by file name).

    `file` is the key; it may be left out when the object has exactly one entry. Returns the arguments of
    apply_layout: hierarchy (comma-joined), separator, ignore, value_col, measure, timestamp_col, carry.
    """
    with open(path, encoding="utf-8") as f:
        doc = json.load(f)
    layouts = doc.get("series_layout") or {}
    if not isinstance(layouts, dict):
        raise SystemExit(f"{path}: series_layout must be an object keyed by file name")
    names = [file] if file else list(layouts)
    if len(names) != 1 or names[0] not in layouts:
        raise SystemExit(f"{path}: expected one series_layout entry{f' for {file!r}' if file else ''}, "
                         f"found {sorted(layouts)}; name it with --file")
    block = layouts[names[0]]
    hierarchy = block.get("hierarchy")
    if isinstance(hierarchy, str):
        hierarchy = [h.strip() for h in hierarchy.split(",")]
    if not hierarchy or not all(isinstance(h, str) and h for h in hierarchy):
        raise SystemExit(f"{path}: series_layout.hierarchy must list the id columns, the last one the metric")
    return {"hierarchy": ",".join(hierarchy), "separator": block.get("separator") or None,
            "ignore": list(block.get("ignored_columns") or []), "value_col": block.get("value_column"),
            "measure": list(block.get("measure_columns") or []), "timestamp_col": block.get("timestamp_column"),
            "carry": list(block.get("carry_columns") or [])}


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
    ap.add_argument("--measure", action="append", default=[],
                    help="a panel: a column that is measured (barometer, pm10, ...); repeatable")
    ap.add_argument("--timestamp-col", help="the column that holds the time of the measurement, e.g. measured_at")
    ap.add_argument("--carry", action="append", default=[],
                    help="a panel: another column whose nearest known value is given to rows imputation adds "
                         "(a station's name); repeatable")
    ap.add_argument("--from-metadata", metavar="PATH",
                    help="read the layout from the seriesLayout block of this metadata.json; flags given here override it")
    ap.add_argument("--file", help="with --from-metadata: the file name of the series_layout entry to read")
    ap.add_argument("--clear", action="store_true", help="remove the layout properties")
    ap.add_argument("--catalogue", default="", help="catalogue id (required with --apply)")
    ap.add_argument("--hub", default=os.environ.get("PIVEAU_DATASETS_URL", "https://dataspace.6gdali.eu/datasets"))
    ap.add_argument("--apply", action="store_true", help="write to piveau; without it, only print the change")
    args = ap.parse_args()

    if not args.hierarchy and not args.clear and not args.from_metadata:
        ap.error("give --hierarchy, --from-metadata, or --clear")
    layout = layout_from_metadata(args.from_metadata, args.file) if args.from_metadata and not args.clear else {}
    key = os.environ.get("PIVEAU_API_KEY", "").strip()
    if not key:
        sys.exit("PIVEAU_API_KEY is not set")
    qs = f"?catalogue={args.catalogue}" if args.catalogue else ""
    url = f"{args.hub.rstrip('/')}/{args.dataset_id}{qs}"
    headers = {"X-API-Key": key, "Accept": "application/ld+json"}

    resp = requests.get(url, headers=headers, timeout=20)
    resp.raise_for_status()
    graph = resp.json()
    if args.clear:
        hierarchy = None
    elif args.hierarchy:
        hierarchy = args.hierarchy[0] if len(args.hierarchy) == 1 else args.hierarchy
    else:
        hierarchy = layout["hierarchy"]
    change = apply_layout(graph, args.asset_id, hierarchy,
                          args.ignore or layout.get("ignore", []),
                          args.value_col if args.value_col != "value" else layout.get("value_col") or "value",
                          args.separator or layout.get("separator"),
                          args.measure or layout.get("measure", []),
                          args.timestamp_col or layout.get("timestamp_col"),
                          args.carry or layout.get("carry", []))
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
