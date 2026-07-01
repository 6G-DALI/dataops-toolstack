from __future__ import annotations

import ast
import json
import math
import urllib.request

import great_expectations as gx

DEFAULT_EXPECTATIONS = [
    {"type": "expect_table_row_count_to_be_between", "min_value": 1},
]

DEFAULT_CONN_ID     = "dali-dataspace"
PIVEAU_DATASETS_URL = "https://dspace.sparkworks.net/datasets"
DALI_NS             = "https://dali-project.eu/ns#"


def sanitize(obj):
    """Recursively replace nan/inf floats with None so the result is JSON-safe."""
    if isinstance(obj, float):
        return None if (math.isnan(obj) or math.isinf(obj)) else obj
    if isinstance(obj, dict):
        return {k: sanitize(v) for k, v in obj.items()}
    if isinstance(obj, list):
        return [sanitize(v) for v in obj]
    return obj


def fetch_columns_from_piveau(dataset_id: str) -> list[str]:
    """Fetch schema:variableMeasured column names from piveau for a dataset."""
    url = f"{PIVEAU_DATASETS_URL}/{dataset_id}"
    req = urllib.request.Request(url, headers={"Accept": "application/ld+json"})
    try:
        with urllib.request.urlopen(req, timeout=10) as resp:
            data = json.loads(resp.read())
        graph = data.get("@graph", [])
        ds_node = next(
            (n for n in graph if any(
                "Dataset" in t for t in (
                    [n.get("@type")] if isinstance(n.get("@type"), str) else n.get("@type", [])
                )
            )),
            None,
        )
        if not ds_node:
            return []
        cols = ds_node.get("schema:variableMeasured") or ds_node.get("https://schema.org/variableMeasured", [])
        if isinstance(cols, str):
            cols = [cols]
        return [c for c in cols if isinstance(c, str)]
    except Exception as exc:
        print(f"[dali] could not fetch columns from piveau: {exc}")
        return []


def exp_class(exp_type: str):
    """Return the Great Expectations class for a given expectation type string."""
    class_name = "".join(word.capitalize() for word in exp_type.split("_"))
    return getattr(gx.expectations, class_name)


def parse_expectations(value) -> list[dict]:
    """Parse an expectations value that may be a list, JSON string, or Python literal."""
    if isinstance(value, list):
        return value
    if isinstance(value, str):
        s = value.strip()
        if s in ("", "[]"):
            return []
        try:
            return json.loads(s)
        except json.JSONDecodeError:
            return ast.literal_eval(s)
    return []
