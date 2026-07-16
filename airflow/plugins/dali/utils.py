from __future__ import annotations

import ast
import json
import math
import os
import urllib.request

import great_expectations as gx

DEFAULT_EXPECTATIONS = [
    {"type": "expect_table_row_count_to_be_between", "min_value": 1},
]

DEFAULT_CONN_ID     = "dali-dataspace"

# S3/MinIO connection IDs are fixed per deployment and supplied through the
# environment — they are intentionally NOT exposed as DAG params so a triggering
# user cannot point a run at a different Data Space / DataOps bucket.
DATASPACE_S3_CONN_ID = os.getenv("DATASPACE_S3_CONN_ID", DEFAULT_CONN_ID)
DATAOPS_S3_CONN_ID   = os.getenv("DATAOPS_S3_CONN_ID", "dali-dataops")

# EDC connector endpoints — fixed per deployment, not DAG params, so a
# triggering user can't point a run at an arbitrary connector. Port layout
# matches tests/edc_test_files/1-prepare-contract-cloud.py:
#   consumer_http_management_port=18181, provider_http_management_port=20001,
#   provider_http_protocol_port=20002.
# Only the consumer's management port and the provider's protocol port are
# actually used by the DataOps pipelines — the provider's management/control
# ports, and the consumer's protocol/control ports, are the respective
# connector operators' concern, not something these pipelines call directly.
EDC_CONSUMER_DOMAIN          = os.getenv("EDC_CONSUMER_DOMAIN", "http://ds.uc1.ac3.sparkworks.net")
EDC_CONSUMER_MANAGEMENT_PORT = int(os.getenv("EDC_CONSUMER_MANAGEMENT_PORT", "18181"))
EDC_CONSUMER_URL             = f"{EDC_CONSUMER_DOMAIN}:{EDC_CONSUMER_MANAGEMENT_PORT}"

EDC_PROVIDER_DOMAIN         = os.getenv("EDC_PROVIDER_DOMAIN", "http://edc.6gdali.sparkworks.net")
EDC_PROVIDER_PROTOCOL_PORT  = int(os.getenv("EDC_PROVIDER_PROTOCOL_PORT", "20002"))
EDC_PROVIDER_PROTOCOL_URL   = f"{EDC_PROVIDER_DOMAIN}:{EDC_PROVIDER_PROTOCOL_PORT}"

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


def _scalar(val) -> str:
    """Unwrap a JSON-LD value node ({"@value": ...} or {"@id": ...}) to a plain string."""
    if isinstance(val, dict):
        return str(val.get("@value") or val.get("@id") or "")
    return str(val) if val else ""


def node_types(node: dict) -> list[str]:
    t = node.get("@type", [])
    return t if isinstance(t, list) else [t]


def dist_keys(node: dict) -> set[str]:
    """All identifiers a distribution node could plausibly be addressed by:
    its full @id, that @id's last path segment, and likewise for
    dct:identifier — matches piveau_client.py's _dist_keys on the UI side,
    since piveau's own distribution "id" doesn't consistently show up in the
    same form in every context."""
    keys: set[str] = set()
    for raw in (node.get("@id", ""), _scalar(node.get("dct:identifier"))):
        if not raw:
            continue
        keys.add(raw)
        keys.add(raw.rstrip("/").rsplit("/", 1)[-1])
    return keys


def _variable_measured_of(node: dict) -> list[str]:
    vm = node.get("schema:variableMeasured") or node.get("https://schema.org/variableMeasured") or []
    if isinstance(vm, (str, dict)):
        vm = [vm]
    return [c for c in (_scalar(v) for v in vm) if c]


def fetch_columns_from_piveau(dataset_id: str, asset_id: str = "") -> list[str]:
    """Fetch schema:variableMeasured column names from piveau for a dataset.

    The application profile places this on the dataset node, but some
    records (e.g. harvested/externally-sourced ones) carry it on the
    dcat:Distribution node instead — both are checked. Critically, only the
    *targeted* distribution (matched via asset_id, see dist_keys) is
    considered, never every distribution on the dataset — a multi-distribution
    dataset can have a different column list per distribution, and mixing
    them in would generate expectations for columns that don't exist in the
    file actually being validated.
    """
    url = f"{PIVEAU_DATASETS_URL}/{dataset_id}"
    req = urllib.request.Request(url, headers={"Accept": "application/ld+json"})
    try:
        with urllib.request.urlopen(req, timeout=10) as resp:
            data = json.loads(resp.read())
        graph = data.get("@graph", [])

        cols: list[str] = []
        for node in graph:
            types = node_types(node)
            is_dataset = any("Dataset" in t for t in types)
            is_target_distribution = (
                any("Distribution" in t for t in types)
                and (not asset_id or asset_id in dist_keys(node))
            )
            if is_dataset or is_target_distribution:
                cols.extend(_variable_measured_of(node))

        seen: set[str] = set()
        deduped = []
        for c in cols:
            if c not in seen:
                seen.add(c)
                deduped.append(c)
        return deduped
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
