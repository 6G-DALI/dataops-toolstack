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
# Bucket on that DataOps store (DATAOPS_S3_CONN_ID): the EDC-staged input and every run's outputs.
DATAOPS_BUCKET       = os.getenv("DATAOPS_BUCKET", "6g-dali-dataops")

# EDC connector base URLs — fixed per deployment, not DAG params, so a triggering user can't
# point a run at an arbitrary connector. The defaults are the connector's public address, which nginx
# routes by path: /management to the management API (port 20001, needs the X-Api-Key header) and
# /protocol to DSP (port 20002). The base URLs carry no port and no path; the code adds /management/v3
# and /protocol. The connector reaches its own public name through an extra_hosts entry that points it
# at this Docker host (see the provider service in the dataspace compose file); the tailnet address
# (http://6gdali-facility-edc:20001 / :20002) still works as an override.
#   EDC_CONSUMER_URL          our consumer connector; Airflow calls {url}/management/v3
#   EDC_PROVIDER_PROTOCOL_URL the provider connector; sent as counterPartyAddress ({url}/protocol)
EDC_CONSUMER_URL          = os.getenv("EDC_CONSUMER_URL", "https://edc.dataspace.6gdali.eu/").rstrip("/")
EDC_PROVIDER_PROTOCOL_URL = os.getenv("EDC_PROVIDER_PROTOCOL_URL", "https://edc.dataspace.6gdali.eu/").rstrip("/")

# API key for our consumer's management API, sent as a header on every call to it. Leave
# EDC_API_KEY unset for a connector that has no key. EDC_API_KEY_HEADER is EDC's default.
EDC_API_KEY        = os.getenv("EDC_API_KEY", "")
EDC_API_KEY_HEADER = os.getenv("EDC_API_KEY_HEADER", "X-Api-Key")
EDC_MANAGEMENT_HEADERS = {EDC_API_KEY_HEADER: EDC_API_KEY} if EDC_API_KEY else {}

print(f"EDC_API_KEY: {EDC_API_KEY}")

PIVEAU_DATASETS_URL = os.getenv("PIVEAU_DATASETS_URL", "https://dataspace.6gdali.eu/datasets").rstrip("/")
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
    same form in every context. Also dali:assetId, which is how the
    s3-asset-monitor records the EDC asset a distribution was registered for."""
    keys: set[str] = set()
    for key in (f"{DALI_NS}assetId", "dali:assetId"):
        asset = _scalar(node.get(key))
        if asset:
            keys.add(asset)
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


def _dali_values_of(node: dict, local: str) -> list[str]:
    """The values of a dali: property on a node, whether piveau returns it as the full IRI or as dali:<name>."""
    raw = node.get(f"{DALI_NS}{local}")
    if raw is None:
        raw = node.get(f"dali:{local}")
    if raw is None:
        return []
    if isinstance(raw, (str, dict)):
        raw = [raw]
    return [v for v in (_scalar(item) for item in raw) if v]


def series_layout_of(node: dict) -> dict | None:
    """How a narrow distribution is unwrapped into one column per series, as its catalogue metadata declares it.

    Three properties on the dcat:Distribution (6G-DALI Metadata Application Profile, section 5.6):
      dali:seriesHierarchy  the id columns that identify one series, in order, comma-separated, the last the
                            metric: "cell_id,ue_id,metric_id". Several values when cell-level and UE-level rows
                            are identified by different columns. The older template form ({cell_id}_{metric_id})
                            is accepted; see dali.unwrap
      dali:seriesSeparator  what joins the parts of a series name (default "_")
      dali:ignoredColumn    one per column of the narrow file that holds a single value (source, gNB_id); repeatable
      dali:valueColumn      the column holding the measured value (default "value" when only the hierarchy is set)
    A *panel* (several entities on their own clocks, one column per measure) adds:
      dali:measureColumn    one per column that is measured (barometer, pm10, latitude, ...); repeatable. The
                            hierarchy then ends in a metric field that is not a column of the file ("entity_id,metric_id")
      dali:timestampColumn  the column that holds the time of the measurement (measured_at); also usable on a narrow file
      dali:carryColumn      one per other column whose nearest known value is given to rows imputation adds (name)
    Returns {"hierarchy": [value, …], "separator", "ignore_columns", "value_col", "measure_cols", "timestamp_col",
    "carry_columns"}, or None when the distribution declares no hierarchy.
    """
    hierarchy = [v.strip() for v in _dali_values_of(node, "seriesHierarchy") if v.strip()]
    if not hierarchy:
        return None
    value_col = next(iter(_dali_values_of(node, "valueColumn")), "") or "value"
    separator = next(iter(_dali_values_of(node, "seriesSeparator")), "") or "_"
    return {"hierarchy": hierarchy, "separator": separator, "ignore_columns": _dali_values_of(node, "ignoredColumn"),
            "value_col": value_col, "measure_cols": _dali_values_of(node, "measureColumn"),
            "timestamp_col": next(iter(_dali_values_of(node, "timestampColumn")), "") or None,
            "carry_columns": _dali_values_of(node, "carryColumn")}


def fetch_series_layout(dataset_id: str, catalogue_id: str = "", asset_id: str = "") -> dict | None:
    """The series layout declared on the targeted distribution in piveau, or None (none declared, or not readable).

    Read the way publish_quality_to_piveau reads: with PIVEAU_API_KEY when set, and the catalogue when given, so a
    private catalogue is readable. Only the distribution matching asset_id (see dist_keys) is considered; without an
    asset_id, only a dataset with exactly one distribution is. Never raises: a catalogue that cannot be read means the
    file is processed as it is.
    """
    url = f"{PIVEAU_DATASETS_URL}/{dataset_id}" + (f"?catalogue={catalogue_id}" if catalogue_id else "")
    headers = {"Accept": "application/ld+json"}
    api_key = os.environ.get("PIVEAU_API_KEY", "").strip()
    if api_key:
        headers["X-API-Key"] = api_key
    try:
        with urllib.request.urlopen(urllib.request.Request(url, headers=headers), timeout=15) as resp:
            graph = json.loads(resp.read()).get("@graph", [])
    except Exception as exc:  # noqa: BLE001 - best effort, see docstring
        print(f"[dali] could not read the series layout from piveau: {exc}")
        return None
    dists = [n for n in graph if any("Distribution" in t for t in node_types(n))]
    if asset_id:
        dists = [n for n in dists if asset_id in dist_keys(n)]
    elif len(dists) != 1:
        return None
    return series_layout_of(dists[0]) if dists else None


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


def detect_format(asset_title: str) -> str:
    """The file format label (csv/tsv/jsonl/json) inferred from asset_title's
    extension — used both to pick a parser (see dataframe_from_content) and
    to label the pre-flight format check (see dali.validation.validate_file_format).
    Defaults to csv, which also covers files with no/unrecognized extension."""
    ext = asset_title.rsplit(".", 1)[-1].lower() if "." in asset_title else ""
    if ext == "tsv":
        return "tsv"
    if ext in ("jsonl", "ndjson"):
        return "jsonl"
    if ext == "json":
        return "json"
    return "csv"


def dataframe_from_content(content: str, asset_title: str):
    """Load a distribution's raw text content into a pandas DataFrame, picking
    the parser from asset_title's extension (see detect_format). Great
    Expectations itself is format-agnostic (it just validates whatever
    DataFrame it's handed) — this is the one place format support needs to
    be added for a new file type. Raises (ValueError, pandas.errors.ParserError,
    etc.) if the content doesn't actually match its expected format."""
    import io
    import pandas as pd

    fmt = detect_format(asset_title)
    if fmt == "tsv":
        return pd.read_csv(io.StringIO(content), sep="\t")
    if fmt == "jsonl":
        return pd.read_json(io.StringIO(content), lines=True)
    if fmt == "json":
        return pd.read_json(io.StringIO(content))
    return pd.read_csv(io.StringIO(content))


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
