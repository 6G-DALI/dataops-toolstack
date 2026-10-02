"""llm-dataset-transformer — single-file pipeline (6G-DALI mode).

Converts a JSON / JSONL dataset into a **6G-DALI-compliant CSV**, following the
6G-DALI Input Dataset Format spec. Two tiers:

  * DALI-clean tabular data (flat, no units, no stacked series) -> transformed
    locally: snake_case column names, empty-cell nulls, ascending timestamp sort.
  * Anything needing a meaning-changing transform (nested objects, embedded units
    like "2048M", composite cells, or a long-format file with a series-identifier
    column stacking the timeline) -> an AWS Bedrock Claude model writes a bespoke
    DALI transform (flatten / unit-split / long->wide pivot), which is saved,
    previewed and confirmed before it runs. This matches the spec's rule that
    meaning-changing steps are "a proposed transformation you confirm", not silent.

Spec sections encoded here: §3.1 (UTF-8, no BOM), §4.2 (snake_case), §5.1 (empty
nulls, no sentinels), §5.2 / §7.1 (one column = one variable), §5.3 / §7.2 / §7.3
(one row per timestamp, long->wide pivot), §6 (timestamps).

Usage
-----
    python pipeline.py data/1.jsonl -o out/1.csv          # auto-routed
    python pipeline.py data/samples/colors.json           # -> Bedrock DALI transform
    python pipeline.py data/x.json --force-llm            # force the model
    python pipeline.py data/x.json --script gen.py -y     # reuse a script
"""

from __future__ import annotations

import argparse
import csv
import json
import os
import re
import subprocess
import sys
from datetime import datetime
from pathlib import Path

try:
    from dotenv import load_dotenv

    load_dotenv()
except Exception:
    pass

DEFAULT_REGION = "us-east-1"
DEFAULT_MODEL = "anthropic.claude-opus-4-8"
_VALUE_PREVIEW_CHARS = 240
_MAX_TOKENS = 32000
_PREVIEW_LINES = 40
_SCAN_LIMIT = 5000

# §6.2 timestamp-column names, in priority order.
_TS_NAMES = [
    "timestamp", "time_stamp", "event_timestamp", "event_time", "datetime",
    "date_time", "time", "ts", "epoch", "epoch_time", "unix_time", "measured_at",
    "observed_at", "recorded_at", "collected_at", "logged_at", "date", "day",
    "created_at", "updated_at", "inserted_at",
]
_TS_SUFFIXES = ("_timestamp", "_datetime", "_time", "_date", "_at")
# A value that is a number immediately followed by a unit, e.g. 2048M, 12ms, 85%.
_UNIT_RE = re.compile(r"^-?\d+(?:\.\d+)?\s*[A-Za-z%°Ω/]+$")


# --------------------------------------------------------------------------- #
# §4.2 column-name normalisation
# --------------------------------------------------------------------------- #
def to_snake(name) -> str:
    """Lowercase, non-alphanumeric runs -> single '_', trim '_'. Per §4.2."""
    return re.sub(r"_+", "_", re.sub(r"[^0-9a-zA-Z]+", "_", str(name).strip().lower())).strip("_")


def normalize_headers(columns: list[str]) -> tuple[dict[str, str], list[str]]:
    """Map each original column to a unique snake_case name; warn on collisions."""
    mapping: dict[str, str] = {}
    used: set[str] = set()
    warnings: list[str] = []
    for col in columns:
        base = to_snake(col) or "col"
        name = base
        i = 2
        while name in used:
            warnings.append(f"column name collision: '{col}' -> '{base}' already taken; using '{base}_{i}'")
            name = f"{base}_{i}"
            i += 1
        used.add(name)
        mapping[col] = name
    return mapping, warnings


# --------------------------------------------------------------------------- #
# Reading — JSONL (streamed), top-level array, {"key":[...]} wrapper, lone object
# --------------------------------------------------------------------------- #
def _first_nonspace_char(path: Path) -> str:
    with path.open("r", encoding="utf-8") as fh:
        while True:
            ch = fh.read(1)
            if ch == "":
                return ""
            if not ch.isspace():
                return ch


def _first_line_is_json(path: Path) -> bool:
    with path.open("r", encoding="utf-8") as fh:
        for line in fh:
            line = line.strip()
            if not line:
                continue
            try:
                json.loads(line)
                return True
            except json.JSONDecodeError:
                return False
    return False


def _unwrap_object(data: dict) -> tuple[list, str]:
    list_keys = [k for k, v in data.items() if isinstance(v, list)]
    if len(list_keys) == 1:
        return data[list_keys[0]], f"unwrapped top-level '{list_keys[0]}' array"
    return [data], "single JSON object -> one record"


def describe_source(path: Path) -> str:
    first = _first_nonspace_char(path)
    if first == "":
        return "empty file"
    if first == "[":
        return "top-level JSON array"
    if first == "{":
        if _first_line_is_json(path):
            return "JSONL (one object per line)"
        data = json.loads(path.read_text(encoding="utf-8"))
        return _unwrap_object(data)[1] if isinstance(data, dict) else "single JSON value"
    return "JSONL (one value per line)"


def iter_records(path: Path, max_records: int | None = None):
    first = _first_nonspace_char(path)
    if first == "":
        return
    if first == "{" and _first_line_is_json(path):  # JSONL, streamed
        count = 0
        with path.open("r", encoding="utf-8") as fh:
            for line in fh:
                line = line.strip()
                if not line:
                    continue
                yield json.loads(line)
                count += 1
                if max_records is not None and count >= max_records:
                    return
        return
    if first in "[{":  # whole-file JSON
        data = json.loads(path.read_text(encoding="utf-8"))
        if isinstance(data, dict):
            records, _ = _unwrap_object(data)
        elif isinstance(data, list):
            records = data
        else:
            records = [data]
        for i, rec in enumerate(records):
            if max_records is not None and i >= max_records:
                return
            yield rec
        return
    count = 0  # fallback: JSONL of bare values
    with path.open("r", encoding="utf-8") as fh:
        for line in fh:
            line = line.strip()
            if not line:
                continue
            yield json.loads(line)
            count += 1
            if max_records is not None and count >= max_records:
                return


# --------------------------------------------------------------------------- #
# Profiling — decide DALI-clean vs needs-transform, and gather hints
# --------------------------------------------------------------------------- #
def _find_timestamp(fields: list[str]) -> str | None:
    snake = {f: to_snake(f) for f in fields}
    for want in _TS_NAMES:  # priority order
        for f in fields:
            if snake[f] == want:
                return f
    for f in fields:  # then suffix match
        if snake[f].endswith(_TS_SUFFIXES):
            return f
    return None


def profile(path: Path, scan_limit: int = _SCAN_LIMIT) -> dict:
    fields: list[str] = []
    nested_fields: list[str] = []
    non_dict = 0
    scanned = 0
    distinct: dict[str, set] = {}
    unit_hits: dict[str, int] = {}
    nonempty: dict[str, int] = {}

    for rec in iter_records(path, max_records=scan_limit):
        scanned += 1
        if not isinstance(rec, dict):
            non_dict += 1
            continue
        for k, v in rec.items():
            if k not in fields:
                fields.append(k)
                distinct[k] = set()
                unit_hits[k] = 0
                nonempty[k] = 0
            if isinstance(v, (dict, list)):
                if k not in nested_fields:
                    nested_fields.append(k)
                continue
            if v is None or v == "":
                continue
            nonempty[k] += 1
            if len(distinct[k]) < 500:
                distinct[k].add(v)
            if isinstance(v, str) and _UNIT_RE.match(v.strip()):
                unit_hits[k] += 1

    ts_col = _find_timestamp(fields)
    n_dict = scanned - non_dict
    distinct_ts = len(distinct.get(ts_col, set())) if ts_col else 0
    long_format = bool(ts_col) and 0 < distinct_ts < n_dict

    # embedded-unit columns: majority of non-empty values look like number+unit
    unit_fields = [f for f in fields if nonempty[f] and unit_hits[f] >= 0.5 * nonempty[f]]

    # candidate series-identifier columns: low-cardinality non-timestamp fields
    id_candidates = []
    if long_format:
        cap = max(2, n_dict // 2)
        for f in fields:
            if f == ts_col or f in nested_fields:
                continue
            d = len(distinct[f])
            if 1 < d <= cap and d < distinct_ts:
                id_candidates.append(f)

    needs_llm = bool(nested_fields or non_dict or long_format or unit_fields)
    return {
        "fields": fields,
        "scanned": scanned,
        "nested_fields": nested_fields,
        "non_dict": non_dict,
        "ts_col": ts_col,
        "distinct_ts": distinct_ts,
        "long_format": long_format,
        "unit_fields": unit_fields,
        "id_candidates": id_candidates,
        "needs_llm": needs_llm,
    }


def route_reason(p: dict) -> str:
    bits = []
    if p["non_dict"]:
        bits.append(f"{p['non_dict']} non-object record(s)")
    if p["nested_fields"]:
        bits.append(f"nested field(s): {', '.join(p['nested_fields'])}")
    if p["long_format"]:
        who = ", ".join(p["id_candidates"]) or "an identifier"
        bits.append(f"long-format: timestamps repeat, stacked by {who} (needs pivot)")
    if p["unit_fields"]:
        bits.append(f"embedded units in: {', '.join(p['unit_fields'])}")
    return "; ".join(bits) if bits else "DALI-clean tabular"


# --------------------------------------------------------------------------- #
# DALI-clean local tier — snake_case, empty nulls, ascending timestamp sort
# --------------------------------------------------------------------------- #
def _ts_key(v):
    try:
        return (0, float(v))
    except (TypeError, ValueError):
        return (1, str(v))


def local_transform(path: Path, output_path: Path, ts_col: str | None) -> tuple[int, int, list[str]]:
    columns: list[str] = []
    for rec in iter_records(path):
        if isinstance(rec, dict):
            for k in rec:
                if k not in columns:
                    columns.append(k)
    mapping, warns = normalize_headers(columns)

    rows = [rec for rec in iter_records(path) if isinstance(rec, dict)]
    if ts_col and ts_col in columns:  # §5.3 ascending order
        rows.sort(key=lambda r: _ts_key(r.get(ts_col)))

    output_path.parent.mkdir(parents=True, exist_ok=True)
    with output_path.open("w", newline="", encoding="utf-8") as out:
        writer = csv.DictWriter(out, fieldnames=[mapping[c] for c in columns], extrasaction="ignore")
        writer.writeheader()
        for rec in rows:
            writer.writerow({mapping[c]: rec.get(c, "") for c in columns})
    return len(rows), len(columns), warns


# --------------------------------------------------------------------------- #
# LLM tier — Bedrock writes a DALI-compliant transform
# --------------------------------------------------------------------------- #
_SYSTEM = (
    "You are a senior data engineer preparing datasets for the 6G-DALI platform. "
    "You write correct, self-contained Python scripts that convert one dataset to "
    "a 6G-DALI-compliant CSV. You output ONLY Python code — no prose, no fences."
)

_DALI_CONTRACT = """\
Write ONE self-contained Python 3 script (standard library ONLY) that converts the
described dataset to a 6G-DALI-COMPLIANT CSV.

MECHANICS
- argparse args exactly: --input PATH and --output PATH. Do not hardcode paths.
- Handle the real file shape (JSONL, a top-level JSON array, a {"key":[...]} wrapper,
  or a single object). Write CSV via csv.DictWriter, UTF-8, no BOM, comma-delimited.
- Print a final one-line summary: rows and columns written.

6G-DALI RULES
- Column names (§4.2): ASCII lowercase snake_case — lowercase, replace each run of
  non-alphanumeric characters with a single "_", trim leading/trailing "_"
  (nested key rgb.r -> rgb_r). No two output columns may collide after this.
- One column = one variable (§5.2, §7.1):
  * Embedded units — a value like "2048M", "12ms", "85%" — must become a NUMBER in a
    column whose name carries the unit (e.g. ram_limit -> ram_limit_mb = 2048).
  * Composite cells — "lat=48.85,lon=2.35", packed strings, or nested objects — must
    be SPLIT into separate columns. Flatten nested objects into snake_case columns.
  * Do NOT concatenate distinct values into one cell (no "a|b|c"); if an array is real
    repeated readings, expand into indexed columns (name_0, name_1, ...).
- Missing values (§5.1): leave the cell EMPTY. Never write sentinels (-1, 0, 999, "N/A").
- Time series (§5.3, §7.2, §7.3) — ONLY if a timestamp column exists:
  * Exactly one row per timestamp, rows sorted ASCENDING by timestamp.
  * If a series-identifier column makes timestamps repeat (LONG format), PIVOT to WIDE:
    one row per timestamp, one column per (identifier, metric) named
    <identifier>_<metric> in snake_case. Align by timestamp; unreported cells stay empty.
  * Timestamps must be Unix epoch SECONDS or ISO-8601; if values are epoch milliseconds,
    divide to seconds.
- Otherwise keep scalar values VERBATIM (do not reformat clean numbers/strings; do not
  decode opaque blobs/hex — leave them in their own column unchanged).

WORKED EXAMPLES 

A) Embedded unit . A column `ram_limit` with values "2048M", "1024M", "4096M"
   -> a numeric column `ram_limit_mb` with 2048, 1024, 4096. The unit moves into the
   name; the value becomes a plain number.

B) Long -> wide pivot . One row per (timestamp, identifier) is WRONG for a
   time series; pivot so each timestamp is one row and the identifier folds into the
   column names as <identifier>_<metric>.
   BEFORE (long — `timestamp` repeats, `node_id` is the series identifier):
       timestamp,            node_id, rsrp, throughput
       2026-05-27T10:00:00Z, node-1,  -85,  120.4
       2026-05-27T10:00:00Z, node-2,  -91,  98.1
       2026-05-27T10:01:00Z, node-1,  -84,  121.0
   AFTER (wide — one row per timestamp):
       timestamp,            node_1_rsrp, node_2_rsrp, node_1_throughput, node_2_throughput
       2026-05-27T10:00:00Z, -85,         -91,         120.4,             98.1
       2026-05-27T10:01:00Z, -84,         ,            121.0,
   Where a source did not report at a timestamp, the cell is EMPTY (never 0 or a sentinel).

Output ONLY the Python source code. No ``` fences, no commentary.
"""


def _preview_value(v):
    if isinstance(v, str) and len(v) > _VALUE_PREVIEW_CHARS:
        return f"{v[:_VALUE_PREVIEW_CHARS]}...[truncated; full length={len(v)} chars]"
    return v


def _normalize_model(model: str, region: str) -> str:
    if model.split(".", 1)[0] in ("us", "eu", "apac", "global"):
        return model
    prefix = {"us": "us", "eu": "eu", "ap": "apac"}.get(region.split("-")[0])
    return f"{prefix}.{model}" if prefix and model.startswith("anthropic.") else model


def generate_code(path: Path, p: dict, fmt: str, region: str, model: str) -> str:
    from anthropic import AnthropicBedrock

    sample = [
        {k: _preview_value(v) for k, v in rec.items()} if isinstance(rec, dict) else rec
        for rec in iter_records(path, max_records=6)
    ]
    hints = (
        f"- timestamp column: {p['ts_col'] or 'none'}\n"
        f"- long-format (timestamps repeat): {'yes' if p['long_format'] else 'no'}\n"
        f"- likely series-identifier column(s): {', '.join(p['id_candidates']) or 'none'}\n"
        f"- columns with embedded units: {', '.join(p['unit_fields']) or 'none'}\n"
        f"- nested fields: {', '.join(p['nested_fields']) or 'none'}\n"
        f"Apply the long->wide pivot ONLY if long-format is yes."
    )
    user = (
        f"Dataset file: {path.name}\nDetected shape: {fmt}\n"
        f"Field names observed: {p['fields']}\n\n"
        f"Analysis hints for THIS dataset:\n{hints}\n\n"
        f"Sample records (long string values truncated; handle full-size values):\n"
        f"{json.dumps(sample, indent=2, ensure_ascii=False)}\n\n"
        f"{_DALI_CONTRACT}"
    )
    client = AnthropicBedrock(aws_region=region)
    with client.messages.stream(
        model=model, max_tokens=_MAX_TOKENS, system=_SYSTEM,
        thinking={"type": "adaptive"}, messages=[{"role": "user", "content": user}],
    ) as stream:
        final = stream.get_final_message()
    code = "".join(b.text for b in final.content if b.type == "text").strip()
    if code.startswith("```"):
        code = code.split("```", 2)[1]
        code = code[len("python"):].lstrip("\n") if code.startswith("python") else code
        code = code.rsplit("```", 1)[0]
    if not code.strip():
        raise RuntimeError("Model returned no code. Check model access and try again.")
    return code.strip() + "\n"


def save_script(code: str, input_path: Path) -> Path:
    gen_dir = Path("generated")
    gen_dir.mkdir(parents=True, exist_ok=True)
    stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    p = gen_dir / f"dali_{input_path.stem}_{stamp}.py"
    p.write_text(
        f"# AUTO-GENERATED (6G-DALI transform) by pipeline.py on {stamp}\n"
        f"# Source: {input_path.name}. Review before trusting.\n\n{code}",
        encoding="utf-8",
    )
    return p


def preview(script_path: Path) -> None:
    lines = script_path.read_text(encoding="utf-8").splitlines()
    print(f"\n--- {script_path} ({len(lines)} lines) ---")
    for ln in lines[:_PREVIEW_LINES]:
        print(f"  {ln}")
    if len(lines) > _PREVIEW_LINES:
        print(f"  ... ({len(lines) - _PREVIEW_LINES} more lines - open the file)")
    print("--- end preview ---\n")


def run_script(script_path: Path, input_path: Path, output_path: Path) -> int:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    cmd = [sys.executable, str(script_path), "--input", str(input_path), "--output", str(output_path)]
    print(f"Running: {' '.join(cmd)}\n")
    r = subprocess.run(cmd, capture_output=True, text=True)
    if r.stdout:
        print(r.stdout.rstrip())
    if r.returncode != 0:
        print("--- transform stderr ---", file=sys.stderr)
        print(r.stderr, file=sys.stderr)
    return r.returncode


# --------------------------------------------------------------------------- #
# CLI
# --------------------------------------------------------------------------- #
def main(argv=None) -> int:
    ap = argparse.ArgumentParser(
        prog="pipeline.py",
        description="JSON/JSONL -> 6G-DALI-compliant CSV. Clean tabular data is "
        "normalised locally; data needing a meaning-changing transform (nesting, "
        "units, long-format) is sent to an AWS Bedrock LLM under a review gate.",
    )
    ap.add_argument("input", type=Path, help="input .json / .jsonl file")
    ap.add_argument("-o", "--output", type=Path, help="output CSV (default: <input>.csv)")
    ap.add_argument("--script", type=Path, help="reuse a saved script (no analysis/model)")
    ap.add_argument("--force-llm", action="store_true", help="use the model even if DALI-clean")
    ap.add_argument("--region", help="AWS region (overrides AWS_REGION)")
    ap.add_argument("--model", help="Bedrock model id (overrides BEDROCK_MODEL_ID)")
    ap.add_argument("-y", "--yes", action="store_true", help="skip the review prompt")
    ap.add_argument("--dry-run", action="store_true", help="LLM path: save script, don't run")
    args = ap.parse_args(argv)

    if not args.input.exists():
        print(f"error: input not found: {args.input}", file=sys.stderr)
        return 2
    output_path = args.output or args.input.with_suffix(".csv")

    def _gate_and_run(script_path: Path) -> int:
        preview(script_path)
        if args.dry_run:
            print(f"--dry-run: not executing. Re-run with --script {script_path}")
            return 0
        if not args.yes:
            try:
                if input("Execute this script? [y/N] ").strip().lower() not in ("y", "yes"):
                    print(f"Aborted. Run later with --script {script_path}")
                    return 1
            except EOFError:
                print(f"No TTY for confirmation; re-run with -y or --script {script_path}", file=sys.stderr)
                return 1
        rc = run_script(script_path, args.input, output_path)
        print(f"\nDone -> {output_path}" if rc == 0 else f"\nFailed (exit {rc}).")
        return rc

    # Route 1: reuse a script
    if args.script:
        if not args.script.exists():
            print(f"error: script not found: {args.script}", file=sys.stderr)
            return 2
        print(f"Reusing script: {args.script}")
        return _gate_and_run(args.script)

    # Profile
    print(f"Analyzing {args.input} ...")
    fmt = describe_source(args.input)
    print(f"  shape: {fmt}")
    p = profile(args.input)
    print(f"  scanned={p['scanned']}  ts_col={p['ts_col']}  -> {route_reason(p)}")

    # Route 2: DALI-clean -> local
    if not p["needs_llm"] and not args.force_llm:
        print("DALI-clean -> local transform (snake_case, empty nulls; no model call).")
        rows, cols, warns = local_transform(args.input, output_path, p["ts_col"])
        for w in warns:
            print(f"  WARN: {w}")
        print(f"Wrote {rows} rows x {cols} columns -> {output_path}")
        return 0

    # Route 3: needs a DALI transform -> Bedrock
    print(f"DALI transform via LLM ({'forced' if args.force_llm else route_reason(p)}).")
    if not os.getenv("AWS_ACCESS_KEY_ID"):
        print("error: AWS credentials not found. Set AWS_ACCESS_KEY_ID / "
              "AWS_SECRET_ACCESS_KEY (or a .env), or pass --script.", file=sys.stderr)
        return 2
    region = args.region or os.getenv("AWS_REGION") or DEFAULT_REGION
    model = _normalize_model(args.model or os.getenv("BEDROCK_MODEL_ID") or DEFAULT_MODEL, region)
    print(f"Asking Bedrock ({model} @ {region}) to write a DALI transform ...")
    code = generate_code(args.input, p, fmt, region, model)
    script_path = save_script(code, args.input)
    print(f"Saved: {script_path}")
    return _gate_and_run(script_path)


if __name__ == "__main__":
    raise SystemExit(main())
