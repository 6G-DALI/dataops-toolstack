"""Timezone-aware timestamps, made naive UTC before the vendored pipeline reads them.

The vendored pipeline (plugins/VENDORED.md) subtracts a naive epoch from the parsed time column, for example in
dataops.timeline._as_seconds, and pandas refuses to subtract a naive timestamp from a timezone-aware one:

    TypeError: Cannot subtract tz-naive and tz-aware datetime-like objects

A time column whose values carry an offset or a "Z" ("2026-10-22T07:52:29Z", "2026-09-10 20:07:46.000 +0300") therefore
crashes the pipeline before it cleans or imputes anything. The vendored code is not edited, so the file is adapted
here, before it is handed over: the time column is converted to UTC and written without an offset. Every other column
is passed on as text, untouched. A narrow dataset is already naive UTC when it gets here (see dali.unwrap), and a column
without offsets is left alone.
"""

from __future__ import annotations

import re
from pathlib import Path

import pandas as pd

_OFFSET = re.compile(r"(Z|[+-]\d{2}:?\d{2})\s*$")
_SAMPLE_ROWS = 1000


def _normalised(name: str) -> str:
    return re.sub(r"[^a-z0-9]+", "_", str(name).strip().lower()).strip("_")


def _find_column(columns, wanted: str) -> str | None:
    """The header named `wanted`: exactly, else by the same snake_case form the pipeline gives it."""
    if wanted in columns:
        return wanted
    target = _normalised(wanted)
    for column in columns:
        if _normalised(column) == target:
            return column
    return None


def normalise_timestamps(csv_path: Path, timestamp_col: str | None, out_path: Path) -> dict | None:
    """Write `csv_path` to `out_path` with a timezone-aware time column converted to naive UTC.

    Returns what was done, or None when nothing needed doing (no time column named, no such column, or its values carry
    no offset); `out_path` is then not written. The result is
    ``{"column", "offset", "rows", "unparsed", "timezone": "UTC"}`` where `offset` is the first one seen.
    A value that cannot be parsed is passed on as it was, so the pipeline's own checks report it.
    """
    if not timestamp_col:
        return None
    frame = pd.read_csv(csv_path, dtype=str, keep_default_na=False)
    column = _find_column(list(frame.columns), timestamp_col)
    if column is None:
        return None

    text = frame[column]
    present = text[text.str.strip() != ""]
    first_offset = None
    for value in present.head(_SAMPLE_ROWS):
        match = _OFFSET.search(value.strip())
        if match:
            first_offset = match.group(1)
            break
    if first_offset is None:
        return None

    try:
        parsed = pd.to_datetime(text.where(text.str.strip() != ""), format="mixed", utc=True, errors="coerce")
    except (TypeError, ValueError):
        parsed = pd.to_datetime(text.where(text.str.strip() != ""), utc=True, errors="coerce")
    naive = parsed.dt.tz_localize(None)

    # One format for every row: the pipeline infers a single format from the first value, and a column that mixes whole
    # and fractional seconds is read badly (see dali.processing._tolerate_mixed_time_formats).
    fractional = bool((naive.dropna().dt.microsecond != 0).any())
    rendered = naive.dt.strftime("%Y-%m-%d %H:%M:%S.%f" if fractional else "%Y-%m-%d %H:%M:%S")
    bad = naive.isna() & (text.str.strip() != "")
    frame[column] = rendered.where(naive.notna(), text)

    frame.to_csv(out_path, index=False)
    return {
        "column": column,
        "offset": "UTC" if first_offset == "Z" else first_offset,
        "rows": int(naive.notna().sum()),
        "unparsed": int(bad.sum()),
        "timezone": "UTC",
    }
