"""Quality checks on the file as received: Great Expectations and pandera over the file's own columns.

dali.validation.run_expectations runs these over the downloaded file, before any unwrapping or cleaning, so every
check, and every quality measurement published from it, names a column the submitter's file has. The DataOps pipeline
does not repeat them (processing.run_imputation switches them off); merge_quality_report joins both halves.

The suite is the one the pipeline used to build for itself, from the same vendored builders
(dataops.ts_checks.build_expectations and dataops.tabular_checks.build_expectations), over the raw frame instead of the
cleaned, possibly unwrapped one.

A narrow file (one row per timestamp, entity and metric, its series layout declared in the catalogue) has no unique
timestamp by design. For it the suite checks that a series has one value per timestamp instead (the timestamp plus the
hierarchy's columns, compound unique), does not require sorted or unique timestamps, and does not ask the hierarchy's
columns to be filled: a level that does not apply to a row is absent on purpose.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

import pandas as pd

from dali import unwrap as narrow

_TS_ONLY_CHECKS = ("expect_column_values_to_be_unique", "expect_column_values_to_be_increasing")
_NOT_NULL = "expect_column_values_to_not_be_null"


@dataclass
class RawPlan:
    """What the checks of one run are built from."""

    configured_mode: str
    mode: str                                   # time_series, tabular, none, or what the run configured
    timestamp_col: str | None
    config: dict
    key_columns: list[str] = field(default_factory=list)   # the series key of a narrow file, empty otherwise


def _config(params: dict) -> dict:
    from dali.processing import DEFAULT_VALIDATION_CONFIG

    return params.get("validation") or DEFAULT_VALIDATION_CONFIG


def _key_columns(df: pd.DataFrame, layout: dict | None) -> list[str]:
    """The hierarchy's columns that the file has, in order, for a narrow file; [] when no layout is declared."""
    if not layout or not layout.get("hierarchy"):
        return []
    try:
        templates = narrow.parse_hierarchies(layout["hierarchy"], layout.get("separator") or "_")
    except Exception:  # noqa: BLE001 - a layout that does not parse is reported by the unwrap, not here
        return []
    fields: list[str] = []
    for names, _ in templates:
        for name in names:
            if name not in fields:
                fields.append(name)
    return [name for name in fields if name in df.columns]


def plan(df: pd.DataFrame, params: dict, layout: dict | None = None) -> RawPlan:
    """Resolve the validation mode and the timestamp column of the file as received.

    The same rules as the pipeline's, over the raw frame: the run's `timestamp_col` when the file has it, else a
    guess from the column names, else the profile's own detection. `validation.mode` overrides; "auto" is a time
    series when a timestamp column is found and tabular data otherwise.
    """
    cfg = _config(params)
    configured = cfg.get("mode", "auto")
    key_columns = _key_columns(df, layout)
    if configured in ("none", "tabular"):
        return RawPlan(configured, configured, None, cfg, key_columns)

    ts = params.get("timestamp_col") or None
    if ts not in df.columns:
        ts = None
    if ts is None:
        from dali.processing import guess_timestamp_in_frame

        ts = guess_timestamp_in_frame(df.head(200))
    if ts is None and configured == "auto":
        try:
            from dataops.profiling import profile

            found = profile(df, timestamp_col=None,
                            allow_step_index_timestamp=bool(cfg.get("allow_step_index_timestamp", False)))
            if found.get("data_type") == "time_series":
                ts = found.get("timestamp_column")
        except Exception as exc:  # noqa: BLE001 - detection is best effort
            print(f"[dali] could not profile the file to find a timestamp column: {exc}")
    if configured == "auto":
        mode = "time_series" if ts else "tabular"
    else:
        mode = configured
    return RawPlan(configured, mode, ts, cfg, key_columns)


def auto_expectations(df: pd.DataFrame, p: RawPlan) -> list:
    """The pipeline's own suite for this file, as GX expectation objects over the file's columns."""
    import great_expectations.expectations as gxe

    cfg = p.config
    if p.mode == "time_series" and p.timestamp_col in df.columns:
        from dataops import ts_checks

        expectations, _ = ts_checks.build_expectations(
            df, p.timestamp_col,
            miss_threshold=float(cfg.get("gx_missing_mostly", 0.98)),
            outlier_q=float(cfg.get("outlier_q", 0.01)),
            outlier_mostly=float(cfg.get("outlier_mostly", 0.95)),
        )
    elif p.mode == "tabular":
        from dataops import tabular_checks

        expectations, _, _ = tabular_checks.build_expectations(
            df,
            miss_threshold_numeric=float(cfg.get("gx_missing_mostly_numeric", 0.95)),
            miss_threshold_cat=float(cfg.get("gx_missing_mostly_cat", 0.90)),
            outlier_q=float(cfg.get("outlier_q", 0.01)),
            outlier_mostly=float(cfg.get("outlier_mostly", 0.95)),
        )
    else:
        return []

    if p.key_columns and p.mode == "time_series":
        def _drop(e) -> bool:
            kind, col = getattr(e, "expectation_type", ""), getattr(e, "column", None)
            return (kind in _TS_ONLY_CHECKS and col == p.timestamp_col) or (kind == _NOT_NULL and col in p.key_columns)

        expectations = [e for e in expectations if not _drop(e)]
        expectations.append(gxe.ExpectCompoundColumnsToBeUnique(column_list=[p.timestamp_col, *p.key_columns]))
    return expectations


def _epoch_seconds(column: pd.Series) -> pd.Series | None:
    """A timestamp column as epoch seconds, or None when it is already numeric or does not parse."""
    if pd.api.types.is_numeric_dtype(column):
        return None
    parsed = pd.to_datetime(column, errors="coerce", utc=True)
    if parsed.notna().mean() < 0.9:
        return None
    return (parsed.dt.tz_localize(None) - pd.Timestamp("1970-01-01")).dt.total_seconds()


def gx_frame(df: pd.DataFrame, p: RawPlan) -> pd.DataFrame:
    """The frame the generated suite runs on: the file as received, its timestamp column as epoch seconds.

    The column keeps its name, so every check still names the file's own column; only the values change, so that
    "increasing" and the quantile bands compare numbers, as the pipeline's suite always did.
    """
    if p.mode != "time_series" or p.timestamp_col not in df.columns:
        return df
    seconds = _epoch_seconds(df[p.timestamp_col])
    if seconds is None:
        return df
    out = df.copy()
    out[p.timestamp_col] = seconds
    return out


def pandera_check(df: pd.DataFrame, p: RawPlan) -> dict:
    """The pandera schema check over the file as received: {configured_mode, mode, pandera_passed, timestamp_column, errors}.

    `pandera_passed` stays None when there is nothing to say: mode "none", or pandera not installed.
    """
    result: dict[str, Any] = {"configured_mode": p.configured_mode, "mode": p.mode, "pandera_passed": None,
                              "timestamp_column": p.timestamp_col, "errors": []}
    if p.mode == "none":
        return result
    try:
        from data_process_modules.validation import validate_numeric_timeseries, validate_tabular_dataframe
    except Exception as exc:  # noqa: BLE001 - the schema check is optional where pandera is missing
        result["errors"] = [f"pandera is not available: {exc}"]
        return result

    cfg = p.config
    frame = df.copy()
    narrow_file = bool(p.key_columns)
    for col in p.key_columns:
        frame[col] = frame[col].fillna("N/A")           # a level that does not apply is absent on purpose
    kwargs = dict(
        expected_columns=cfg.get("expected_columns") or None,
        numeric_bounds=cfg.get("numeric_bounds") or None,
        missing_threshold=float(cfg.get("missing_threshold", 0.0)),
    )
    try:
        if p.mode == "time_series":
            if not p.timestamp_col:
                raise ValueError("time_series validation requires a timestamp column")
            if not pd.api.types.is_numeric_dtype(frame[p.timestamp_col]):
                parsed = pd.to_datetime(frame[p.timestamp_col], errors="coerce", utc=True)
                frame[p.timestamp_col] = parsed.dt.tz_localize(None)
            validate_numeric_timeseries(
                frame, timestamp_col=p.timestamp_col,
                require_timestamp_unique=bool(cfg.get("require_timestamp_unique", True)) and not narrow_file,
                require_timestamp_monotonic=bool(cfg.get("require_timestamp_monotonic", True)) and not narrow_file,
                **kwargs,
            )
        elif p.mode == "tabular":
            validate_tabular_dataframe(frame, **kwargs)
        else:
            raise ValueError("validation.mode must be one of: auto, time_series, tabular, none")
        result["pandera_passed"] = True
    except Exception as exc:  # noqa: BLE001 - reported in the entry, not raised
        result["pandera_passed"] = False
        result["errors"] = [str(exc)]
    return result
