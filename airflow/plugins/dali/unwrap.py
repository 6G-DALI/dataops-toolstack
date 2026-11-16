"""
Unwrap a narrow (long) measurement CSV into one time series per column, and wrap the processed series back.

The imputation pipeline works on a wide frame: one timestamp column and one column per series. Many testbed datasets
arrive narrow instead: one row per (timestamp, entity, metric), with the entity spread over id columns, e.g.

    source,metric_id,timestamp,unit,value,cell_id,gNB_id,type,ue_id
    Digital_Twin,DRB.UEThpDl,2026-05-28T11:36:54.462892+02:00,N/A,9.28,1,1193046,Cell,N/A

A run names the hierarchy that identifies one series: the id columns, in order, the last one the metric,

    cell_id,ue_id,metric_id          (joined with a separator, "_" unless given)

(the older template form, {cell_id}_{ue_id}_{metric_id}, is accepted too) and every distinct combination becomes a
column named that way (`1_DRB.UEThpDl` for the cell row above,
`3_4_DRB.UEBlerDl` for a UE row). A level that is absent for a row (ue_id "N/A" on a cell-level row) is left out of
the name, with its separator. The last field of the template is the metric.

There can be several hierarchies, for datasets whose rows identify a series by different columns (cell-level and
UE-level rows): a list of them, or templates written out and comma-separated,

    {cell_id}_{metric_id},{cell_id}_{ue_id}_{metric_id}

Each row takes the hierarchy with the most fields that all hold a value on that row (ties: the first listed); a row
that fits none takes the first, with its absent levels left out. All end in the same metric field.

`ignore` lists columns of the narrow file that do not change (`source`, `gNB_id`: one value in the whole dataset). They
are left out of the series names and the processing, and `wrap` puts them back with that value.

    wide, state = unwrap(narrow, "cell_id,ue_id,metric_id", ignore=["source", "gNB_id"])
    ... the pipeline fills `wide` ...
    narrow_again = wrap(filled, state)

`unwrap` also turns the timestamps into timezone-naive UTC: the pipeline's time-column detection cannot subtract a
naive epoch from timezone-aware values. `wrap` renders them back in the offset the input used.

Pure pandas, nothing imported from Airflow, so it is testable on its own.
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from typing import Iterable, Mapping

import pandas as pd

# Tokens that mean "this level does not apply to the row". They are dropped from the series name and written back
# exactly as they came.
ABSENT = frozenset({"", "n/a", "na", "nan", "none", "null", "-"})

_FIELD = re.compile(r"\{([^{}]+)\}")
_TIMESTAMP_FORMAT = "%Y-%m-%dT%H:%M:%S.%f"


class UnwrapError(ValueError):
    """The hierarchy, the ignored columns or the data do not fit together."""


def parse_hierarchy(template: str) -> tuple[list[str], str]:
    """The fields of a hierarchy template and the separator between them.

    "{gNB_id}_{cell_id}_{ue_id}_{metric_id}" -> (["gNB_id", "cell_id", "ue_id", "metric_id"], "_").
    At least two fields and one separator, used between all of them; the last field is the metric.
    """
    template = (template or "").strip()
    pieces = _FIELD.split(template)              # [text, field, text, field, …, text]
    fields, literals = pieces[1::2], pieces[0::2]
    if len(fields) < 2:
        raise UnwrapError(f"hierarchy {template!r} needs at least two {{fields}}, the last one the metric")
    leading, between, trailing = literals[0], set(literals[1:-1]), literals[-1]
    if leading or trailing or len(between) != 1 or "" in between:
        raise UnwrapError(
            f"hierarchy {template!r}: use the fields and one separator between all of them, e.g. {{a}}_{{b}}_{{c}}")
    if len(set(fields)) != len(fields):
        raise UnwrapError(f"hierarchy {template!r} repeats a field")
    return fields, between.pop()


def _hierarchy_from_fields(fields: list[str], separator: str, raw: str) -> tuple[list[str], str]:
    if len(fields) < 2:
        raise UnwrapError(f"hierarchy {raw!r} needs at least two fields, the last one the metric")
    if len(set(fields)) != len(fields):
        raise UnwrapError(f"hierarchy {raw!r} repeats a field")
    if not separator or any(ch in separator for ch in "{},"):
        raise UnwrapError(f"separator {separator!r} must be non-empty and contain no braces or commas")
    return fields, separator


def parse_hierarchies(spec, separator: str = "_") -> list[tuple[list[str], str]]:
    """The hierarchies, as [(fields, separator), …] in the order given.

    `spec` is one string or a list of strings. A string without braces is the fields in order, comma-separated
    ("cell_id,ue_id,metric_id"), joined with `separator`. A string with braces is a template written out
    ("{cell_id}_{ue_id}_{metric_id}"), which carries its own separator; several are comma-separated.
    All must end in the same metric field.
    """
    templates = []
    for raw in ([spec] if isinstance(spec, str) else list(spec or [])):
        raw = str(raw).strip()
        if not raw:
            continue
        if "{" in raw:
            templates += [parse_hierarchy(p) for p in raw.split(",") if p.strip()]
        else:
            fields = [f.strip() for f in raw.split(",") if f.strip()]
            if fields:
                templates.append(_hierarchy_from_fields(fields, separator, raw))
    if not templates:
        raise UnwrapError("the hierarchy is empty")
    metrics = {fields[-1] for fields, _ in templates}
    if len(metrics) > 1:
        raise UnwrapError(f"every hierarchy must end in the same metric field, these end in {sorted(metrics)}")
    return templates


def _absent(token: str) -> bool:
    return str(token).strip().lower() in ABSENT


def series_name(parts: Iterable[str], sep: str) -> str:
    """The column name for one series: the parts that apply, joined."""
    return sep.join(str(p) for p in parts if not _absent(p))


@dataclass
class UnwrapState:
    """What `wrap` needs to put the series back."""

    fields: list[str]                                   # every field of every template, in order of appearance
    sep: str                                            # the first template's separator
    timestamp_col: str
    value_col: str
    columns: list[str]                                  # the narrow frame's columns, in their order
    series: dict[str, dict[str, str]]                   # series name -> every non-time, non-value column's token
    constants: dict[str, str]                           # ignored column -> its one value, as it came
    tz_offset: str = ""                                 # "+02:00", or "" when the input carried none
    templates: list[tuple[list[str], str]] = field(default_factory=list)
    stats: dict = field(default_factory=dict)


def _offset_of(stamp: str) -> str:
    match = re.search(r"([+-]\d{2}:?\d{2}|Z)$", str(stamp).strip())
    if not match:
        return ""
    off = match.group(1)
    if off == "Z":
        return "+00:00"
    return off if ":" in off else f"{off[:3]}:{off[3:]}"


def _series_names(df: pd.DataFrame, templates: list[tuple[list[str], str]]) -> pd.Series:
    """One series name per row, from the template that fits it (see the module docstring)."""
    names = pd.Series(index=df.index, dtype=object)
    todo = pd.Series(True, index=df.index)
    # most fields first; sorted() is stable, so the first listed wins a tie
    for fields, sep in sorted(templates, key=lambda t: -len(t[0])):
        fits = todo & ~df[fields].apply(lambda col: col.map(_absent)).any(axis=1)
        if fits.any():
            names[fits] = df.loc[fits, fields].apply(lambda row: series_name(row, sep), axis=1)
            todo &= ~fits
    if todo.any():
        fields, sep = templates[0]
        names[todo] = df.loc[todo, fields].apply(lambda row: series_name(row, sep), axis=1)
    return names


def unwrap(
    narrow: pd.DataFrame,
    hierarchy: str | list[str],
    ignore: Iterable[str] = (),
    *,
    timestamp_col: str = "timestamp",
    value_col: str = "value",
    separator: str = "_",
) -> tuple[pd.DataFrame, UnwrapState]:
    """Pivot a narrow frame into one column per series. Returns (wide frame, state for `wrap`).

    `ignore` names columns that hold one value in the whole file; they are removed from the series and restored by
    `wrap`. Raises UnwrapError for an ignored column that does not exist, is part of the hierarchy, or varies.

    `narrow` must be read as text (dtype=str, keep_default_na=False) so that tokens such as "N/A" survive; values are
    parsed as numbers here. The wide frame has `timestamp_col` first (timezone-naive UTC ISO strings, sorted) and one
    column per series, ordered by the hierarchy then the metric. Raises UnwrapError when a field or column is missing,
    when two different series would get the same name, or when a series has two values for one timestamp that differ
    in a column that is not part of the hierarchy.
    """
    templates = parse_hierarchies(hierarchy, separator)
    # every field of every template; the longest template's order first, so series sort by cell, then UE, then metric
    fields = list(dict.fromkeys(
        f for tpl_fields, _ in sorted(templates, key=lambda t: -len(t[0])) for f in tpl_fields))
    sep = templates[0][1]
    missing = [c for c in [*fields, timestamp_col, value_col] if c not in narrow.columns]
    if missing:
        raise UnwrapError(f"the dataset has no column {missing} (it has {list(narrow.columns)})")
    ignored_cols = list(dict.fromkeys(str(c).strip() for c in (ignore or []) if str(c).strip()))
    unknown = [c for c in ignored_cols if c not in narrow.columns]
    if unknown:
        raise UnwrapError(f"ignored column(s) {unknown} are not in the dataset (it has {list(narrow.columns)})")
    structural = [c for c in ignored_cols if c in (*fields, timestamp_col, value_col)]
    if structural:
        raise UnwrapError(f"{structural} cannot be ignored: they are the hierarchy, the timestamp or the value")
    extra_cols = [c for c in narrow.columns if c not in (*fields, timestamp_col, value_col, *ignored_cols)]

    df = narrow.copy()
    for column in df.columns:
        df[column] = df[column].astype(str)
    if df.empty:
        raise UnwrapError("the dataset has no rows")
    constants = {}
    for column in ignored_cols:
        distinct = df[column].unique()
        if len(distinct) > 1:
            raise UnwrapError(
                f"ignored column {column!r} holds {len(distinct)} different values ({list(distinct[:3])}...): only a "
                "column with one value in the whole dataset can be ignored")
        constants[column] = str(distinct[0])
    series_rows = df.copy()
    series_rows["_name"] = _series_names(df, templates)

    # timestamps -> naive UTC, and the offset to give them back in
    stamps = series_rows[timestamp_col]
    offsets = [o for o in (_offset_of(s) for s in stamps.head(1000)) if o]
    tz_offset = offsets[0] if offsets else ""
    parsed = pd.to_datetime(stamps, format="ISO8601", utc=True, errors="coerce")
    bad = int(parsed.isna().sum())
    if bad:
        raise UnwrapError(f"{bad} rows have a {timestamp_col!r} that is not a timestamp, e.g. {stamps[parsed.isna()].iloc[0]!r}")
    series_rows["_t"] = parsed.dt.tz_localize(None)

    values = pd.to_numeric(series_rows[value_col].replace({"": None}), errors="coerce")
    series_rows["_v"] = values
    non_numeric = int(values.isna().sum())

    # what identifies a series, and what else is true of it (source, unit, type, …)
    attrs: dict[str, dict[str, str]] = {}
    for name, group in series_rows.groupby("_name", sort=False):
        tokens = {}
        for column in [*fields, *extra_cols]:
            distinct = group[column].unique()
            if column in extra_cols and len(distinct) > 1:
                raise UnwrapError(
                    f"series {name!r} has {len(distinct)} different values in {column!r} ({list(distinct[:3])}): "
                    "that column is not constant per series, so it cannot be restored after wrapping")
            tokens[column] = str(distinct[0])
        attrs[name] = tokens
    keys: dict[str, tuple] = {}
    for name, tokens in attrs.items():
        key = tuple(tokens[f] for f in fields)
        if keys.setdefault(name, key) != key:
            raise UnwrapError(f"two different series are both named {name!r}")
    # two keys that differ only in an absent token ("N/A" vs "") would collide above, caught by the groupby merging
    # them; a merge of genuinely different tokens would have raised on the constant check for the hierarchy fields:
    for name, group in series_rows.groupby("_name", sort=False):
        for f_ in fields:
            if group[f_].nunique() > 1 and not all(_absent(x) for x in group[f_].unique()):
                raise UnwrapError(f"series {name!r} mixes {list(group[f_].unique()[:3])} in {f_!r}: the name does not "
                                  "tell them apart")

    duplicates = int(series_rows.duplicated(["_t", "_name"]).sum())
    deduped = series_rows.drop_duplicates(["_t", "_name"], keep="last")
    wide = deduped.pivot(index="_t", columns="_name", values="_v").sort_index()
    wide.columns.name = None

    def order(name: str):
        return tuple(_sortable(attrs[name][f]) for f in fields)

    wide = wide[sorted(wide.columns, key=order)]
    wide.insert(0, timestamp_col, wide.index.strftime(_TIMESTAMP_FORMAT))
    wide = wide.reset_index(drop=True)

    state = UnwrapState(
        fields=fields, sep=sep, templates=templates, timestamp_col=timestamp_col, value_col=value_col,
        columns=list(narrow.columns), series=attrs, constants=constants, tz_offset=tz_offset,
        stats={
            "hierarchies": [sep_.join(f"{{{x}}}" for x in f) for f, sep_ in templates],
            "narrow_rows": int(len(narrow)), "series": int(len(attrs)), "timestamps": int(len(wide)),
            "ignored_columns": ignored_cols,
            "duplicate_samples_dropped": duplicates, "non_numeric_values": non_numeric,
            "fill_rate": round(float(wide.drop(columns=timestamp_col).notna().values.mean()), 4),
        },
    )
    return wide, state


def _sortable(token: str):
    """Ids sort as numbers when they are numbers, absent levels first."""
    if _absent(token):
        return (0, 0, "")
    return (1, int(token), "") if str(token).isdigit() else (2, 0, str(token))


def wrap(
    wide: pd.DataFrame,
    state: UnwrapState,
    *,
    time_col: str | None = None,
    rename: Mapping[str, str] | None = None,
    time_unit: str = "datetime",
) -> pd.DataFrame:
    """Put processed series back into the narrow layout, with the ignored columns restored.

    `wide` has a time column and series columns. `time_col` names the time column (default: state.timestamp_col). The
    pipeline renames columns (snake_case) and turns the time into epoch seconds, so:
      rename     {column in `wide`: original series name}, to undo the renaming. Columns it does not know are
                 skipped (a column the pipeline added, such as `split`).
      time_unit  "datetime" (strings, naive UTC) or "epoch" (seconds since 1970, UTC).
    Every non-empty cell becomes one row, in the narrow frame's original columns and order, with each series'
    identifying and constant columns restored, and each ignored column given its one value.
    """
    time_col = time_col or state.timestamp_col
    if time_col not in wide.columns:
        raise UnwrapError(f"the frame has no time column {time_col!r} (it has {list(wide.columns)[:6]}…)")
    rename = dict(rename or {})
    series_cols = {}
    for column in wide.columns:
        if column == time_col:
            continue
        name = rename.get(column, column)
        if name in state.series:
            series_cols[column] = name
    if not series_cols:
        raise UnwrapError("none of the frame's columns is a series of the unwrapped dataset")

    if time_unit == "epoch":
        stamps = pd.to_datetime(pd.to_numeric(wide[time_col]), unit="s").dt.round("us")
    else:
        stamps = pd.to_datetime(wide[time_col], format="ISO8601")
    offset = state.tz_offset
    if offset:
        sign = -1 if offset.startswith("-") else 1
        hours, minutes = int(offset[1:3]), int(offset[4:6])
        stamps = stamps + sign * pd.Timedelta(hours=hours, minutes=minutes)
    rendered = stamps.dt.strftime(_TIMESTAMP_FORMAT) + offset

    long = (
        pd.DataFrame({"_ts": rendered, "_order": range(len(wide)), **{c: wide[c].to_numpy() for c in series_cols}})
        .melt(id_vars=["_ts", "_order"], value_vars=list(series_cols), var_name="_col", value_name=state.value_col)
        .dropna(subset=[state.value_col])
    )
    long["_name"] = long["_col"].map(series_cols)
    rank = {name: i for i, name in enumerate(state.series)}
    long["_rank"] = long["_name"].map(rank)
    long = long.sort_values(["_order", "_rank"], kind="stable")

    out = pd.DataFrame({state.timestamp_col: long["_ts"].to_numpy(), state.value_col: long[state.value_col].to_numpy()})
    for column in state.columns:
        if column in (state.timestamp_col, state.value_col):
            continue
        if column in state.constants:
            out[column] = state.constants[column]
        else:
            out[column] = long["_name"].map(lambda n, c=column: state.series[n][c]).to_numpy()
    return out[[c for c in state.columns]]
