"""
Runs WaveStitchPlus' minimal DataOps pipeline (clean → validate → profile →
remediate) over a distribution pulled from the Data Space, and hands its
artifacts to dali.datalake.upload_artifacts.

The pipeline itself is vendored into this plugins directory alongside dali —
minimal_dataops.py and the dataops / data_process_modules packages it imports,
copied verbatim from the WaveStitchPlus checkout (see plugins/VENDORED.md).
Airflow puts plugins_folder on sys.path, so they import like any other plugin
module, with no mount or PYTHONPATH juggling.

It is file-based where dali.datalake's tasks pass content around as strings, so
this module is the adapter between the two: it materialises the downloaded
content into a scratch directory, runs the pipeline against it, and reports
back where each artifact landed.

The import is deferred to task runtime rather than done at module import.
minimal_dataops pulls in pandas, great_expectations and pandera, and DAG files
are re-parsed constantly by the scheduler — paying that cost only when a task
actually runs keeps parsing cheap for every DAG in the dagbag.
"""

from __future__ import annotations

import json
import os
import re
import shutil
import sys
import tempfile
import time
import warnings
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from airflow.decorators import task
from airflow.sdk import get_current_context

from dali import pypots, unwrap as narrow, wavestitch
from dali.timestamps import normalise_timestamps
from dali.utils import fetch_series_layout, sanitize

# The Darts runner, vendored verbatim next to this package (see VENDORED.md). The
# engine="darts" path of dataops.imputation_runner subprocesses it, and its default
# location (dockers/tools/Darts_app) does not exist in the Airflow image.
_DARTS_RUNNER = Path(__file__).resolve().parents[1] / "Darts_app" / "run_imputation.py"

# Defaults mirroring WaveStitchPlus/config/dataops.yaml's `validation:` block,
# used when a run passes no validation config of its own. Kept here rather than
# read from that YAML because the file is a local-CLI convenience — a DAG run
# is configured through its params, not through a file on disk.
DEFAULT_VALIDATION_CONFIG: dict[str, Any] = {
    "mode": "auto",
    "expected_columns": [],
    "numeric_bounds": {},
    "missing_threshold": 0.0,
    "require_timestamp_unique": True,
    "require_timestamp_monotonic": True,
    "allow_step_index_timestamp": False,
    "outlier_q": 0.01,
    "outlier_mostly": 0.95,
}

# The pipeline's imputation handoff only *prepares* a bundle — it never imputes
# ("The pipeline never runs imputation", minimal_dataops._build_handoff). So a
# DAG run does two things the pipeline will not: it asks for the bundle, and it
# then runs the imputation itself over that bundle (see impute_prepared_bundle).
#
# Something does consume this now — the run results view in dataops-ui — which
# is why the bundle is on. Without it, remediation in time_series mode clips
# outliers and records gaps with status "deferred_to_imputation" without ever
# filling them, so a successful run showed no imputed values at all.
#
# `lib`/`method` name a built-in in dataops.imputation_runner, whose pandas
# engine reproduces Darts' MissingValuesFiller without importing darts (checked:
# identical output on every interpolation method). Pandas implements linear
# itself; nearest/cubic/quadratic/slinear/zero delegate to SciPy, which the image
# installs (airflow/base/requirements.txt). An image built without it records
# every such run as a failed imputation, so linear is the method to fall back on.
#
# Set `impute: false` to get the bundle without filling it.
#
# `runs` lists the imputations to perform over the one bundle, each
# {"lib", "method", "engine"?}; every run writes its own files and gets its own
# summary, and the first is the primary one the results view shows. Without
# `runs`, a top-level `lib`/`method` pair is a single run, as it always was.
# `engine` defaults to "pandas" (in-process, nothing to install); "darts"
# subprocesses the vendored Darts runner and is chosen automatically for
# darts/kalman, which the pandas engine cannot do. See imputation_runs() for the
# combinations that work.
#
# `convert_units` is whether the bundle is built with the 6G-schema unit
# conversions of preprocess_csv: ram_limit → ram_limit_mb, ram_usage (bytes) →
# ram_usage_mb, lat*/mean/min microseconds → <name>_ms, cpu_usage % → fraction,
# each by column name (a dataset without those names is unchanged). This DAG leaves the
# data as it came unless a run asks for it with `"convert_units": true`; the
# pipeline's own default is on, which is why it is set here, for every run, by
# imputation_config_for.
DEFAULT_IMPUTATION_CONFIG: dict[str, Any] = {
    "build_bundle": True,
    "impute": True,
    "convert_units": False,
    # The two Darts fills run in seconds, and the first one is the run the results view shows. Kalman fits a model
    # on the observed points in the vendored Darts runner (a subprocess, engine "darts"). The three WaveStitch+
    # methods run on the WaveStitch+ service (without WAVESTITCH_URL each is recorded as a failed run and the others
    # still happen). v1 comes first of them, because v2 and harpoon read its output, and runs in its fast mode (a
    # few epochs, a few sampling steps): runs started by the s3-asset-monitor and the upload form send no imputation
    # setting, so every new table gets these. The full v1 training takes minutes to hours on CPU: ask for it with
    # {"lib": "wavestitchplus", "method": "v1"} in the run's `imputation`. v2 and harpoon work from v1's result.
    "runs": [
        {"lib": "darts", "method": "linear"},
        {"lib": "darts", "method": "nearest"},
        {"lib": "darts", "method": "kalman"},
        {"lib": "wavestitchplus", "method": "v1", "params": {"fast": True}},
        {"lib": "wavestitchplus", "method": "v2"},
        {"lib": "wavestitchplus", "method": "harpoon"},
    ],
}


def imputation_config_for(params: dict) -> dict:
    """The run's imputation config: its `imputation` param, or the defaults, with `convert_units` off unless asked.

    A run that passes its own `imputation` replaces DEFAULT_IMPUTATION_CONFIG (it is not merged, because `runs` in the
    defaults would then win over a `lib`/`method` pair), so the one default that must hold for every run is applied
    here, to a copy: the caller's dict is not changed.
    """
    config = dict(params.get("imputation") or DEFAULT_IMPUTATION_CONFIG)
    config.setdefault("convert_units", False)
    return config


# Timeline handling, passed to run_pipeline as timeline_config. Left empty the
# pipeline drops rows that go backwards in time (disorder_policy "drop"); here
# they are sorted into place instead, which is what this DAG did before the
# pipeline gained the option. Duplicate timestamps still keep the last row.
# Pass `timeline` in the DAG params to override, e.g.
# {"sweep_aware": true} for parameter-sweep files whose backward jumps mark new
# acquisition runs rather than misplaced rows.
DEFAULT_TIMELINE_CONFIG: dict[str, Any] = {
    "disorder_policy": "sort",
}


# Column names that name a timestamp, most specific first. The pipeline's own
# detection (dataops.profiling.analyze_time_series) is purely content-based —
# it walks the columns in order and never looks at their names — so it returns
# the first *plausible* column rather than the intended one, and discards a
# monotonic step index entirely unless allow_step_index_timestamp is set.
# Naming the column up front short-circuits that with its "Configured Timestamp
# Column" path.
#
# Ordered deliberately: an explicit "timestamp" beats a bare "time", which beats
# a date-only column, which beats record-keeping columns like "created_at" —
# those describe when the *row* was written, not when the measurement happened.
_TIMESTAMP_NAMES = (
    "timestamp", "time_stamp", "event_timestamp", "event_time", "datetime",
    "date_time", "time", "ts", "epoch", "epoch_time", "unix_time",
    "measured_at", "observed_at", "recorded_at", "collected_at", "logged_at",
    "date", "day", "created_at", "updated_at", "inserted_at",
)

# Weaker, positional fallbacks tried after the exact names above.
_TIMESTAMP_SUFFIXES = ("_timestamp", "_datetime", "_time", "_date", "_at")

# Rows read to sanity-check a candidate. Enough to tell a timestamp from an
# unrelated column with a timestamp-ish name, cheap enough that reading the file
# a second time (run_pipeline reads it in full itself) costs nothing.
_GUESS_SAMPLE_ROWS = 500


def _normalise(name: object) -> str:
    """Column name reduced for matching, the same way dataops.cleaning.snake_case
    reduces it — so a guess made against the raw header still matches the name
    the pipeline will use after cleaning."""
    text = re.sub(r"[^0-9a-zA-Z]+", "_", str(name).strip().lower())
    return re.sub(r"_+", "_", text).strip("_")


def _is_timestamp_like(series) -> bool:
    """Whether a column could actually carry timestamps.

    Guards the name match: a column called "date_of_birth" or a "time" column
    holding elapsed seconds would otherwise be forced on the pipeline as the
    timeline, which is worse than letting content-based detection choose. The
    three accepted shapes mirror the ones analyze_time_series recognises —
    datetime strings, Unix-epoch numerics, and a monotonic step index.
    """
    import pandas as pd

    nonnull = series.dropna()
    if nonnull.empty:
        return False
    if pd.api.types.is_datetime64_any_dtype(series):
        return True
    if pd.api.types.is_numeric_dtype(series):
        if 1e9 < float(nonnull.mean()) < 3e9:          # Unix epoch seconds
            return True
        return bool(series.is_monotonic_increasing and series.nunique() > len(series) * 0.9)
    with warnings.catch_warnings():
        # Probing a column with no known format is expected to be noisy.
        warnings.simplefilter("ignore")
        parsed = pd.to_datetime(nonnull, errors="coerce")
    return bool(parsed.notna().mean() >= 0.9)


def guess_timestamp_column(input_csv: Path) -> str | None:
    """Guess the timestamp column from the header, when a run did not name one.

    Returns the column's *original* header name (run_pipeline snake_cases it
    itself), or None to leave the pipeline's own detection in charge — which is
    what happens when no name matches, or when the best-named candidate does not
    hold anything timestamp-like. Never returning a bad guess matters more than
    always returning one: a wrong column here overrides detection that would
    otherwise have got it right.
    """
    import pandas as pd

    try:
        sample = pd.read_csv(input_csv, nrows=_GUESS_SAMPLE_ROWS)
    except Exception as exc:  # noqa: BLE001 - a guess is best-effort; the
        # pipeline reads the file itself and will report a real parse failure.
        print(f"[dataops] could not sample the input to guess a timestamp column: {exc}")
        return None
    return guess_timestamp_in_frame(sample)


def guess_timestamp_in_frame(sample) -> str | None:
    """:func:`guess_timestamp_column` over a frame already read, by the same rules."""
    normalised = [(_normalise(col), col) for col in sample.columns]
    by_name: dict[str, str] = {}
    for norm, col in normalised:
        # First column wins a duplicate normalised name, so the ranking below
        # stays stable rather than depending on which duplicate came last.
        by_name.setdefault(norm, col)

    ranked = [by_name[n] for n in _TIMESTAMP_NAMES if n in by_name]
    ranked += [
        col for norm, col in normalised
        if col not in ranked and norm.endswith(_TIMESTAMP_SUFFIXES)
    ]
    if not ranked:
        print(f"[dataops] no timestamp-like column name among {list(sample.columns)}")
        return None

    for col in ranked:
        if _is_timestamp_like(sample[col]):
            print(f"[dataops] guessed timestamp column {col!r} from the column names")
            return col
        print(f"[dataops] {col!r} is named like a timestamp but does not hold one — skipping")

    print("[dataops] no named candidate held timestamps — leaving detection to the pipeline")
    return None


def _tolerate_mixed_time_formats() -> None:
    """Let the vendored pipeline read a time column that mixes whole and fractional seconds.

    DataFrame.to_csv formats a datetime column in chunks of 100000 // (number of columns) rows, each on its own: a
    chunk with a fractional second is written "2026-07-19 10:03:50.000", one without "2026-09-19 01:02:54". The
    pipeline writes its remediated frame that way and then builds the imputation bundle from the file, and
    dataops._preprocess_impl.coerce_time_column infers one format from the first value and drops the rest ("Could not
    parse time column: only 0/6488 parse as numeric and 826/6488 parse as datetime"). A dataset with a few sub-second
    timestamps and enough rows or columns to span two chunks hits it.

    The fix belongs in WaveStitchPlus; until it is there, the plugin copy is not edited (see plugins/VENDORED.md) and
    the function is wrapped here instead: when it raises, the column is read once more with format="mixed", each value
    on its own. Idempotent, and a no-op for pandas older than 2.0, which has no such format.
    """
    import dataops._preprocess_impl as impl
    import dataops.transform as transform

    original = impl.coerce_time_column
    if getattr(original, "_dali_mixed_formats", False):
        return

    def coerce_time_column(series, min_valid_frac: float = 0.9):
        try:
            return original(series, min_valid_frac)
        except ValueError as first:
            import warnings

            import pandas as pd
            try:
                with warnings.catch_warnings():
                    warnings.simplefilter("ignore")
                    parsed = pd.to_datetime(series, errors="coerce", utc=False, format="mixed")
            except (TypeError, ValueError):
                raise first
            if len(series) == 0 or parsed.notna().sum() < min_valid_frac * len(series):
                raise first
            return (parsed - pd.Timestamp("1970-01-01")).dt.total_seconds(), "datetime"

    coerce_time_column._dali_mixed_formats = True
    impl.coerce_time_column = coerce_time_column
    transform.coerce_time_column = coerce_time_column       # imported by name, so patched where it is used too


def _load_run_pipeline():
    """Import run_pipeline from the vendored pipeline module.

    Deferred to task runtime on purpose — see the module docstring."""
    try:
        from minimal_dataops import run_pipeline
        _tolerate_mixed_time_formats()
    except ImportError as exc:
        raise RuntimeError(
            f"[dataops] could not import the vendored WaveStitchPlus pipeline: "
            f"{exc}. plugins/minimal_dataops.py and the dataops / "
            f"data_process_modules packages next to it are required, and "
            f"pandera must be installed in the image "
            f"(see plugins/VENDORED.md and airflow/base/requirements.txt)."
        ) from exc
    return run_pipeline


def _bundle_facts(prepared_dir: str) -> dict:
    """What the bundle's own meta.json says about how it was built.

    `regularized_rows == original_rows` is the tell that regularization was
    skipped: preprocess_csv drops it when a uniform grid would be more than
    `sparse_skip_pct` empty (dataops._preprocess_impl.regularize), and a bundle
    that was never regularized has no inserted gaps to fill. Without this the
    run reports "0 cells filled" and looks broken when it is behaving exactly as
    configured.
    """
    try:
        meta = json.loads((Path(prepared_dir) / "meta.json").read_text(encoding="utf-8"))
    except (OSError, ValueError) as exc:
        print(f"[dataops] could not read the bundle's meta.json: {exc}")
        return {}
    original = meta.get("original_rows")
    regularized = meta.get("regularized_rows")
    return {
        "original_rows":    original,
        "regularized_rows": regularized,
        "base_dt":          meta.get("base_dt"),
        "sparse_skip_pct":  meta.get("sparse_skip_pct"),
        "regularized":      None if original is None or regularized is None
                            else regularized > original,
    }


def _build_final_dataset(prepared_dir: str, bundle_result: dict, *, lib: str, method: str) -> dict:
    """Stitch the imputed train and test splits into one gap-free timeline.

    The splits are how the bundle is *modelled*, not how anyone wants to read
    the result: two files, in train/test order, carrying the engineered
    conditioning features the imputer needed. dataops.imputation_runner's
    build_final_dataset — "the analysis-ready endpoint of the pipeline" — sorts
    them back into one series on `time`, keeps a `split` label so the boundary
    stays visible, and drops the modelling features. Without this the run's most
    useful output is the one it never produced.

    Never raises: the splits are already written and uploaded by the time this
    runs, so a stitching failure costs the convenience frame, not the run.
    """
    try:
        from dataops.imputation_runner import build_final_dataset

        prepared = Path(prepared_dir)
        stem = prepared.name.removesuffix("_regularized")
        output_path = prepared.parent / f"{stem}_imputed_{lib}_{method}_final.csv"
        final = build_final_dataset(
            prepared,
            method=method,
            lib=lib,
            engine="pandas",
            output_path=output_path,
            # The splits this run just wrote, so it stitches those rather than
            # re-imputing to find them.
            bundle_result=bundle_result,
        )
    except Exception as exc:  # noqa: BLE001 - recorded, never fails the run
        print(f"[dataops] final dataset not built: {exc.__class__.__name__}: {exc}")
        return {"error": f"{exc.__class__.__name__}: {exc}"}

    print(f"[dataops] final dataset: {final.get('rows')} rows, "
          f"fill rate {final.get('fill_rate')} -> {final.get('path')}")
    return final


def imputation_runs(imputation_config: dict) -> list[dict]:
    """The imputations a run should perform, as [{lib, method, engine}, …].

    `runs` wins when present: a list of {"lib", "method", "engine"?} objects or
    "lib/method" strings. Otherwise a top-level `lib`/`method` pair is one run.
    A (lib, method) pair appears once — each writes
    `<lib>_<method>_<split>_imputed.csv` into the bundle's output directory, so a
    second engine for the same pair would overwrite the first.

    The engine is "pandas" unless asked otherwise, except darts/kalman, which
    only the Darts runner can do, lib "wavestitchplus", which only the
    WaveStitch+ service can run (engine "wavestitch"), and lib "pypots", which
    only the PyPOTS service can run (engine "pypots").

    Pairs that work: engine "pandas" with lib "darts" (auto, linear, nearest,
    quadratic, cubic, slinear, zero — everything but linear and auto needs
    SciPy) or lib "imputegap" (interpolation, mean, mean_by_series, min, zero);
    engine "darts", lib "darts" only (those plus kalman); engine "wavestitch",
    lib "wavestitchplus" (v1, v2, harpoon; "full" and "anchored" are accepted for
    v1 and v2); engine "pypots", lib "pypots" (saits, transformer, timesnet, brits,
    mrnn, gpvae, csdi, usgan; `params` are epochs, batch_size, window, stride).
    Anything else is recorded as an error for that run and the other
    runs still happen.

    A wavestitchplus run may carry `device` ("cpu", "gpu" or "auto"), `params`
    (options of its runner) and, for v2 and harpoon, `v1_params` (for the v1 run
    they need). v2 and harpoon read what v1 produced on the same bundle, so v1 is
    moved to just before the first of them when it is listed after one. When it
    is not listed at all the service runs it first.
    """
    raw = (imputation_config or {}).get("runs")
    if not raw:
        raw = [{"lib": imputation_config.get("lib"), "method": imputation_config.get("method")}]
    runs: list[dict] = []
    seen: set[tuple[str, str]] = set()
    for item in raw:
        if isinstance(item, str):
            lib, _, method = item.partition("/")
            item = {"lib": lib, "method": method}
        lib = item.get("lib") or "darts"
        method = item.get("method") or "linear"
        if lib == wavestitch.LIB:
            method = wavestitch.normalize_method(method)
        if (lib, method) in seen:
            continue
        seen.add((lib, method))
        if lib == wavestitch.LIB:
            default_engine = wavestitch.ENGINE
        elif lib == pypots.LIB:
            default_engine = pypots.ENGINE
        else:
            default_engine = "darts" if (lib, method) == ("darts", "kalman") else "pandas"
        run = {"lib": lib, "method": method, "engine": item.get("engine") or default_engine}
        for key in ("device", "params", "v1_params"):   # only the WaveStitch+ and PyPOTS services take these
            if item.get(key) is not None:
                run[key] = item[key]
        runs.append(run)
    return _v1_before_its_dependents(runs)


def _v1_before_its_dependents(runs: list[dict]) -> list[dict]:
    """WaveStitch+ v2 and harpoon read what v1 produced on the bundle, so v1 goes just before the first of them.

    Nothing else moves: the first run stays the primary one unless v1 is moved in front of it.
    """
    def is_wsp(run: dict, methods: tuple[str, ...]) -> bool:
        return run["lib"] == wavestitch.LIB and run["method"] in methods

    v1 = next((i for i, r in enumerate(runs) if is_wsp(r, ("v1",))), None)
    first_dependent = next((i for i, r in enumerate(runs) if is_wsp(r, wavestitch.NEEDS_V1)), None)
    if v1 is None or first_dependent is None or v1 < first_dependent:
        return runs
    moved = runs.pop(v1)
    runs.insert(first_dependent, moved)
    return runs


# A column whose spread is below this share of its largest absolute value does not vary (see _column_spread).
_NO_SPREAD = 1e-9


def _column_spread(truth, train, column: str) -> float:
    """Standard deviation of a column's true values: the divisor of its nrmse.

    Taken from test_gt.csv. On a sparse dataset that file may hold a single observed value in a column (the
    spread of one value is 0, so the column could never be scored); then the observed values of train.csv are
    pooled in. Every run on the bundle uses the same divisor, so the ranking is unaffected.

    A column that does not vary has no spread to divide by, and the answer is 0.0 (the column is left out of the
    average). That includes a column that varies only by rounding noise: a stationary sensor's latitude, written as
    37.0245925641345 and 37.0245925641346, has a spread of about 4e-15, and any error divided by that is 1e12. A spread
    below one part in a billion of the column's largest value counts as none.
    """
    import numpy as np
    import pandas as pd

    def observed(frame):
        if frame is None or column not in frame.columns:
            return np.array([])
        values = pd.to_numeric(frame[column], errors="coerce").to_numpy(dtype=float)
        return values[~np.isnan(values)]

    def spread_of(values) -> float:
        if len(values) < 2:
            return 0.0
        spread = float(np.std(values))
        return spread if spread > _NO_SPREAD * float(np.max(np.abs(values))) else 0.0

    own = observed(truth)
    if spread_of(own) > 0:
        return spread_of(own)
    return spread_of(np.concatenate([own, observed(train)]))


def _fill_errors(prepared_dir: str, summary: dict) -> dict:
    """What a run did to the cells hidden to score it: the arrays `_score_mask` works from.

    The bundle blanks about 15% of the test split in short blocks and keeps the true values in test_gt.csv.
    `hidden` counts those cells, and `ok` marks the ones this run filled: a method that leaves a hole (darts, on a
    column with no neighbour in the split) has fewer. Returns {"cols", "gt", "imp", "ok", "hidden"}.
    """
    import numpy as np
    import pandas as pd
    from dataops.imputation_runner import _load_meta

    prepared = Path(prepared_dir)
    cols = _load_meta(prepared)["target_cols"]
    given = pd.read_csv(prepared / "test_input.csv")[cols].to_numpy(dtype=float)
    gt = pd.read_csv(prepared / "test_gt.csv")[cols].to_numpy(dtype=float)
    hidden = np.isnan(given) & ~np.isnan(gt)
    path = ((summary.get("files") or {}).get("test") or {}).get("path")
    imp = pd.read_csv(path)[cols].to_numpy(dtype=float) if path else np.full_like(gt, np.nan)
    return {"cols": cols, "gt": gt, "imp": imp, "ok": hidden & ~np.isnan(imp), "hidden": int(hidden.sum())}


def _score_mask(prepared_dir: str, errors: dict, mask) -> dict:
    """MAE, RMSE and nrmse of one run on the cells in `mask`, per column and overall.

    nrmse is RMSE divided by the spread (standard deviation) of that column's true values (see _column_spread),
    then averaged over the columns with equal weight. 0 is a perfect fill; about 1 is no better than guessing the
    column's mean. A column whose truth has no spread is left out of the average. Raw MAE and RMSE stay beside it
    in per-column units.
    """
    import numpy as np
    import pandas as pd

    truth = pd.read_csv(Path(prepared_dir) / "test_gt.csv")
    train_path = Path(prepared_dir) / "train.csv"
    train = pd.read_csv(train_path) if train_path.exists() else None
    per_column: dict[str, dict] = {}
    for j, column in enumerate(errors["cols"]):
        rows = mask[:, j]
        if not rows.any():
            continue
        diff = errors["imp"][rows, j] - errors["gt"][rows, j]
        rmse = float(np.sqrt(np.mean(diff ** 2)))
        spread = _column_spread(truth, train, column)
        per_column[column] = {"eval_cells": int(rows.sum()), "MAE": float(np.mean(np.abs(diff))), "RMSE": rmse,
                              "nrmse": rmse / spread if spread > 0 else None}
        if spread <= 0:
            per_column[column]["constant"] = True            # no spread: left out of the average, RMSE and MAE say how far off
    scored = [c["nrmse"] for c in per_column.values() if c["nrmse"] is not None]
    return {"metric": "nrmse", "nrmse": float(np.mean(scored)) if scored else None,
            "eval_cells": int(mask.sum()), "per_column": per_column}


def _score_run(prepared_dir: str, summary: dict) -> dict:
    """How close one run's fills are to the truth hidden to score them, on the cells this run filled.

    `hidden_cells` is how many cells were hidden and `coverage` the share this run filled. Runs that fill
    different cells are not comparable on these figures; impute_prepared_bundle re-scores them all on the cells
    every run filled (_score_on_common_cells) before ranking.

    Never raises: a run that cannot be scored still counts as imputed.
    """
    scale = {"metric": "nrmse"}
    try:
        errors = _fill_errors(prepared_dir, summary)
        if not errors["hidden"]:
            return {**scale, "nrmse": None, "eval_cells": 0,
                    "reason": "the bundle holds no hidden cells to score against"}
        accuracy = _score_mask(prepared_dir, errors, errors["ok"])
        accuracy["hidden_cells"] = errors["hidden"]
        accuracy["coverage"] = round(accuracy["eval_cells"] / errors["hidden"], 4)
        return accuracy
    except Exception as exc:  # noqa: BLE001 - scoring is a bonus, never a failure
        print(f"[dataops] could not score {summary.get('lib')}/{summary.get('method')}: "
              f"{exc.__class__.__name__}: {exc}")
        return {**scale, "nrmse": None, "error": f"{exc.__class__.__name__}: {exc}"}


def _score_on_common_cells(prepared_dir: str, summaries: list[dict]) -> None:
    """Re-score the runs on the hidden cells every one of them filled, so their scores can be compared.

    A run that leaves holes is scored by `_score_run` on the easier cells only, which can put it ahead of a run that
    filled them all. Here every imputed run is scored on the intersection. Each run's `accuracy` then carries the
    common figure as `nrmse` and `eval_cells` (what the ranking reads), `scored_on: "common"`, and its own figure as
    `own_nrmse` / `own_cells`, with `hidden_cells` and `coverage` kept. A single run, or no cell filled by all,
    keeps its own figure. Never raises.
    """
    try:
        runs = [r for r in summaries if r.get("status") == "imputed" and (r.get("accuracy") or {}).get("eval_cells")]
        if len(runs) < 2:
            return
        errors = {id(r): _fill_errors(prepared_dir, r) for r in runs}
        common = None
        for r in runs:
            common = errors[id(r)]["ok"] if common is None else common & errors[id(r)]["ok"]
        if common is None or not common.any():
            for r in runs:
                r["accuracy"]["scored_on"] = "own"
                r["accuracy"]["note"] = "no hidden cell was filled by every run, so scores are not comparable"
            return
        for r in runs:
            own = r["accuracy"]
            scored = _score_mask(prepared_dir, errors[id(r)], common)
            r["accuracy"] = {**scored, "scored_on": "common", "own_nrmse": own.get("nrmse"),
                             "own_cells": own.get("eval_cells"), "hidden_cells": own.get("hidden_cells"),
                             "coverage": own.get("coverage")}
    except Exception as exc:  # noqa: BLE001 - scoring is a bonus, never a failure
        print(f"[dataops] could not score the runs on common cells: {exc.__class__.__name__}: {exc}")


def _rank_runs(runs: list[dict]) -> list[dict]:
    """Order the runs that could be scored by nrmse, best first, and stamp each
    with its `rank`. Runs without a score (failed, nothing to score against) are
    not ranked."""
    scored = sorted(
        (r for r in runs if (r.get("accuracy") or {}).get("nrmse") is not None),
        key=lambda r: r["accuracy"]["nrmse"],
    )
    ranking = []
    for rank, run in enumerate(scored, 1):
        run["rank"] = rank
        ranking.append({"lib": run["lib"], "method": run["method"], "rank": rank,
                        "nrmse": run["accuracy"]["nrmse"],
                        "eval_cells": run["accuracy"].get("eval_cells")})
    return ranking


def _impute_one(prepared_dir: str, bundle: dict, *, lib: str, method: str, engine: str,
                device: str | None = None, params: dict | None = None, v1_params: dict | None = None) -> dict:
    """One imputation over the bundle. Never raises: a failure is a summary with
    status "error", so the runs after it still happen.

    `device`, `params` and `v1_params` are for the WaveStitch+ service (engine "wavestitch"); the PyPOTS service
    (engine "pypots") takes `params` only."""
    base = {"lib": lib, "method": method, "engine": engine, "files": {}, "filled": 0}
    try:
        if engine == wavestitch.ENGINE:
            if lib != wavestitch.LIB:
                raise ValueError(f"engine {engine!r} only applies to lib {wavestitch.LIB!r}")
            summary = wavestitch.impute(prepared_dir, method=method, device=device, params=params,
                                        v1_params=v1_params)
        elif engine == pypots.ENGINE:
            if lib != pypots.LIB:
                raise ValueError(f"engine {engine!r} only applies to lib {pypots.LIB!r}")
            summary = pypots.impute(prepared_dir, method=method, params=params)
        else:
            from dataops.imputation_runner import impute_bundle

            extra = {"python_exe": sys.executable, "runner_path": _DARTS_RUNNER} if engine == "darts" else {}
            summary = impute_bundle(prepared_dir, method=method, lib=lib, engine=engine, **extra)
    except Exception as exc:  # noqa: BLE001 - recorded, never fails the run
        print(f"[dataops] imputation {lib}/{method} ({engine}) failed: {exc.__class__.__name__}: {exc}")
        return {**base, "status": "error", "bundle": bundle,
                "reason": f"{exc.__class__.__name__}: {exc}",
                "error": f"{exc.__class__.__name__}: {exc}"}

    files = summary.get("files") or {}
    filled = sum(f.get("filled", 0) for f in files.values())
    missing = sum(f.get("nan_before", 0) for f in files.values())
    final = _build_final_dataset(prepared_dir, summary, lib=lib, method=method)
    summary = {**summary, "bundle": bundle, "filled": filled,
               "missing_before": missing, "final": final}
    summary["accuracy"] = _score_run(prepared_dir, summary)
    nrmse = summary["accuracy"].get("nrmse")

    if filled:
        print(f"[dataops] imputed with {lib}/{method} ({engine}): {filled} cells filled "
              f"across {len(files)} split(s)"
              + (f", nrmse {nrmse:.3f} on {summary['accuracy'].get('eval_cells')} hidden cells"
                 if nrmse is not None else ""))
        return {**summary, "status": "imputed",
                "reason": f"{filled} missing values filled with {lib}/{method}"}

    # Ran, found nothing. Almost always the sparse-grid skip: a series whose
    # gaps dwarf its cadence would regularize into a frame that is mostly holes,
    # so preprocess_csv keeps the observed rows instead — and observed rows have
    # no holes to fill.
    if bundle.get("regularized") is False:
        reason = (
            "the timeline was not regularized, so the bundle holds only observed "
            f"rows and has no missing values to fill — a uniform grid at "
            f"{bundle.get('base_dt')}s would have been more than "
            f"{bundle.get('sparse_skip_pct')}% empty"
        )
    else:
        reason = "the bundle held no missing values"
    print(f"[dataops] nothing imputed with {lib}/{method}: {reason}")
    return {**summary, "status": "nothing_to_fill", "reason": reason}


def _timed_impute_one(prepared_dir: str, bundle: dict, **run) -> dict:
    """:func:`_impute_one`, with how long it took: `timing` {started_at, finished_at, seconds} on the run's summary.

    The seconds are the wall clock of the whole run, including the wait on a service that does the work (WaveStitch+,
    PyPOTS) and the scoring and stitching that follow it. Runs are made one after the other, so they do not overlap.
    A run that failed carries its timing too.
    """
    started = datetime.now(timezone.utc)
    begin = time.monotonic()
    summary = _impute_one(prepared_dir, bundle, **run)
    seconds = round(time.monotonic() - begin, 3)
    summary["timing"] = {"started_at": started.isoformat(), "finished_at": datetime.now(timezone.utc).isoformat(),
                         "seconds": seconds}
    print(f"[dataops] imputation {run.get('lib')}/{run.get('method')}: {summary.get('status')} in {seconds:.1f} s")
    return summary


def impute_prepared_bundle(report: dict, imputation_config: dict) -> dict:
    """Run the configured imputations over the handoff's regularized bundle, in-process.

    The pipeline deliberately stops at the handoff: it regularizes the timeline
    into a prepared-dir bundle and records which (app, method) an external
    orchestrator *would* invoke, but fills nothing ("The pipeline never runs
    imputation" — minimal_dataops._build_handoff). This runs that step, so a DAG
    run ends with imputed values rather than an invoke_hint.

    Each run (see imputation_runs) goes through dataops.imputation_runner, by
    default its "pandas" engine, which reproduces Darts' MissingValuesFiller for
    the interpolation family without importing darts. Engine "darts" runs the
    vendored Darts runner in a subprocess instead.

    Returns the **primary** (first) run's summary — the shape the results view
    already reads — plus `runs`, the summaries of all of them, `runs_total` /
    `runs_imputed`, and `ranking`: the runs ordered by how close their fills came to
    the values hidden to score them (see _score_run), best first. Each run carries
    its own `accuracy`, `rank` and `timing` (see _timed_impute_one); `runs_seconds` is their total. The first run stays the primary one whatever the
    ranking says. One run failing does not stop the others.

    Always returns a summary carrying a `status` and a `reason`, including when
    nothing was imputed. Returning None for that case — which this used to do —
    left report["imputation"] absent, so a run that correctly had nothing to fill
    was indistinguishable from one where the step never ran, in the logs and in
    the results view alike. Never raises: a failure is recorded and the run still
    uploads its artifacts, the same way a validation failure is.

    status is one of:
      disabled        the run was configured not to impute
      no_bundle       no bundle to work on — handoff.reason says why
      error           the bundle was built but the runner failed
      nothing_to_fill it ran and found no missing values (see `bundle`)
      imputed         values were filled
    """
    runs = imputation_runs(imputation_config)
    first = runs[0]
    base = {"lib": first["lib"], "method": first["method"], "engine": first["engine"],
            "files": {}, "filled": 0}

    if not imputation_config.get("impute", True):
        return {**base, "status": "disabled",
                "reason": "imputation was switched off for this run"}

    handoff = (report or {}).get("handoff") or {}
    prepared_dir = handoff.get("prepared_dir")
    if not handoff.get("bundle_written") or not prepared_dir:
        # needs_ts_imputation false (no gaps), not a time series, or the bundle
        # failed to build — handoff.reason / handoff.bundle_error says which.
        reason = handoff.get("bundle_error") or {
            "no_time_gaps":    "the timeline has no gaps to fill",
            "not_time_series": "the dataset was not validated as a time series",
        }.get(handoff.get("reason"), f"no bundle was written ({handoff.get('reason')!r})")
        print(f"[dataops] nothing to impute: {reason}")
        return {**base, "status": "no_bundle", "reason": reason,
                "handoff_reason": handoff.get("reason"),
                "bundle_error": handoff.get("bundle_error")}

    bundle = _bundle_facts(prepared_dir)
    summaries = [_timed_impute_one(prepared_dir, bundle, **run) for run in runs]
    _score_on_common_cells(prepared_dir, summaries)
    ranking = _rank_runs(summaries)
    return {**summaries[0], "runs": summaries, "runs_total": len(summaries),
            "runs_imputed": sum(1 for r in summaries if r.get("status") == "imputed"),
            "runs_seconds": round(sum((r.get("timing") or {}).get("seconds") or 0.0 for r in summaries), 3),
            "ranking": ranking}


def resolve_series_layout(params: dict) -> dict | None:
    """The narrow-dataset layout for this run: {"hierarchy", "separator", "ignore_columns", "value_col", "source"}, or None.

    It comes only from the targeted distribution's catalogue metadata (dali:seriesHierarchy, dali:seriesSeparator,
    dali:ignoredColumn, dali:valueColumn, see dali.utils.series_layout_of), so a dataset carries its own layout and
    every run on it unwraps the same way. The run's params never name one: a stray `hierarchy` in the run's conf is
    ignored. None when the distribution declares no hierarchy, or the catalogue cannot be read.
    """
    if not params.get("dataset_id"):
        return None
    found = fetch_series_layout(params["dataset_id"], params.get("catalogue_id") or "", params.get("asset_id") or "")
    return {**found, "source": "piveau"} if found else None


def _unwrap_input(input_csv: Path, workdir: Path, stem: str, layout: dict, timestamp_col: str | None):
    """Turn a narrow input into one column per series, when the run names a `hierarchy`.

    Returns (path of the wide CSV, UnwrapState, timestamp column). The wide file is what the pipeline reads; the
    narrow file stays the run's `input_csv`, so the raw -> remediated lineage still starts from what arrived.
    Raises dali.unwrap.UnwrapError when the hierarchy does not fit the data.
    """
    import pandas as pd

    narrow_df = pd.read_csv(input_csv, dtype=str, keep_default_na=False)
    timestamp_col = layout.get("timestamp_col") or timestamp_col or guess_timestamp_column(input_csv) or "timestamp"
    if layout.get("measure_cols"):
        # a panel: one row per entity and time, a column per measure (see dali.unwrap.unwrap_panel)
        wide, state = narrow.unwrap_panel(
            narrow_df,
            layout["hierarchy"],
            layout["measure_cols"],
            timestamp_col=timestamp_col,
            separator=layout.get("separator") or "_",
            carry=layout.get("carry_columns") or (),
        )
    else:
        wide, state = narrow.unwrap(
            narrow_df,
            layout["hierarchy"],
            layout["ignore_columns"],
            timestamp_col=timestamp_col,
            value_col=layout["value_col"],
            separator=layout.get("separator") or "_",
        )
    wide_csv = workdir / f"{stem}_unwrapped.csv"
    wide.to_csv(wide_csv, index=False)
    print(f"[dataops] unwrapped {state.stats['narrow_rows']} rows into {state.stats['series']} series over "
          f"{state.stats['timestamps']} timestamps (fill {state.stats['fill_rate']:.1%}); "
          f"ignored columns {state.stats['ignored_columns']} held back")
    return wide_csv, state, timestamp_col, list(wide.columns)


_WRAPPED_FRAME = re.compile(r"^(soft_cleaned_csv|output_csv|imputed_.+_(?:train|test|final)_csv)$")


def _wrap_artifacts(produced: dict[str, str], state, wide_columns: list[str], report: dict, prepared_dir: str | None,
                    workdir: Path) -> tuple[dict[str, str], dict[str, int], dict[str, str]]:
    """Store a narrow dataset's results in the narrow layout: every frame the pipeline produced, wrapped back.

    `produced` maps artifact names to paths. The frames the pipeline works on are wide, one column per series: the
    soft-cleaned and remediated frames, and each imputation's splits and full timeline. Each is wrapped back under its
    own name, so the artifacts a run leaves are all in the layout of the file that came in, and none holds a column
    that the unwrapping generated. The wide input (`unwrapped`) is not kept.

    The pipeline snake_cases column names (dataops.cleaning) and writes the imputed time as epoch seconds, so the
    columns are mapped back through the same renaming it applied, and the time column is read in its own form. The
    ignored columns are restored with their one value. Never raises: a frame that cannot be wrapped is left out, never
    stored wide, and the reason is returned.

    Returns (the new `produced`, {artifact: narrow rows}, {artifact: why it was left out}).
    """
    import pandas as pd
    from dataops.cleaning import build_column_mapping, snake_case

    inverse = {cleaned: original for original, cleaned in build_column_mapping(wide_columns).items()}
    meta = {}
    if prepared_dir:
        try:
            meta = json.loads((Path(prepared_dir) / "meta.json").read_text(encoding="utf-8"))
        except (OSError, ValueError):
            meta = {}
    pipeline_time = (report.get("validation") or {}).get("timestamp_column") or snake_case(state.timestamp_col)

    original = None
    if state.panel:
        # a panel is stored in its own layout, and the rows that were in the file get their other columns back
        original = pd.read_csv(produced["input_csv"], dtype=str, keep_default_na=False)

    out: dict[str, str] = {}
    rows: dict[str, int] = {}
    left_out: dict[str, str] = {}
    for name, path in produced.items():
        if name == "unwrapped":
            continue                                    # the wide input is an intermediate, never an artifact
        if not _WRAPPED_FRAME.match(name):
            out[name] = path
            continue
        try:
            frame = pd.read_csv(path)
            imputed = name.startswith("imputed_")
            time_col = (meta.get("time_col") if imputed else None) or pipeline_time
            if time_col not in frame.columns:
                time_col = next((c for c in ("timestamp", "time") if c in frame.columns), frame.columns[0])
            if pd.api.types.is_numeric_dtype(frame[time_col]):
                frame[time_col] = frame[time_col] * float(meta.get("time_unit_seconds") or 1.0 if imputed else 1.0)
                unit = "epoch"
            else:
                # format="mixed": the pipeline's own to_csv writes a time column with and without fractional seconds
                frame[time_col] = pd.to_datetime(frame[time_col], format="mixed")
                unit = "datetime"
            if original is not None:
                narrow_df = narrow.wrap_panel(frame, state, original, time_col=time_col, rename=inverse, time_unit=unit)
            else:
                narrow_df = narrow.wrap(frame, state, time_col=time_col, rename=inverse, time_unit=unit)
            target = Path(workdir) / f"{Path(path).stem}_narrow.csv"
            narrow_df.to_csv(target, index=False)
            out[name] = str(target)
            rows[name] = int(len(narrow_df))
            print(f"[dataops] wrapped {name}: {len(narrow_df)} narrow rows -> {target}")
        except Exception as exc:  # noqa: BLE001 - recorded, never fails the run
            left_out[name] = f"{exc.__class__.__name__}: {exc}"
            print(f"[dataops] could not wrap {name} back to the narrow layout, so it is not stored: "
                  f"{exc.__class__.__name__}: {exc}")
    return out, rows, left_out


def _unwrap_row_counts(report: dict, state, wrapped_rows: dict[str, int], imputation: dict | None) -> dict:
    """Rows in, out and imputed for a narrow dataset, in both layouts: the report's `unwrap.rows`.

    `narrow` is the layout of the file that came in and of every stored artifact: one row per timestamp, entity
    and metric, and a missing sample is a missing row. `wide` is the layout the pipeline worked on: one row per
    timestamp and one column per series, where a missing sample is an empty cell. So the narrow rows an imputation
    adds (out - in) are the cells it filled, and the wide row count does not change.

    `stages` lists the narrow rows of each stored frame; `runs` one entry per imputation run. Columns are given the
    same way: the narrow file keeps its columns (the hierarchy, the timestamp, the value, and what was ignored), and
    the wide frame has the timestamp plus one column per series.
    """
    stats = state.stats
    cleaning = report.get("cleaning") or {}
    narrow_in = stats.get("narrow_rows")
    series = stats.get("series")
    cells_total = (series or 0) * (stats.get("timestamps") or 0)
    narrow_columns = len((state.panel or {}).get("columns") or state.columns)

    def column_count(value) -> int | None:
        return len(value) if isinstance(value, list) else value if isinstance(value, int) else None

    runs = []
    for run in (imputation or {}).get("runs") or ([imputation] if imputation else []):
        stem = f"imputed_{run.get('lib')}_{run.get('method')}"
        narrow_out = wrapped_rows.get(f"{stem}_final_csv")
        runs.append({
            "lib": run.get("lib"), "method": run.get("method"), "status": run.get("status"),
            "wide": {"rows_in": stats.get("timestamps"), "rows_out": (run.get("final") or {}).get("rows"),
                     "columns_in": cleaning.get("input_columns"),
                     "columns_out": column_count((run.get("final") or {}).get("columns")),
                     "cells_missing": run.get("missing_before"), "cells_imputed": run.get("filled")},
            "narrow": {"rows_in": narrow_in, "rows_out": narrow_out,
                       "columns_in": narrow_columns, "columns_out": narrow_columns if narrow_out is not None else None,
                       "rows_imputed": narrow_out - narrow_in if narrow_out is not None and narrow_in is not None
                       else None},
        })
    return {
        "narrow": {"rows_in": narrow_in, "columns_in": narrow_columns, "columns_out": narrow_columns,
                   "stages": dict(wrapped_rows)},
        "wide": {"rows_in": stats.get("timestamps"), "series": series, "cells": cells_total,
                 "columns_in": cleaning.get("input_columns"), "columns_cleaned": cleaning.get("output_columns"),
                 "cells_present": narrow_in,
                 "rows_cleaned": cleaning.get("output_rows"),
                 "dropped_duplicate_rows": cleaning.get("dropped_duplicate_rows"),
                 "dropped_empty_rows": cleaning.get("dropped_empty_rows")},
        "runs": runs,
    }


@task
def unwrap_dataset(file_content: str, asset_title: str, format_check: dict | None = None) -> dict:
    """Unwrap a narrow dataset into one column per series, just before the DataOps pipeline.

    The layout is the one the targeted distribution declares in the catalogue (resolve_series_layout). Only the
    pipeline's input is transformed: the quality checks of the file as received (dali.validation.run_expectations)
    take the original download, so what they report names the file's own columns.

    Returns {"file_content", "timestamp_col", "wide_columns", "layout", "state"} for run_imputation, with the
    unwrapped CSV as file_content and the state as plain JSON; {"error"} when the hierarchy does not fit the data; and
    {"unwrapped": False} when the dataset declares no layout, or failed its format check, so there is nothing to do.
    """
    if format_check is not None and not format_check.get("success"):
        return {"unwrapped": False, "reason": "failed the format check"}

    params = get_current_context()["params"]
    layout = resolve_series_layout(params)
    if not layout:
        print("[dataops] no series layout declared for this distribution: the file is processed as it is")
        return {"unwrapped": False, "reason": "no layout declared"}

    print(f"[dataops] narrow dataset: hierarchy {layout['hierarchy']!r} from {layout['source']}, "
          f"{len(layout['ignore_columns'])} ignored column(s), value column {layout['value_col']!r}")
    stem = Path(asset_title).stem or "dataset"
    with tempfile.TemporaryDirectory(prefix="dali-unwrap-") as tmp:
        workdir = Path(tmp)
        input_csv = workdir / f"{stem}.csv"
        input_csv.write_text(file_content, encoding="utf-8")
        timestamp_col = layout.get("timestamp_col") or params.get("timestamp_col") or guess_timestamp_column(input_csv)
        try:
            wide_csv, state, timestamp_col, wide_columns = _unwrap_input(
                input_csv, workdir, stem, layout, timestamp_col)
        except Exception as exc:  # noqa: BLE001 - reported, and the pipeline skips; see run_imputation
            return {"unwrapped": False, "error": f"{exc.__class__.__name__}: {exc}"}
        wide_content = Path(wide_csv).read_text(encoding="utf-8")
    return {"unwrapped": True, "file_content": wide_content, "timestamp_col": timestamp_col,
            "wide_columns": wide_columns, "layout": layout, "state": state.to_dict()}


@task(multiple_outputs=True)
def run_imputation(file_content: str, asset_title: str, format_check: dict | None = None,
                         unwrapped_input: dict | None = None) -> dict:
    """Run the minimal DataOps pipeline over a downloaded distribution.

    The pipeline reads and writes files, so the content is first written to a
    scratch directory that also receives the outputs. Every path handed to it
    is absolute, which is why this calls run_pipeline directly rather than
    run_from_config — there is no dataops.yaml to resolve relative paths
    against here, and the run's configuration comes from DAG params.

    Three artifacts come out, not two: alongside the remediated CSV and the
    JSON report, the pipeline always writes the *soft-cleaned* frame (the
    conservative clean, before per-issue remediation). It is passed explicitly
    so it lands in the scratch directory under a predictable name instead of
    being derived from the output's stem.

    A validation failure is not treated as a task failure. run_pipeline writes
    every artifact *before* re-raising a validation error, so the error is
    captured and returned instead of propagating — the artifacts are still
    worth uploading, and the outcome is reported at the end of the DAG (the
    same way dali.validation.report_outcome reports a failed expectation suite
    rather than failing the run).

    `format_check` is dali.validation.validate_file_format's result. The
    pipeline's very first step is to read the file into a DataFrame, so a file
    that already failed the format check has nothing here to clean or validate
    — the run is skipped and reported as such, rather than crashing on a
    garbage parse and turning a reportable data problem into a DAG failure.

    `unwrapped_input` is dali.processing.unwrap_dataset's result. When it is given, the run uses it as it is: its
    wide CSV is what the pipeline reads, its state wraps the results back, and an unwrap that failed there skips the
    run here. When it is None the layout is resolved and the file unwrapped in this task, as before.

    The quality checks of the file as received, Great Expectations and pandera, are not run here: they are
    dali.validation.run_expectations' job, over the file's own columns, and merge_quality_report joins them to this
    run's report. Here they are switched off (validation `run_gx` and `run_pandera` False), so nothing in this task's
    report names a column that the unwrapping generated.

    Returns the artifact paths plus the pipeline's own report, for
    dali.datalake.upload_artifacts.
    """
    if format_check is not None and not format_check.get("success"):
        reason = (format_check.get("result") or {}).get("error", "invalid file format")
        print(f"[dataops] skipping pipeline — {asset_title!r} failed the format check: {reason}")
        return {
            "workdir":   None,
            "artifacts": {},
            "report":    None,
            "error":     f"skipped: format check failed ({reason})",
        }

    params = get_current_context()["params"]
    run_pipeline = _load_run_pipeline()

    # A scratch directory per run: the pipeline writes several files derived
    # from the output's name, and a shared directory would let concurrent runs
    # over different distributions overwrite each other.
    workdir = _scratch_dir()
    # basename only — asset_title comes from a remote catalogue, and a path
    # separator in it would otherwise write outside the scratch directory.
    stem = Path(asset_title).stem or "dataset"

    input_csv        = workdir / f"{stem}.csv"
    output_csv       = workdir / f"{stem}_remediated.csv"
    soft_cleaned_csv = workdir / f"{stem}_soft_cleaned.csv"
    report_json      = workdir / f"{stem}_report.json"

    input_csv.write_text(file_content, encoding="utf-8")
    print(f"[dataops] wrote {len(file_content)} bytes to {input_csv}")

    validation_config = {**(params.get("validation") or DEFAULT_VALIDATION_CONFIG), "run_gx": False, "run_pandera": False}
    imputation_config = imputation_config_for(params)
    timeline_config   = params.get("timeline") or DEFAULT_TIMELINE_CONFIG
    timestamp_col     = params.get("timestamp_col") or None
    if not timestamp_col:
        timestamp_col = guess_timestamp_column(input_csv)

    # A narrow dataset (one row per timestamp, entity and metric) is unwrapped into one column per series for the
    # pipeline, and the imputed timelines are wrapped back at the end. See dali.unwrap. The DAG does the unwrapping in
    # its own task, unwrap_dataset, just before this one; without that input it is done here.
    unwrapped = None
    pipeline_csv = input_csv
    if unwrapped_input is None:
        layout = resolve_series_layout(params)
        if layout:
            print(f"[dataops] narrow dataset: hierarchy {layout['hierarchy']!r} from {layout['source']}, "
                  f"{len(layout['ignore_columns'])} ignored column(s), value column {layout['value_col']!r}")
            try:
                pipeline_csv, state, timestamp_col, wide_columns = _unwrap_input(
                    input_csv, workdir, stem, layout, timestamp_col)
                unwrapped = (state, wide_columns)
            except Exception as exc:  # noqa: BLE001 - a bad hierarchy is reported like a skipped run, not a crash
                reason = f"{exc.__class__.__name__}: {exc}"
                print(f"[dataops] skipping pipeline - the hierarchy could not be applied: {reason}")
                return {"workdir": str(workdir), "artifacts": {"input_csv": str(input_csv)}, "report": None,
                        "error": f"skipped: hierarchy not applied ({reason})"}
    elif unwrapped_input.get("error"):
        reason = unwrapped_input["error"]
        print(f"[dataops] skipping pipeline - the hierarchy could not be applied: {reason}")
        return {"workdir": str(workdir), "artifacts": {"input_csv": str(input_csv)}, "report": None,
                "error": f"skipped: hierarchy not applied ({reason})"}
    elif unwrapped_input.get("state"):
        layout = unwrapped_input["layout"]
        state = narrow.UnwrapState.from_dict(unwrapped_input["state"])
        pipeline_csv = workdir / f"{stem}_unwrapped.csv"
        pipeline_csv.write_text(unwrapped_input["file_content"], encoding="utf-8")
        timestamp_col = unwrapped_input.get("timestamp_col") or timestamp_col
        unwrapped = (state, list(unwrapped_input.get("wide_columns") or []))
        print(f"[dataops] narrow dataset: hierarchy {layout['hierarchy']!r} from {layout['source']}, "
              f"{len(layout['ignore_columns'])} ignored column(s), value column {layout['value_col']!r}; "
              f"unwrapped by the DAG into {state.stats.get('series')} series")
    print(f"[dataops] mode={validation_config.get('mode')!r} timestamp_col={timestamp_col!r}")

    # The vendored pipeline cannot read timezone-aware timestamps (dali.timestamps), so a time column with an offset or a
    # "Z" is converted to naive UTC here, before the file is handed over. Best effort: if it cannot be done the pipeline
    # gets the file as it came and reports what it makes of it.
    timestamps_utc = None
    try:
        timestamps_utc = normalise_timestamps(pipeline_csv, timestamp_col, workdir / f"{stem}_utc.csv")
    except Exception as exc:  # noqa: BLE001 - an adaptation that fails must not take the run down with it
        print(f"[dataops] could not normalise the timestamps to UTC, passing the file as it is: "
              f"{exc.__class__.__name__}: {exc}")
    if timestamps_utc:
        pipeline_csv = workdir / f"{stem}_utc.csv"
        print(f"[dataops] timestamps in {timestamps_utc['column']!r} carried an offset ({timestamps_utc['offset']}): "
              f"converted to UTC for the pipeline ({timestamps_utc['rows']} row(s), {timestamps_utc['unparsed']} unparsed)")

    pipeline_error = None
    try:
        run_pipeline(
            str(pipeline_csv),
            str(output_csv),
            str(report_json),
            timestamp_col=timestamp_col,
            validation_config=validation_config,
            imputation_config=imputation_config,
            timeline_config=timeline_config,
            soft_cleaned_csv=str(soft_cleaned_csv),
        )
    except Exception as exc:  # noqa: BLE001 - reported, not raised; see docstring
        pipeline_error = f"{exc.__class__.__name__}: {exc}"
        print(f"[dataops] pipeline reported a validation failure: {pipeline_error}")

    # Read the report back from the file rather than using run_pipeline's return
    # value. Two reasons, and the first is not optional: the returned dict holds
    # live pandas objects — profile.preview is a list of rows whose timestamps
    # are pd.Timestamp — and Airflow's XCom serializer refuses them outright
    # ("cannot serialize object of type ... Timestamp"). The file is written with
    # json.dumps(default=str), so reading it back yields the same content in
    # JSON-safe form. It also guarantees the XCom and the uploaded artifact are
    # byte-for-byte the same report, rather than two views that could drift.
    # The file is written before run_pipeline re-raises a validation error, so
    # this works on the failure path too.
    report: dict = {}
    if report_json.exists():
        report = json.loads(report_json.read_text(encoding="utf-8"))
    else:
        print("[dataops] no report file was written")

    # The raw frame is uploaded too, not just the pipeline's own outputs: the
    # report describes a raw -> soft-cleaned -> remediated lineage, and without
    # the first stage a reader can see what changed but never what it started
    # from. It is the bytes as transferred over EDC, which is not otherwise
    # recoverable from this bucket — the distribution was pulled from the
    # provider's connector, not from here.
    # The pipeline stops at the handoff; run the imputation it prepared. Done
    # here rather than inside run_pipeline because minimal_dataops is vendored
    # verbatim from WaveStitchPlus (plugins/VENDORED.md) and must not diverge.
    imputation = impute_prepared_bundle(report, imputation_config)

    # The imputed splits are what a reader actually wants to see, so they are
    # uploaded alongside the frames. The bundle's other files stay on disk: they
    # are an intermediate the imputation consumed, not a result.
    # Every run's artifacts are named for what produced them
    # (imputed_darts_linear_train_csv, …), the first included. Runs made before
    # this carry the bare names (imputed_train_csv, …); dataops-ui reads both.
    imputed_files: dict[str, str] = {}
    for run in (imputation or {}).get("runs") or ([imputation] if imputation else []):
        run_stem = f"imputed_{run.get('lib')}_{run.get('method')}"
        for kind, info in (run.get("files") or {}).items():
            if info.get("path") and Path(info["path"]).exists():
                imputed_files[f"{run_stem}_{kind}_csv"] = info["path"]
        # …and the stitched timeline, which is the one to read if you only read one.
        final_path = (run.get("final") or {}).get("path")
        if final_path and Path(final_path).exists():
            imputed_files[f"{run_stem}_final_csv"] = final_path

    produced = {
        name: str(path)
        for name, path in (
            ("input_csv",        input_csv),
            ("output_csv",       output_csv),
            ("report_json",      report_json),
            ("soft_cleaned_csv", soft_cleaned_csv),
        )
        if path.exists()
    }
    produced.update(imputed_files)

    # A narrow dataset is stored narrow: the pipeline worked on one column per series, but every frame it produced is
    # wrapped back into the layout of the file that came in before anything is uploaded (see _wrap_artifacts).
    if unwrapped:
        state, wide_columns = unwrapped
        prepared_dir = ((report.get("handoff") or {}).get("prepared_dir"))
        produced, wrapped_rows, not_wrapped = _wrap_artifacts(
            produced, state, wide_columns, report, prepared_dir, workdir)
        report["unwrap"] = {"hierarchy": layout["hierarchy"], "separator": state.sep, "source": layout["source"],
                            "ignore_columns": list(state.constants),
                            "timestamp_col": timestamp_col, "value_col": state.value_col,
                            "tz_offset": state.tz_offset, **state.stats,
                            "wrapped_artifacts": wrapped_rows, "not_wrapped": not_wrapped,
                            "layout": "panel" if state.panel else "narrow",
                            "levels": state.levels(),
                            "rows": _unwrap_row_counts(report, state, wrapped_rows, imputation)}
    if timestamps_utc:
        report["timestamps"] = timestamps_utc
    # The shape of the file that came in: wide (one column per series) unless it was unwrapped, in which case it was
    # narrow or a panel. Every run says it, so a wide file is named as much as the other two.
    if report:
        report["layout"] = report["unwrap"]["layout"] if unwrapped else "wide"
    if report or imputation or unwrapped or timestamps_utc:
        if imputation:
            report["imputation"] = imputation
        # Written back to the file, not just to the dict: the report artifact is
        # uploaded from disk, and it is what the results view reads. Leaving the
        # summary in memory only would upload a report that never mentions the
        # imputation or the layout of this run.
        report_json.write_text(
            json.dumps(report, indent=2, default=str) + "\n", encoding="utf-8"
        )

    for name, path in produced.items():
        print(f"[dataops] {name}: {path} ({os.path.getsize(path)} bytes)")

    # The report carries NaN cells (e.g. the profile's row preview). They are not valid JSON,
    # and the XCom push of this return value fails on them, so replace them with None.
    return sanitize({
        "workdir":   str(workdir),
        "artifacts": produced,
        "report":    report,
        "error":     pipeline_error,
    })


@task
def report_pipeline_outcome(pipeline: dict, uploaded: dict) -> None:
    """Log what the pipeline did and where its artifacts went.

    Mirrors dali.validation.report_outcome: a data-quality failure is surfaced
    in the logs and in the uploaded report, not by failing the DAG.
    """
    report = pipeline.get("report") or {}
    cleaning = report.get("cleaning") or {}
    validation = report.get("validation") or {}
    quality = report.get("quality") or {}

    status = "FAILED" if pipeline.get("error") else "PASSED"
    print(f"[dataops] pipeline {status} (mode={validation.get('mode')}, "
          f"pandera_passed={validation.get('pandera_passed')}, "
          f"gx_passed={(quality.get('report') or {}).get('gx_passed')})")
    print(f"[dataops] rows: {cleaning.get('input_rows')} in -> "
          f"{cleaning.get('output_rows')} out")
    imputation = report.get("imputation") or {}
    if imputation:
        # status/reason rather than filled-or-error: "0 cells filled" is a
        # correct outcome for a series with nothing to fill, and reading it as a
        # failure is exactly the confusion this line exists to prevent.
        for run in imputation.get("runs") or [imputation]:
            print(f"[dataops] imputation {run.get('lib')}/{run.get('method')} "
                  f"({run.get('engine')}) {run.get('status')}: {run.get('reason')}")
            if run.get("status") == "imputed":
                print(f"[dataops]   {run.get('filled')} of {run.get('missing_before')} "
                      f"missing values filled")
    for name, key in (uploaded or {}).items():
        print(f"[dataops] uploaded {name} -> {key}")
    if pipeline.get("error"):
        print(f"[dataops] validation error: {pipeline['error']}")
    for err in validation.get("errors") or []:
        print(f"[dataops]   - {err}")


def _scratch_dir() -> Path:
    """A fresh scratch directory for one run: under DALI_WORK_ROOT when that is set, else in the system temp.

    DALI_WORK_ROOT is the volume the WaveStitch+ service shares with Airflow. The bundle the pipeline writes has to be
    on it for the service to read, so everything a run writes goes there. Unset (no service in this stack), nothing
    changes from the system temp directory.
    """
    root = os.environ.get("DALI_WORK_ROOT", "").strip()
    if not root:
        return Path(tempfile.mkdtemp(prefix="dali-dataops-"))
    Path(root).mkdir(parents=True, exist_ok=True)
    return Path(tempfile.mkdtemp(prefix="dali-dataops-", dir=root))


@task
def cleanup_workdir(workdir: str | None) -> None:
    """Remove the scratch directory once its artifacts are uploaded.

    Airflow workers are long-lived, so a run that left its scratch directory
    behind would leak a full copy of every dataset it processed onto the
    worker's disk. A skipped pipeline never made one — nothing to remove.
    """
    if not workdir:
        print("[dataops] no scratch directory to remove — pipeline was skipped")
        return
    shutil.rmtree(workdir, ignore_errors=True)
    print(f"[dataops] removed {workdir}")
