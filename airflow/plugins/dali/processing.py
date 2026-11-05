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
import warnings
from pathlib import Path
from typing import Any

from airflow.decorators import task
from airflow.sdk import get_current_context

from dali.utils import sanitize

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
DEFAULT_IMPUTATION_CONFIG: dict[str, Any] = {
    "build_bundle": True,
    "impute": True,
    "runs": [
        {"lib": "darts", "method": "linear"},
        {"lib": "darts", "method": "nearest"},
    ],
}


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


def _load_run_pipeline():
    """Import run_pipeline from the vendored pipeline module.

    Deferred to task runtime on purpose — see the module docstring."""
    try:
        from minimal_dataops import run_pipeline
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
    only the Darts runner can do.

    Pairs that work: engine "pandas" with lib "darts" (auto, linear, nearest,
    quadratic, cubic, slinear, zero — everything but linear and auto needs
    SciPy) or lib "imputegap" (interpolation, mean, mean_by_series, min, zero);
    engine "darts", lib "darts" only (those plus kalman). Anything else is
    recorded as an error for that run and the other runs still happen.
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
        if (lib, method) in seen:
            continue
        seen.add((lib, method))
        engine = item.get("engine") or ("darts" if (lib, method) == ("darts", "kalman") else "pandas")
        runs.append({"lib": lib, "method": method, "engine": engine})
    return runs


def _score_run(prepared_dir: str, summary: dict) -> dict:
    """How close one run's fills are to the truth hidden to score them.

    The bundle blanks about 15% of the test split in short blocks and keeps the
    true values in test_gt.csv, so every run is scored on the same cells.
    dataops.imputation_runner.compare_clean_vs_imputed reports the error on them
    per column; this adds the normalised figure that can be averaged and ranked.

    nrmse is RMSE divided by the spread (standard deviation) of that column's
    true values in test_gt.csv, then averaged over the columns with equal weight.
    0 is a perfect fill; about 1 is no better than guessing the column's mean.
    A column whose truth is constant has no spread to divide by and is left out.
    Raw MAE and RMSE stay beside it in per-column units.

    Never raises: a run that cannot be scored still counts as imputed.
    """
    scale = {"metric": "nrmse"}
    try:
        import numpy as np
        import pandas as pd
        from dataops.imputation_runner import compare_clean_vs_imputed

        split = (compare_clean_vs_imputed(prepared_dir, summary).get("splits") or {}).get("test") or {}
        accuracy = split.get("accuracy") or {}
        if not accuracy.get("eval_cells"):
            return {**scale, "nrmse": None, "eval_cells": 0,
                    "reason": "the bundle holds no hidden cells to score against"}

        truth = pd.read_csv(Path(prepared_dir) / "test_gt.csv")
        per_column: dict[str, dict] = {}
        for column, errors in (accuracy.get("per_column") or {}).items():
            spread = float(np.nanstd(pd.to_numeric(truth[column], errors="coerce")))
            per_column[column] = {
                "eval_cells": errors["eval_cells"],
                "MAE":        errors["MAE"],
                "RMSE":       errors["RMSE"],
                "nrmse":      errors["RMSE"] / spread if spread > 0 else None,
            }
        scored = [c["nrmse"] for c in per_column.values() if c["nrmse"] is not None]
        return {**scale, "nrmse": float(np.mean(scored)) if scored else None,
                "eval_cells": accuracy["eval_cells"], "per_column": per_column}
    except Exception as exc:  # noqa: BLE001 - scoring is a bonus, never a failure
        print(f"[dataops] could not score {summary.get('lib')}/{summary.get('method')}: "
              f"{exc.__class__.__name__}: {exc}")
        return {**scale, "nrmse": None, "error": f"{exc.__class__.__name__}: {exc}"}


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


def _impute_one(prepared_dir: str, bundle: dict, *, lib: str, method: str, engine: str) -> dict:
    """One imputation over the bundle. Never raises: a failure is a summary with
    status "error", so the runs after it still happen."""
    base = {"lib": lib, "method": method, "engine": engine, "files": {}, "filled": 0}
    try:
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
    its own `accuracy` and `rank`. The first run stays the primary one whatever the
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
    summaries = [_impute_one(prepared_dir, bundle, **run) for run in runs]
    ranking = _rank_runs(summaries)
    return {**summaries[0], "runs": summaries, "runs_total": len(summaries),
            "runs_imputed": sum(1 for r in summaries if r.get("status") == "imputed"),
            "ranking": ranking}


@task(multiple_outputs=True)
def run_dataops_pipeline(file_content: str, asset_title: str, format_check: dict | None = None) -> dict:
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
    workdir = Path(tempfile.mkdtemp(prefix="dali-dataops-"))
    # basename only — asset_title comes from a remote catalogue, and a path
    # separator in it would otherwise write outside the scratch directory.
    stem = Path(asset_title).stem or "dataset"

    input_csv        = workdir / f"{stem}.csv"
    output_csv       = workdir / f"{stem}_remediated.csv"
    soft_cleaned_csv = workdir / f"{stem}_soft_cleaned.csv"
    report_json      = workdir / f"{stem}_report.json"

    input_csv.write_text(file_content, encoding="utf-8")
    print(f"[dataops] wrote {len(file_content)} bytes to {input_csv}")

    validation_config = params.get("validation") or DEFAULT_VALIDATION_CONFIG
    imputation_config = params.get("imputation") or DEFAULT_IMPUTATION_CONFIG
    timeline_config   = params.get("timeline") or DEFAULT_TIMELINE_CONFIG
    timestamp_col     = params.get("timestamp_col") or None
    if not timestamp_col:
        timestamp_col = guess_timestamp_column(input_csv)
    print(f"[dataops] mode={validation_config.get('mode')!r} timestamp_col={timestamp_col!r}")

    pipeline_error = None
    try:
        run_pipeline(
            str(input_csv),
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
    if imputation:
        report["imputation"] = imputation
        # Written back to the file, not just to the dict: the report artifact is
        # uploaded from disk, and it is what the results view reads. Leaving the
        # summary in memory only would upload a report that never mentions the
        # imputation this run performed.
        report_json.write_text(
            json.dumps(report, indent=2, default=str) + "\n", encoding="utf-8"
        )

    # The imputed splits are what a reader actually wants to see, so they are
    # uploaded alongside the frames. The bundle's other files stay on disk: they
    # are an intermediate the imputation consumed, not a result.
    # Every run's artifacts are named for what produced them
    # (imputed_darts_linear_train_csv, …), the first included. Runs made before
    # this carry the bare names (imputed_train_csv, …); dataops-ui reads both.
    imputed_files: dict[str, str] = {}
    for run in (imputation or {}).get("runs") or ([imputation] if imputation else []):
        stem = f"imputed_{run.get('lib')}_{run.get('method')}"
        for kind, info in (run.get("files") or {}).items():
            if info.get("path") and Path(info["path"]).exists():
                imputed_files[f"{stem}_{kind}_csv"] = info["path"]
        # …and the stitched timeline, which is the one to read if you only read one.
        final_path = (run.get("final") or {}).get("path")
        if final_path and Path(final_path).exists():
            imputed_files[f"{stem}_final_csv"] = final_path

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
