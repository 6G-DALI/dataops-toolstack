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
import tempfile
import warnings
from pathlib import Path
from typing import Any

from airflow.decorators import task
from airflow.sdk import get_current_context

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
# engine reproduces Darts' MissingValuesFiller bit-faithfully without importing
# darts. "linear" specifically, not the runner's own "nearest" default: pandas
# implements linear interpolation itself, while nearest/cubic/quadratic/slinear/
# zero all delegate to SciPy, which the Airflow image does not install (see
# airflow/base/requirements.txt — the heavy imputation extras belong to the
# imputation apps). With scipy absent, "nearest" raises and impute_prepared_bundle
# would report a failed imputation on every run.
#
# Set `impute: false` to get the bundle without filling it.
DEFAULT_IMPUTATION_CONFIG: dict[str, Any] = {
    "build_bundle": True,
    "impute": True,
    "lib": "darts",
    "method": "linear",
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


def impute_prepared_bundle(report: dict, imputation_config: dict) -> dict | None:
    """Run imputation over the handoff's regularized bundle, in-process.

    The pipeline deliberately stops at the handoff: it regularizes the timeline
    into a prepared-dir bundle and records which (app, method) an external
    orchestrator *would* invoke, but fills nothing ("The pipeline never runs
    imputation" — minimal_dataops._build_handoff). This runs that step, so a DAG
    run ends with imputed values rather than an invoke_hint.

    dataops.imputation_runner's "pandas" engine is used, not the Darts
    subprocess: it reproduces Darts' MissingValuesFiller exactly for the
    interpolation family, so the result is bit-faithful without putting darts in
    the Airflow image.

    Returns the runner's summary, or None when there was nothing to do — no gaps
    detected, no bundle written, or imputation switched off. Never raises: a
    failure is recorded and the run still uploads its artifacts, the same way a
    validation failure is.
    """
    if not imputation_config.get("impute", True):
        return None

    handoff = (report or {}).get("handoff") or {}
    prepared_dir = handoff.get("prepared_dir")
    if not handoff.get("bundle_written") or not prepared_dir:
        # needs_ts_imputation false (no gaps), not a time series, or the bundle
        # failed to build — handoff.reason / handoff.bundle_error says which.
        print(f"[dataops] no bundle to impute (reason={handoff.get('reason')!r}, "
              f"bundle_error={handoff.get('bundle_error')!r})")
        return None

    lib    = imputation_config.get("lib") or "darts"
    method = imputation_config.get("method") or "nearest"
    try:
        from dataops.imputation_runner import impute_bundle

        summary = impute_bundle(prepared_dir, method=method, lib=lib, engine="pandas")
    except Exception as exc:  # noqa: BLE001 - recorded, never fails the run
        print(f"[dataops] imputation failed: {exc.__class__.__name__}: {exc}")
        return {"lib": lib, "method": method, "engine": "pandas",
                "error": f"{exc.__class__.__name__}: {exc}", "files": {}}

    filled = sum(f.get("filled", 0) for f in summary.get("files", {}).values())
    print(f"[dataops] imputed with {lib}/{method}: {filled} cells filled across "
          f"{len(summary.get('files', {}))} split(s)")
    return summary


@task(multiple_outputs=True)
def run_dataops_pipeline(file_content: str, asset_title: str) -> dict:
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

    Returns the artifact paths plus the pipeline's own report, for
    dali.datalake.upload_artifacts.
    """
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
    imputed_files = {
        f"imputed_{kind}_csv": info["path"]
        for kind, info in ((imputation or {}).get("files") or {}).items()
        if info.get("path") and Path(info["path"]).exists()
    }

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

    return {
        "workdir":   str(workdir),
        "artifacts": produced,
        "report":    report,
        "error":     pipeline_error,
    }


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
        if imputation.get("error"):
            print(f"[dataops] imputation failed: {imputation['error']}")
        else:
            filled = sum(f.get("filled", 0) for f in (imputation.get("files") or {}).values())
            print(f"[dataops] imputation {imputation.get('lib')}/{imputation.get('method')}: "
                  f"{filled} cells filled")
    for name, key in (uploaded or {}).items():
        print(f"[dataops] uploaded {name} -> {key}")
    if pipeline.get("error"):
        print(f"[dataops] validation error: {pipeline['error']}")
    for err in validation.get("errors") or []:
        print(f"[dataops]   - {err}")


@task
def cleanup_workdir(workdir: str) -> None:
    """Remove the scratch directory once its artifacts are uploaded.

    Airflow workers are long-lived, so a run that left its scratch directory
    behind would leak a full copy of every dataset it processed onto the
    worker's disk.
    """
    shutil.rmtree(workdir, ignore_errors=True)
    print(f"[dataops] removed {workdir}")
