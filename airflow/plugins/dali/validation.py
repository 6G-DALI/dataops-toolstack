from __future__ import annotations

import os
import sys
from datetime import datetime, timezone

import great_expectations as gx

from airflow.decorators import task
from airflow.sdk import get_current_context

from dali.utils import (
    DEFAULT_EXPECTATIONS,
    dataframe_from_content,
    detect_format,
    exp_class,
    fetch_columns_from_piveau,
    parse_expectations,
    sanitize,
)


@task
def validate_file_format(file_content: str, asset_title: str) -> dict:
    """Pre-flight check, run before the Great Expectations suite: does the
    file's content actually match its own media-type/extension — i.e. is the
    "csv" file a proper CSV, the "jsonl" file proper JSON Lines, etc. — and
    can it be loaded into a DataFrame at all. GX expectations validate
    *values* in a DataFrame; they can't run at all if the file can't even be
    parsed into one, so this needs to happen first and be reported as its own
    result rather than crashing the DAG deep inside run_expectations.

    Returns a dict shaped like one entry of run_expectations' report["results"]
    (expectation_type/kwargs/success/result), so it can be prepended there
    and shown/published alongside the real GX results without special-casing."""
    fmt = detect_format(asset_title)
    try:
        df = dataframe_from_content(file_content, asset_title)
    except Exception as exc:
        print(f"[dali] {asset_title!r} failed {fmt} format validation: {exc}")
        return {
            "expectation_type": f"expect_file_to_be_valid_{fmt}",
            "kwargs":           {"asset_title": asset_title},
            "success":          False,
            "result":           {"error": str(exc)},
        }
    print(f"[dali] {asset_title!r} passed {fmt} format validation: "
          f"{len(df)} rows, columns={list(df.columns)}")
    return {
        "expectation_type": f"expect_file_to_be_valid_{fmt}",
        "kwargs":           {"asset_title": asset_title},
        "success":          True,
        "result":           {"row_count": len(df), "columns": list(df.columns)},
    }


@task
def run_expectations(file_content: str, asset_title: str, format_check: dict) -> dict:
    params = get_current_context()["params"]
    dataset_id = params["dataset_id"]
    input_key = f"{dataset_id}/{asset_title}"
    run_time = datetime.now(timezone.utc).isoformat()

    if not format_check["success"]:
        # No valid DataFrame to run GX expectations against — report just
        # the failed format check rather than crashing on a garbage parse.
        return {
            "input_key":  input_key,
            "run_time":   run_time,
            "success":    False,
            "statistics": {
                "evaluated_expectations":   1,
                "successful_expectations":  0,
                "unsuccessful_expectations": 1,
                "success_percent":          0.0,
            },
            "results": [format_check],
        }

    expectations = parse_expectations(params["expectations"])

    df = dataframe_from_content(file_content, asset_title)

    context = gx.get_context(mode="ephemeral")
    datasource = context.data_sources.add_pandas("runtime_source")
    asset = datasource.add_dataframe_asset("dataset")
    batch_definition = asset.add_batch_definition_whole_dataframe("batch")

    suite = context.suites.add(gx.ExpectationSuite(name="dali_validation_suite"))
    if expectations:
        resolved = expectations
    else:
        asset_id = params.get("asset_id", "")
        columns = fetch_columns_from_piveau(dataset_id, asset_id)
        resolved = list(DEFAULT_EXPECTATIONS)
        for col in columns:
            resolved.append({"type": "expect_column_to_exist", "column": col})
            resolved.append({"type": "expect_column_values_to_not_be_null", "column": col})
        print(f"[dali] auto-generated {len(resolved)} expectations from {len(columns)} piveau columns")
    for exp in resolved:
        exp_type = exp.get("type")
        kwargs = {k: v for k, v in exp.items() if k != "type"}
        suite.add_expectation(exp_class(exp_type)(**kwargs))

    validation_definition = context.validation_definitions.add(
        gx.ValidationDefinition(name="validation", data=batch_definition, suite=suite)
    )
    # Suppress GX tqdm progress bars so Airflow doesn't treat stderr output as an error
    _old_stderr = sys.stderr
    sys.stderr = open(os.devnull, "w")
    try:
        results = validation_definition.run(batch_parameters={"dataframe": df})
    finally:
        sys.stderr.close()
        sys.stderr = _old_stderr

    gx_stats = results.statistics
    evaluated  = gx_stats.get("evaluated_expectations", 0) + 1
    successful = gx_stats.get("successful_expectations", 0) + 1  # format_check already passed
    return {
        "input_key":  input_key,
        "run_time":   run_time,
        "success":    bool(results.success),  # format_check passing doesn't affect this — it already did
        "statistics": {
            "evaluated_expectations":    evaluated,
            "successful_expectations":   successful,
            "unsuccessful_expectations": evaluated - successful,
            "success_percent":           100.0 * successful / evaluated if evaluated else 0.0,
        },
        "results": [format_check] + [
            {
                "expectation_type": r.expectation_config.type,
                "kwargs":           r.expectation_config.kwargs,
                "success":          r.success,
                "result":           sanitize({
                    k: v for k, v in r.result.items()
                    if not k.startswith("partial")
                }),
            }
            for r in results.results
        ],
    }


@task
def report_outcome(output_key: str, report: dict) -> None:
    status = "PASSED" if report["success"] else "FAILED"
    total  = report["statistics"].get("evaluated_expectations", 0)
    passed = report["statistics"].get("successful_expectations", 0)
    print(f"Validation {status}: {passed}/{total} expectations passed")
    print(f"Results written to: {output_key}")
    if not report["success"]:
        failed = [
            f"{r['expectation_type']}({r['kwargs'].get('column', '')})"
            for r in report["results"] if not r["success"]
        ]
        print(f"[dali] {len(failed)} expectation(s) failed: {failed}")


# ── Merging the DataOps pipeline's own verdict in ─────────────────────────────
#
# The WaveStitchPlus pipeline (dali.processing.run_dataops_pipeline) runs its
# own auto-generated Great Expectations suite and a pandera schema over the same
# distribution this DAG validates against the conf-supplied suite. Those
# findings used to stop at the pipeline's report.json; merge_quality_report
# folds them into the report this DAG already knows how to store and publish.
#
# Everything is normalised into the same four-key entry shape
# validate_file_format established — {expectation_type, kwargs, success, result}
# — so upload_results, publish_quality_to_piveau and report_outcome consume the
# merged report exactly as they consume run_expectations' own, and
# dali.dataspace needs no change: each entry still becomes one
# dqv:QualityMeasurement.

# The two GX suites are reported as one execution, under the expectations' own
# names. publish_quality_to_piveau mints a measurement URI per entry as
# {dist_uri}/quality/{expectation_type}[_{column}], so an expectation both
# suites declare on the same column addresses one node — which is right, since
# it is one assertion about one column of one frame. It is folded into a single
# entry here rather than published twice, or disambiguated into two nodes that
# would imply the data was checked for two different things.
#
# TODO(quality): unify the *execution*, not just the reporting.
#
# GX still runs twice, in two Airflow tasks, against two frames:
#
#   run_expectations       own ephemeral context, suite "dali_validation_suite",
#                          over the RAW file parsed from file_content
#   ts_checks.run          dataops.gx.get_gx_context, suite "ts_quality", over
#   (run_dataops_pipeline) the SOFT-CLEANED frame — minimal_dataops.run_pipeline
#                          calls clean_dataframe() before _run_quality_checks
#
# So a folded entry ANDs a verdict on the raw frame with a verdict on a frame
# that has already had empty/duplicate rows dropped, columns snake_cased, and
# (in time_series mode) duplicate timestamps dropped and rows sorted. Both
# passing is the common case and hides this; on a dirty file the two can
# legitimately disagree, and the fold would report the stricter one without
# saying they measured different things. The snake_casing is a second hazard:
# a piveau column "Lat99" becomes "lat99" in the pipeline's half, so the two
# halves can key the same column differently and fail to fold at all.
#
# Fixing it means choosing which frame the single pass validates, and that is a
# real decision, not a refactor:
#   - RAW is what the catalogue serves and what a dqv:QualityMeasurement should
#     therefore describe — but the pipeline's quantile bands and gap checks are
#     designed to run after cleaning and change meaning on it.
#   - SOFT-CLEANED keeps those checks meaningful, but publishes quality claims
#     about a frame no consumer can fetch — the same objection that stopped us
#     publishing the post-remediation re-check (see PUBLISHED_STAGE).
# Either way the combined suite should be built in one place and run once,
# which reaches into minimal_dataops and the vendored dataops/ tree.
# Deferred 2026-08-31; reporting was unified first because it was separable.

PANDERA_EXPECTATION = "expect_pandera_schema_to_validate"
TOTALS_EXPECTATION  = "expect_all_quality_checks_to_pass"

# Only the pre-remediation stage is published. The post-remediation re-check
# re-runs the same expectations against the *remediated* frame, so publishing it
# would put two measurements of the same check on the distribution, describing
# two different frames — and only one of them, the raw one, is the frame the
# catalogue actually serves. Its verdict is still reported in "sources", where
# the UI can show it, but it mints no dqv:QualityMeasurement.
PUBLISHED_STAGE = "pre_remediation"


def _check_key(entry: dict) -> tuple[str, str]:
    """What publish_quality_to_piveau keys a measurement URI on.

    It builds {dist_uri}/quality/{expectation_type}[_{column}], so two entries
    sharing this pair are two writes to one node — the reason duplicates have to
    be folded here rather than left for the catalogue to arbitrate."""
    return entry["expectation_type"], (entry.get("kwargs") or {}).get("column") or ""


def _stage_checks(quality: dict | None) -> list[dict]:
    """The pipeline's GX stage as result entries, under the checks' own names.

    Deliberately not namespaced: the pipeline runs real GX expectations, and
    prefixing them would publish `expect_column_values_to_not_be_null` on the
    same column twice under two different metric URIs. Sharing the name is what
    lets _fold_duplicate collapse them into one measurement.

    Detail comes from summarize_gx's "expectations" — the complete roll of the
    suite, passing checks included. Older reports carry only the truncated
    "failed_expectations", and degrade to the failures they do have."""
    if not quality:
        return []

    gx_summary = (quality.get("report") or {}).get("gx") or {}
    checks = gx_summary.get("expectations")
    if checks is None:
        checks = [{**f, "success": False} for f in gx_summary.get("failed_expectations") or []]

    return [{
        "expectation_type": check.get("expectation"),
        "kwargs":           {"column": check.get("column") or ""},
        "success":          bool(check.get("success")),
        "result":           sanitize({"unexpected_percent": check.get("unexpected_percent")}),
    } for check in checks]


def _fold_duplicate(existing: dict, incoming: dict) -> None:
    """Merge a check declared by both suites into the entry already holding it.

    The kept entry is the conf suite's, whose result carries GX's full payload
    (element_count, unexpected_count, …) against the pipeline's percentage
    alone. Success is the conjunction: a check is only passed if it passed
    everywhere it was declared.

    Which suite declared it is deliberately not recorded on the entry. The two
    suites are one Great Expectations pass over one frame as far as a reader —
    or a catalogue consumer — is concerned, and stamping each measurement with
    its origin would publish an implementation detail of this DAG as though it
    were a property of the data. The overlap is still reported once, at the
    report's top level, as duplicate_checks."""
    existing["success"] = bool(existing.get("success")) and bool(incoming.get("success"))

    pct = (incoming.get("result") or {}).get("unexpected_percent")
    if pct is not None:
        existing.setdefault("result", {}).setdefault("unexpected_percent", pct)


def _pandera_entry(validation: dict) -> dict:
    """The pandera schema check as a single result entry.

    One entry, not one per failure: pandera is invoked without lazy=True (see
    dataops.validation.pandera_schemas), so it fails fast with a single
    exception rather than a failure_cases frame — and the pipeline's own
    pre-checks raise plain ValueErrors into the same list. All that survives is
    a list of strings, which belongs in the entry's result."""
    return {
        "expectation_type": PANDERA_EXPECTATION,
        "kwargs":           {"source": "pandera", "mode": validation.get("mode")},
        "success":          bool(validation.get("pandera_passed")),
        "result":           sanitize({
            "configured_mode":  validation.get("configured_mode"),
            "timestamp_column": validation.get("timestamp_column"),
            "errors":           list(validation.get("errors") or []),
        }),
    }


def _stage_source(quality: dict | None) -> dict:
    gx_summary = (quality or {}).get("report", {}).get("gx") or {}
    return {
        "success":   bool((quality or {}).get("gx_passed")),
        "evaluated": int(gx_summary.get("evaluated") or 0),
        "passed":    int(gx_summary.get("passed") or 0),
    }


@task
def merge_quality_report(gx_report: dict, pipeline: dict) -> dict:
    """Combine the conf-driven GX suite with the DataOps pipeline's own GX and
    pandera results into one report of distinct checks.

    Returns the same shape run_expectations returns, plus:
      - "sources", one verdict per *kind* of check — great_expectations and
        pandera — plus the post-remediation re-check, which is reported but
        never published;
      - "duplicate_checks", the expectations declared by both halves of the GX
        suite and counted once;
      - a totals entry in "results", so the combined figure reaches piveau as a
        measurement of its own rather than living only in "statistics", which
        is never published.

    The statistics count *distinct* checks: the conf-supplied and generated
    halves of the GX suite overlap heavily (both assert not-null on every
    column), and counting each declaration separately would inflate the total
    with work that only ever tested one thing.

    Never raises: a merge failure must not lose the GX results that already ran,
    so a skipped or crashed pipeline just contributes fewer entries."""
    pipeline_report = (pipeline or {}).get("report") or {}
    pipeline_error  = (pipeline or {}).get("error")

    # Copied, not referenced: entries are folded in place below, and the XCom
    # payload should not appear mutated to anything else reading it.
    checks: list[dict] = [
        {**entry, "kwargs": dict(entry.get("kwargs") or {}), "result": dict(entry.get("result") or {})}
        for entry in (gx_report.get("results") or [])
    ]
    index: dict[tuple[str, str], dict] = {}
    for entry in checks:
        index.setdefault(_check_key(entry), entry)

    duplicates: list[dict] = []

    quality = pipeline_report.get("quality")
    if quality:
        for entry in _stage_checks(quality):
            key = _check_key(entry)
            existing = index.get(key)
            if existing is None:
                index[key] = entry
                checks.append(entry)
            else:
                _fold_duplicate(existing, entry)
                duplicates.append({"expectation_type": key[0], "column": key[1]})

    results = list(checks)

    # Great Expectations is reported as one execution. The DAG happens to build
    # its suite from two places — the conf-supplied/piveau-derived expectations
    # and the pipeline's own generated ones — but they run over the same frame,
    # at the same point, to the same end, and their overlap has already been
    # folded above. Splitting the report along that seam would describe how this
    # DAG is wired rather than what was checked.
    gx_evaluated = len(checks)
    gx_passed    = sum(1 for entry in checks if entry["success"])
    sources: dict = {}

    if pipeline_report:
        validation = pipeline_report.get("validation") or {}
        entry = _pandera_entry(validation)
        results.append(entry)
        sources["pandera"] = {
            "success": entry["success"],
            "mode":    validation.get("mode"),
            "errors":  len(validation.get("errors") or []),
        }

    sources["great_expectations"] = {
        "success":   gx_passed == gx_evaluated,
        "evaluated": gx_evaluated,
        "passed":    gx_passed,
    }

    # Reported, never published — see PUBLISHED_STAGE. Kept out of the totals and
    # last in the ordering: it measures the remediated frame, so it belongs with
    # remediation rather than with the checks on the frame as received.
    if pipeline_report.get("quality_after"):
        sources["great_expectations_after_remediation"] = _stage_source(pipeline_report["quality_after"])

    evaluated = len(results)
    passed    = sum(1 for entry in results if entry["success"])
    success   = passed == evaluated and not pipeline_error
    statistics = {
        "evaluated_expectations":    evaluated,
        "successful_expectations":   passed,
        "unsuccessful_expectations": evaluated - passed,
        "success_percent":           100.0 * passed / evaluated if evaluated else 0.0,
    }

    # The totals entry goes last so it reads as the summary of everything above
    # it, and so report_outcome lists it last among any failures.
    results.append({
        "expectation_type": TOTALS_EXPECTATION,
        "kwargs":           {"source": "merged", "sources": sorted(sources)},
        "success":          success,
        "result":           sanitize({
            **statistics,
            "duplicate_checks": len(duplicates),
            "pipeline_error":   pipeline_error,
        }),
    })

    print(f"[dali] merged quality report: {passed}/{evaluated} distinct checks passed "
          f"(great_expectations {gx_passed}/{gx_evaluated})")
    if duplicates:
        print(f"[dali] {len(duplicates)} expectation(s) declared by both halves of the GX "
              f"suite, counted once: {sorted({d['expectation_type'] for d in duplicates})}")
    if pipeline_error:
        print(f"[dali] DataOps pipeline reported: {pipeline_error}")

    return {
        "input_key":        gx_report.get("input_key"),
        "run_time":         gx_report.get("run_time"),
        "success":          success,
        "statistics":       statistics,
        "sources":          sources,
        "duplicate_checks": duplicates,
        "results":          results,
    }
