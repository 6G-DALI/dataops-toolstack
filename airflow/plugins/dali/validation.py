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
