from __future__ import annotations

import io
import os
import sys
from datetime import datetime, timezone

import great_expectations as gx
import pandas as pd

from airflow.decorators import task
from airflow.sdk import get_current_context

from dali.utils import (
    DEFAULT_EXPECTATIONS,
    exp_class,
    fetch_columns_from_piveau,
    parse_expectations,
    sanitize,
)


@task
def run_expectations(csv_content: str) -> dict:
    params = get_current_context()["params"]
    input_key = params["input_key"]
    expectations = parse_expectations(params["expectations"])

    df = pd.read_csv(io.StringIO(csv_content))

    context = gx.get_context(mode="ephemeral")
    datasource = context.data_sources.add_pandas("runtime_source")
    asset = datasource.add_dataframe_asset("dataset")
    batch_definition = asset.add_batch_definition_whole_dataframe("batch")

    suite = context.suites.add(gx.ExpectationSuite(name="dali_validation_suite"))
    if expectations:
        resolved = expectations
    else:
        dataset_id = input_key.split("/")[0]
        columns = fetch_columns_from_piveau(dataset_id)
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

    return {
        "input_key":  input_key,
        "run_time":   datetime.now(timezone.utc).isoformat(),
        "success":    bool(results.success),
        "statistics": results.statistics,
        "results": [
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
