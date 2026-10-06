"""
DAG: dali_dataspace_validate_dataset

Validates *and* processes a single distribution of a dataset in one run: it
retrieves the file over EDC (a contract negotiation and transfer against the
provider connector, see dali.datalake.download_dataset_edc, rather than a
direct read out of the bucket), then puts it through two independent quality
regimes and reports their combined verdict.

  1. A configurable Great Expectations suite — the checks a submitter (or
     piveau's own column metadata) asks for. See `expectations` below.
  2. The WaveStitchPlus minimal DataOps pipeline (minimal_dataops.run_pipeline,
     vendored into plugins/ — see plugins/VENDORED.md — and wrapped by
     dali.processing.run_dataops_pipeline): conservative cleaning, profiling,
     its *own* auto-generated GX suite, pandera schema validation, per-issue
     remediation and imputation.

Both used to be separate DAGs, each pulling the same file over EDC and each
judging it alone, with only the first one's findings ever reaching the
catalogue. dali.validation.merge_quality_report now folds every check from both
into a single report — one entry per check, plus a total across all of them —
which is what gets stored, published to piveau, and rendered in dataops-ui.
Great Expectations is *reported* as one execution: an expectation both suites
declare becomes one dqv:QualityMeasurement, and nothing records which suite
declared it.

It is not yet *run* as one execution. The two suites remain two GX runs in two
tasks over two frames — run_expectations sees the raw file, the pipeline's suite
sees the soft-cleaned one — so a folded check ANDs two verdicts that were not
measured over the same data. See the TODO(quality) note above
merge_quality_report in dali/validation.py for why unifying them is a decision
about which frame to validate rather than a refactor.

A format check runs before either regime: GX expectations validate *values* in
a DataFrame, and the pipeline's first act is to read one, so neither can run
against a file that is not the CSV/TSV/JSON Lines it claims to be (see
dali.validation.validate_file_format). When it fails, the GX suite and the
pipeline are both skipped and the report reflects just that failure.

Trigger via dag_run.conf:
{
    "catalogue_id":  "6g-dali-staging-eur",
    "dataset_id":    "6g-dali-staging-eur-exp-0004",
    "asset_id":      "ab7f9ca6-4f16-463b-8d0a-d246c4314e31",   # required
    "expectations":  [                                         # optional
        {"type": "expect_table_row_count_to_be_between", "min_value": 1},
        {"type": "expect_column_values_to_not_be_null",  "column": "timestamp"},
        {"type": "expect_column_values_to_not_be_null",  "column": "value"}
    ],
    "timestamp_col": "timestamp",                              # optional
    "validation":    {"mode": "auto"},                         # optional
    "imputation":    {"method": "linear"}                      # optional
}

`asset_id` is the distribution's dali:assetId — the EDC asset's own @id, the
identifier dataops-orchestrator registers each distribution under (see
edc_client.register_asset), so piveau and EDC agree on one identifier per
distribution. A dataset can have more than one distribution, and this is what
tells the run which one it is working on. It is therefore REQUIRED, and is used
three ways:
  - dali.datalake.download_dataset_edc filters the provider's EDC catalogue by
    it, then reads the matched entry's own name/contenttype properties to
    recover the filename (and thus the extension) the format check needs.
  - dali.dataspace.publish_quality_to_piveau matches it against a
    dcat:Distribution node via dist_keys (dali/utils.py) — asset_id is embedded
    as the last path segment of dct:identifier, which is stable, unlike the
    node's own @id (piveau mints its own UUID for that on write, discarding
    whatever @id was submitted).
  - it scopes every uploaded object's key.
When omitted, publish_quality_to_piveau falls back to the first
dcat:Distribution node found — correct only when the dataset has a single
distribution — while download_dataset_edc has nothing to look up and fails.

`expectations` drives regime 1 only. Left empty, dali.validation.run_expectations
builds a suite from piveau's own schema:variableMeasured for the distribution
(expect_column_to_exist plus expect_column_values_to_not_be_null per column),
on top of a row-count check.

`timestamp_col` drives regime 2, and matters more than it looks: the pipeline's
validation mode is "auto" by default, and it only takes the time-series path —
gap detection, timestamp ordering, the imputation handoff — when it can resolve
a timestamp column. Left unset, dali.processing.guess_timestamp_column picks one
from the column names before the run starts (the pipeline's own detection looks
only at column *contents*, in column order, so it can settle on the wrong column
or discard a step index outright). The guess is refused unless the named column
actually holds timestamps, in which case detection is left in charge exactly as
before. Set this param to override both.

`validation` and `imputation` are passed straight through to run_pipeline as its
validation_config / imputation_config. Omitted, they fall back to
dali.processing's defaults, which mirror WaveStitchPlus/config/dataops.yaml
(mode auto, no expected columns or numeric bounds, outlier_q 0.01) except that
the imputation bundle *is* built and filled — see below.
Recognised validation keys: mode (auto|time_series|tabular|none),
expected_columns, numeric_bounds, missing_threshold, require_timestamp_unique,
require_timestamp_monotonic, allow_step_index_timestamp, outlier_q,
outlier_mostly, recheck_after_remediation.

The run also *performs* the imputation, which the pipeline itself never does —
its handoff only regularizes the timeline into a bundle and records which app an
external orchestrator would invoke ("The pipeline never runs imputation",
minimal_dataops._build_handoff). dali.processing.impute_prepared_bundle runs
dataops.imputation_runner over that bundle in-process and uploads the filled
splits, so a run ends with imputed values rather than an invoke_hint. This
matters in time_series mode especially: remediation there clips outliers but
records gaps as "deferred_to_imputation" without filling them, so without this
step a successful run produced nothing imputed at all.

The default is darts/linear via the runner's dependency-free pandas engine,
which reproduces Darts' MissingValuesFiller exactly. Linear rather than the
runner's own "nearest" default because pandas implements linear itself while the
other methods delegate to SciPy, which the Airflow image does not carry. Pass
`{"imputation": {"impute": false}}` for the bundle without filling it, or
`{"imputation": {"method": "cubic"}}` once scipy is installed.

What a run leaves behind, all in the catalogue bucket and all scoped to this one
distribution, so concurrent runs over different distributions never collide:

    <dataset_id>/<asset_id>_<timestamp>.gx                  the merged quality report
    <dataset_id>/<asset_id>_<timestamp>_raw.csv             as transferred over EDC
    <dataset_id>/<asset_id>_<timestamp>_soft_cleaned.csv    before per-issue remediation
    <dataset_id>/<asset_id>_<timestamp>_remediated.csv      the pipeline's output
    <dataset_id>/<asset_id>_<timestamp>_report.json         the pipeline's report, with
                                                            the merged quality report
                                                            nested under "dali_quality"
    <dataset_id>/<asset_id>_<timestamp>_imputed_train.csv
    <dataset_id>/<asset_id>_<timestamp>_imputed_test.csv

The raw frame goes up with the rest because those bytes arrived over EDC from
the provider's connector and are not otherwise in this bucket — so the report's
raw → soft-cleaned → remediated lineage can be read end to end.

A quality failure — of either regime — does not fail this DAG. run_pipeline
writes every artifact before re-raising, so the error is captured, the artifacts
are uploaded anyway, and the outcome is logged by report_outcome and
report_pipeline_outcome. Failing the run would lose exactly the report that
explains why it failed.

The pipeline needs `pandera`, which the stock Airflow image does not carry — it
is listed in airflow/base/requirements.txt, so the image has to be rebuilt
before this DAG can run.

Configuration (from the environment, not DAG params):
    DATASPACE_S3_CONN_ID  Airflow connection ID for the Data Space MinIO/S3
                          (type: Amazon Web Services; default "dali-dataspace").
                          Extra: {"endpoint_url": "http://<minio-host>:9000"},
                          Login: <access key>, Password: <secret key>.
    DATAOPS_S3_CONN_ID    Airflow connection ID for the DataOps MinIO/S3 the
                          EDC transfer stages the file into (default
                          "dali-dataops").
    EDC_CONSUMER_DOMAIN / EDC_CONSUMER_MANAGEMENT_PORT
                          Our own consumer connector's management API — every
                          catalog/negotiation/transfer call is made against it.
    EDC_PROVIDER_DOMAIN / EDC_PROVIDER_PROTOCOL_PORT
                          The provider connector's DSP address, used as
                          counterPartyAddress (see dali.utils).
    EDC_POLL_INTERVAL / EDC_POLL_TIMEOUT
                          How often, and for how long, the negotiation and
                          transfer are polled (default 3s, 120s).
    PIVEAU_API_KEY        X-API-Key for the piveau-hub-repo write API, used when
                          publishing quality annotations back to the catalogue.
"""

from __future__ import annotations

from datetime import datetime

from airflow.decorators import dag
from airflow.models.param import Param

from dali.dataspace import publish_quality_to_piveau
from dali.datalake import download_dataset_edc, upload_artifacts, upload_results
from dali.processing import cleanup_workdir, report_pipeline_outcome, run_dataops_pipeline
from dali.validation import (
    merge_quality_report,
    report_outcome,
    run_expectations,
    validate_file_format,
)


@dag(
    dag_id="dali_dataspace_validate_dataset",
    description="Validate and remediate a single distribution with Great Expectations, pandera and the WaveStitchPlus DataOps pipeline, and publish the combined quality report",
    start_date=datetime(2025, 1, 1),
    schedule=None,
    catchup=False,
    tags=["6gdali", "dataspace", "dataops", "great-expectations", "pandera", "validation", "wavestitchplus"],
    params={
        "catalogue_id":  Param("", type="string", description="Catalogue ID — the bucket results and artifacts are written to"),
        "dataset_id":    Param("", type="string", description="Dataset ID — the prefix results and artifacts are written under"),
        "asset_id":      Param("", type="string", description="Distribution's dali:assetId — the EDC asset @id to negotiate and transfer, and the dcat:Distribution node quality results are published to. Required."),
        "expectations":  Param([], type="array",  description="List of GX expectation configs. Empty, a suite is built from piveau's column metadata."),
        "timestamp_col": Param(None, type=["null", "string"], description="Timestamp column. Drives the pipeline's time-series validation. Omitted, it is guessed from the column names, then from the data."),
        "validation":    Param({}, type="object", description="run_pipeline's validation_config: mode, expected_columns, numeric_bounds, missing_threshold, outlier_q, …"),
        "imputation":    Param({}, type="object", description="Imputation: build_bundle, impute, lib, method, prepared_dir. Defaults to building the bundle and filling it with darts/linear."),
    },
)
def dali_dataspace_validate_dataset():
    downloaded   = download_dataset_edc()
    format_check = validate_file_format(
        file_content=downloaded["content"], asset_title=downloaded["asset_title"]
    )

    # The two quality regimes are independent and both take the format check —
    # they run in parallel off the single EDC transfer, which is the point of
    # merging them into one DAG.
    gx_report = run_expectations(
        file_content=downloaded["content"],
        asset_title=downloaded["asset_title"],
        format_check=format_check,
    )
    pipeline  = run_dataops_pipeline(
        file_content=downloaded["content"],
        asset_title=downloaded["asset_title"],
        format_check=format_check,
    )

    merged     = merge_quality_report(gx_report=gx_report, pipeline=pipeline)
    artifacts  = upload_artifacts(pipeline=pipeline, quality=merged)
    output_key = upload_results(report=merged)

    publish_quality_to_piveau(report=merged)
    report_outcome(output_key=output_key, report=merged)
    report_pipeline_outcome(pipeline=pipeline, uploaded=artifacts)

    # Explicit edge: cleanup takes only the path, so nothing would otherwise
    # order it after the upload that still has to read those files off disk.
    artifacts >> cleanup_workdir(workdir=pipeline["workdir"])


dali_dataspace_validate_dataset()
