"""
DAG: dali_dataspace_process_dataset

Runs WaveStitchPlus' minimal DataOps pipeline over a single distribution of a
dataset: pulls the file over EDC (contract negotiation + transfer, see
dali.datalake.download_dataset_edc), cleans / validates / profiles / remediates
it (minimal_dataops.py's run_pipeline — vendored into plugins/ from the
WaveStitchPlus checkout, see plugins/VENDORED.md — wrapped by
dali.processing.run_dataops_pipeline), then uploads the pipeline's artifacts
back to the same Data Space bucket the distribution lives in.

Six artifacts are produced and uploaded — four here, plus the two imputed
splits described further down. The pipeline always writes the
conservatively *soft-cleaned* frame alongside the remediated one, and the raw
frame goes up with them so the report's raw → soft-cleaned → remediated lineage
can be read end to end — those bytes arrived over EDC from the provider's
connector, so they are not otherwise in this bucket:

    <dataset_id>/<asset_id>_<timestamp>_raw.csv            as transferred over EDC
    <dataset_id>/<asset_id>_<timestamp>_soft_cleaned.csv   before per-issue remediation
    <dataset_id>/<asset_id>_<timestamp>_remediated.csv     the pipeline's output
    <dataset_id>/<asset_id>_<timestamp>_report.json        cleaning/quality/validation report

Trigger via dag_run.conf:
{
    "catalogue_id":  "6g-dali-staging-eur",
    "dataset_id":    "6g-dali-staging-eur-exp-0004",
    "asset_id":      "ab7f9ca6-4f16-463b-8d0a-d246c4314e31",   # required
    "timestamp_col": "timestamp",                              # optional
    "validation":    {"mode": "auto"},                         # optional
    "imputation":    {"method": "linear"}                       # optional
}

`asset_id` is the distribution's dali:assetId — the EDC asset's own @id, the
identifier dataops-orchestrator registers each distribution under (see
edc_client.register_asset). It selects which distribution is pulled, and scopes
the uploaded artifacts' keys. See dali_dataspace_validate_dataset for the
identifier's full story.

`timestamp_col` matters more than it looks: the pipeline's validation mode is
"auto" by default, and it only takes the time-series path — gap detection,
timestamp ordering, the imputation handoff — when it can resolve a timestamp
column. Left unset, dali.processing.guess_timestamp_column picks one from the
column names before the run starts (the pipeline's own detection looks only at
column *contents*, in column order, so it can settle on the wrong column or
discard a step index outright). The guess is refused unless the named column
actually holds timestamps, in which case detection is left in charge exactly as
before. Set this param to override both.

The run also *performs* the imputation, which the pipeline itself never does —
its handoff only regularizes the timeline into a bundle and records which app an
external orchestrator would invoke ("The pipeline never runs imputation",
minimal_dataops._build_handoff). dali.processing.impute_prepared_bundle runs
dataops.imputation_runner over that bundle in-process and uploads the filled
splits, so a run ends with imputed values rather than an invoke_hint. This
matters in time_series mode especially: remediation there clips outliers but
records gaps as "deferred_to_imputation" without filling them, so without this
step a successful run produced nothing imputed at all.

    <dataset_id>/<asset_id>_<timestamp>_imputed_train.csv
    <dataset_id>/<asset_id>_<timestamp>_imputed_test.csv

The default is darts/linear via the runner's dependency-free pandas engine,
which reproduces Darts' MissingValuesFiller exactly. Linear rather than the
runner's own "nearest" default because pandas implements linear itself while the
other methods delegate to SciPy, which the Airflow image does not carry. Pass
`{"imputation": {"impute": false}}` for the bundle without filling it, or
`{"imputation": {"method": "cubic"}}` once scipy is installed.

`validation` and `imputation` are passed straight through to run_pipeline as
its validation_config / imputation_config. Omitted, they fall back to
dali.processing's defaults, which mirror WaveStitchPlus/config/dataops.yaml
(mode auto, no expected columns or numeric bounds, outlier_q 0.01) except that
the imputation bundle *is* built and filled — see above.
Recognised validation keys: mode (auto|time_series|tabular|none),
expected_columns, numeric_bounds, missing_threshold, require_timestamp_unique,
require_timestamp_monotonic, allow_step_index_timestamp, outlier_q,
outlier_mostly, recheck_after_remediation.

A validation failure does not fail this DAG. run_pipeline writes every artifact
before re-raising, so the error is captured, the artifacts are uploaded anyway,
and the outcome is logged by report_pipeline_outcome — the same way
dali_dataspace_validate_dataset reports a failed expectation suite.

The pipeline needs `pandera`, which the stock Airflow image does not carry —
it is listed in airflow/base/requirements.txt, so the image has to be rebuilt
before this DAG can run.

Configuration (from the environment, not DAG params):
    DATASPACE_S3_CONN_ID  Airflow connection ID for the Data Space MinIO/S3 the
                          artifacts are uploaded to (default "dali-dataspace").
    DATAOPS_S3_CONN_ID    Airflow connection ID for the DataOps MinIO/S3 the EDC
                          transfer stages the download into (default "dali-dataops").
    EDC_CONSUMER_DOMAIN / EDC_CONSUMER_MANAGEMENT_PORT
    EDC_PROVIDER_DOMAIN / EDC_PROVIDER_PROTOCOL_PORT
    EDC_POLL_INTERVAL / EDC_POLL_TIMEOUT
                          The EDC endpoints and polling knobs (see dali.utils).
"""

from __future__ import annotations

from datetime import datetime

from airflow.decorators import dag
from airflow.models.param import Param

from dali.datalake import download_dataset_edc, upload_artifacts
from dali.processing import cleanup_workdir, report_pipeline_outcome, run_dataops_pipeline


@dag(
    dag_id="dali_dataspace_process_dataset",
    description="Clean, validate and remediate a distribution with the WaveStitchPlus DataOps pipeline, and store the artifacts",
    start_date=datetime(2025, 1, 1),
    schedule=None,
    catchup=False,
    tags=["6gdali", "dataspace", "dataops", "cleaning", "wavestitchplus"],
    params={
        "catalogue_id":  Param("", type="string", description="Catalogue ID — the bucket artifacts are uploaded to"),
        "dataset_id":    Param("", type="string", description="Dataset ID — the prefix artifacts are uploaded under"),
        "asset_id":      Param("", type="string", description="Distribution's dali:assetId — the EDC asset @id to negotiate and transfer. Required."),
        "timestamp_col": Param(None, type=["null", "string"], description="Timestamp column. Drives time-series validation. Omitted, it is guessed from the column names, then from the data."),
        "validation":    Param({}, type="object", description="run_pipeline's validation_config: mode, expected_columns, numeric_bounds, missing_threshold, outlier_q, …"),
        "imputation":    Param({}, type="object", description="Imputation: build_bundle, impute, lib, method, prepared_dir. Defaults to building the bundle and filling it with darts/linear."),
    },
)
def dali_dataspace_process_dataset():
    downloaded = download_dataset_edc()
    pipeline   = run_dataops_pipeline(
        file_content=downloaded["content"], asset_title=downloaded["asset_title"]
    )
    uploaded   = upload_artifacts(pipeline=pipeline)
    report_pipeline_outcome(pipeline=pipeline, uploaded=uploaded)
    # Explicit edge: cleanup takes only the path, so nothing would otherwise
    # order it after the upload that still has to read those files off disk.
    uploaded >> cleanup_workdir(workdir=pipeline["workdir"])


dali_dataspace_process_dataset()
