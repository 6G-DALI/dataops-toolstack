"""
DAG: dali_dataspace_validate_dataset

Validates a single distribution of a dataset: retrieves its file from a
MinIO/S3 location, runs a configurable set of Great Expectations checks,
writes the validation results as a JSON file back to the same MinIO/S3
bucket, and publishes the results as dqv:QualityMeasurement nodes attached
to that distribution's own record in piveau (not the dataset).

A dataset can have more than one distribution — `asset_id` is what tells
this run which one it's validating, both for finding the right
dcat:Distribution node in piveau (see dali.dataspace.publish_quality_to_piveau)
and for locating its file in the Data Lake (see dali.datalake.download_dataset).

Trigger via dag_run.conf:
{
    "catalogue_id": "6g-dali-staging-eur",
    "dataset_id":   "6g-dali-staging-eur-exp-0004",
    "asset_id":     "ab7f9ca6-4f16-463b-8d0a-d246c4314e31",   # optional but recommended
    "expectations": [                                         # optional
        {"type": "expect_table_row_count_to_be_between", "min_value": 1},
        {"type": "expect_column_values_to_not_be_null",  "column": "timestamp"},
        {"type": "expect_column_values_to_not_be_null",  "column": "value"}
    ]
}

`asset_id` is the distribution's dali:assetId — the identifier
dataops-orchestrator generates at upload time and names the S3 object after
(see piveau_dataset_client.py's add_distribution and routers/datasets.py's
add_distribution endpoint). It is used two ways:
  - dali.datalake.download_dataset lists the bucket for
    "<dataset_id>/<asset_id>.*" to resolve the object's actual key (and thus
    its extension) without needing a separate lookup or DAG param for it.
  - dali.dataspace.publish_quality_to_piveau matches it against a
    dcat:Distribution node via dist_keys (dali/utils.py) — asset_id is
    embedded as the last path segment of dct:identifier, which is stable,
    unlike the node's own @id (piveau mints its own UUID for that on write,
    discarding whatever @id was submitted).
When omitted, both tasks fall back to the first dcat:Distribution node/object
found — correct only when the dataset has a single distribution.

The source object is read from  <dataset_id>/<asset_id>.<ext>  and results are
written back to  <dataset_id>/<asset_id>_<timestamp>.gx — both scoped to this
one distribution's file, so concurrent validation runs against different
distributions of the same dataset never collide.

Configuration (from the environment, not DAG params):
    DATASPACE_S3_CONN_ID  Airflow connection ID for the Data Space MinIO/S3
                          (type: Amazon Web Services; default "dali-dataspace").
                          Extra: {"endpoint_url": "http://<minio-host>:9000"},
                          Login: <access key>, Password: <secret key>.
    PIVEAU_API_KEY        X-API-Key for the piveau-hub-repo write API, used when
                          publishing quality annotations back to the catalogue.
"""

from __future__ import annotations

from datetime import datetime

from airflow.decorators import dag
from airflow.models.param import Param

from dali.dataspace import publish_quality_to_piveau
from dali.datalake import download_dataset, upload_results
from dali.validation import report_outcome, run_expectations


@dag(
    dag_id="dali_dataspace_validate_dataset",
    description="Validate a single distribution of a dataset with Great Expectations and store/publish the results",
    start_date=datetime(2025, 1, 1),
    schedule=None,
    catchup=False,
    tags=["6gdali", "dataspace", "great-expectations", "validation"],
    params={
        "catalogue_id": Param("", type="string", description="Catalogue ID"),
        "dataset_id":   Param("", type="string", description="Dataset ID"),
        "asset_id":     Param("", type="string", description="Distribution's dali:assetId — identifies which distribution to validate and which dcat:Distribution node quality results are published to"),
        "expectations": Param([], type="array",  description="List of GX expectation configs"),
    },
)
def dali_dataspace_validate_dataset():
    downloaded  = download_dataset()
    report      = run_expectations(csv_content=downloaded["content"], asset_title=downloaded["asset_title"])
    output_key  = upload_results(report=report)
    publish_quality_to_piveau(report=report)
    report_outcome(output_key=output_key, report=report)


dali_dataspace_validate_dataset()