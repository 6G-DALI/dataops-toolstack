"""
DAG: dali_dataspace_validate_dataset

Validates a single distribution of a dataset: retrieves its file from a
MinIO/S3 location, runs a configurable set of Great Expectations checks,
writes the validation results as a JSON file back to the same MinIO/S3
bucket, and publishes the results as dqv:QualityMeasurement nodes attached
to that distribution's own record in piveau (not the dataset).

A dataset can have more than one distribution — `distribution_id` is what
tells this run which one it's validating, both for finding the right
dcat:Distribution node in piveau (see dali.dataspace.publish_quality_to_piveau)
and for keeping repeat/manual triggers scoped to that same distribution
rather than "whichever distribution happens to be first" in the record.

Trigger via dag_run.conf:
{
    "catalogue_id":    "6g-dali-staging-eur",
    "dataset_id":      "6g-dali-staging-eur-exp-0004",
    "distribution_id": "1",                             # optional but recommended
    "expectations": [                                   # optional
        {"type": "expect_table_row_count_to_be_between", "min_value": 1},
        {"type": "expect_column_values_to_not_be_null",  "column": "timestamp"},
        {"type": "expect_column_values_to_not_be_null",  "column": "value"}
    ]
}

There is no `asset_title` param — the distribution's S3 object filename is
resolved by dali.dataspace.resolve_asset_title from `distribution_id` and
that distribution's dcat:mediaType (fetched from piveau), as
`{distribution_id}.{extension}`. This is the same rule
dataops-orchestrator's submit_dataset applies when it first uploads the
file (see piveau_dataset_client.py's FIRST_DISTRIBUTION_ID), so a freshly
submitted dataset's object key and this DAG's resolved asset_title always
agree without needing to pass the filename around as a separate value.
`distribution_id` is piveau's own distribution identifier (its @id's last
path segment, or dct:identifier). When omitted, both resolve_asset_title
and publish_quality_to_piveau fall back to the first dcat:Distribution node
found — correct only when the dataset has a single distribution.

The source object is read from  <dataset_id>/<asset_title>  and results are
written back to  <dataset_id>/<asset_title_without_extension>_<timestamp>.gx
— both scoped to this one distribution's file, so concurrent validation
runs against different distributions of the same dataset never collide.

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

from dali.dataspace import publish_quality_to_piveau, resolve_asset_title
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
        "catalogue_id":    Param("", type="string", description="Catalogue ID"),
        "dataset_id":      Param("", type="string", description="Dataset ID"),
        "distribution_id": Param("", type="string", description="Piveau distribution ID — identifies which distribution to validate and which dcat:Distribution node quality results are published to"),
        "expectations":    Param([], type="array",  description="List of GX expectation configs"),
    },
)
def dali_dataspace_validate_dataset():
    asset_title = resolve_asset_title()
    csv_content = download_dataset(asset_title=asset_title)
    report      = run_expectations(csv_content=csv_content, asset_title=asset_title)
    output_key  = upload_results(report=report)
    publish_quality_to_piveau(report=report)
    report_outcome(output_key=output_key, report=report)


dali_dataspace_validate_dataset()
