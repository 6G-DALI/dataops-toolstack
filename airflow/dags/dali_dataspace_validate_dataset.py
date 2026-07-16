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
    "asset_title":     "part-00000.csv",
    "distribution_id": "1",                             # optional but recommended
    "expectations": [                                   # optional
        {"type": "expect_table_row_count_to_be_between", "min_value": 1},
        {"type": "expect_column_values_to_not_be_null",  "column": "timestamp"},
        {"type": "expect_column_values_to_not_be_null",  "column": "value"}
    ]
}

`asset_title` is the distribution's filename, used purely to address the S3
object — it is unrelated to the distribution's dct:title in piveau (a
human-readable label), so it cannot be used to identify the distribution
node in piveau's graph. `distribution_id` is piveau's own distribution
identifier (its @id's last path segment, or dct:identifier) and is what's
used for that lookup. When omitted, publish_quality_to_piveau falls back to
the first dcat:Distribution node found — correct only when the dataset has
a single distribution.

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
        "catalogue_id":    Param("", type="string", description="Catalogue ID"),
        "dataset_id":      Param("", type="string", description="Dataset ID"),
        "asset_title":     Param("", type="string", description="Distribution's filename (S3 object key component)"),
        "distribution_id": Param("", type="string", description="Piveau distribution ID — identifies which distribution's dcat:Distribution node quality results are published to"),
        "expectations":    Param([], type="array",  description="List of GX expectation configs"),
    },
)
def dali_dataspace_validate_dataset():
    csv_content = download_dataset()
    report      = run_expectations(csv_content=csv_content)
    output_key  = upload_results(report=report)
    publish_quality_to_piveau(report=report)
    report_outcome(output_key=output_key, report=report)


dali_dataspace_validate_dataset()
