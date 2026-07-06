"""
DAG: dali_dataspace_validate_dataset

Retrieves a dataset from a MinIO/S3 location, runs a configurable set of
Great Expectations checks, and writes the validation results as a JSON file
back to the same MinIO/S3 bucket.

Trigger via dag_run.conf:
{
    "catalogue_id": "6g-dali-staging-eur",
    "dataset_id":   "6g-dali-staging-eur-exp-0004",
    "asset_title":  "part-00000.csv",
    "expectations": [                                   # optional
        {"type": "expect_table_row_count_to_be_between", "min_value": 1},
        {"type": "expect_column_values_to_not_be_null",  "column": "timestamp"},
        {"type": "expect_column_values_to_not_be_null",  "column": "value"}
    ]
}

The source object is read from  <dataset_id>/<asset_title>  and results are
written back to  <dataset_id>/<asset_title_without_extension>_<timestamp>.gx

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
    description="Validate a MinIO/S3 dataset with Great Expectations and store results",
    start_date=datetime(2025, 1, 1),
    schedule=None,
    catchup=False,
    tags=["6gdali", "dataspace", "great-expectations", "validation"],
    params={
        "catalogue_id":   Param("", type="string", description="Catalogue ID"),
        "dataset_id":     Param("", type="string", description="Dataset ID"),
        "asset_title":    Param("", type="string", description="Asset Title"),
        "expectations":   Param([], type="array",  description="List of GX expectation configs"),
    },
)
def dali_dataspace_validate_dataset():
    csv_content = download_dataset()
    report      = run_expectations(csv_content=csv_content)
    output_key  = upload_results(report=report)
    publish_quality_to_piveau(report=report)
    report_outcome(output_key=output_key, report=report)


dali_dataspace_validate_dataset()
