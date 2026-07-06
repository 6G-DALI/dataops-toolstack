"""
DAG: dali_demo_op

Demo DAG that retrieves a dataset from the 6G-DALI Data Space via EDC
connector-to-connector transfer and prints each row of the CSV to the logs.

Trigger via dag_run.conf:
{
    "dataset_id":       "6g-dali-staging-eur-exp-0004",
    "asset_title":      "part-00000.csv",
    "catalogue_id":     "6g-dali-staging-eur",
    "provider_id":      "provider"                      # optional, default "provider"
}

The provider EDC pushes the asset to a presigned PUT URL on the DataOps S3;
the transferred object lands at  <catalogue_id>/<dataset_id>/<asset_title>.

Configuration (from the environment, not DAG params):
    DATAOPS_S3_CONN_ID          Airflow connection ID for the DataOps MinIO/S3
                                (type: Amazon Web Services; default "dali-dataspace").
    EDC_CONSUMER_URL            Base URL of the DataOps EDC consumer
                                connector's management API.
    EDC_PROVIDER_PROTOCOL_URL   Base URL of the 6G-DALI provider EDC
                                connector's protocol (DSP) API — fixed, not
                                configurable per DAG run (see dali.datalake).
"""

from __future__ import annotations

from datetime import datetime

from airflow.decorators import dag, task
from airflow.models.param import Param

from dali.datalake import download_dataset_edc


@task
def print_csv_rows(csv_content: str) -> None:
    """Print each row of the CSV with its line number."""
    lines = csv_content.splitlines()
    print(f"[dali_demo_op] {len(lines)} rows total")
    for i, line in enumerate(lines):
        print(f"[dali_demo_op] row {i:>4}: {line}")


@dag(
    dag_id="dali_demo_op",
    description="Demo: retrieve a dataset via EDC and print its CSV rows",
    start_date=datetime(2025, 1, 1),
    schedule=None,
    catchup=False,
    tags=["6gdali", "dataops", "demo", "edc"],
    params={
        "catalogue_id":     Param("",              type="string", description="S3 bucket / catalogue ID"),
        "dataset_id":       Param("",              type="string", description="Dataset folder within the bucket"),
        "asset_title":      Param("",              type="string", description="Filename of the target object"),
    },
)
def dali_demo_op():
    csv_content = download_dataset_edc()
    print_csv_rows(csv_content)


dali_demo_op()
