"""
Uploads submitted dataset files to the Data Lake (Data Space MinIO/S3),
mirroring the endpoint/credentials Airflow's "dali-dataspace" connection
points at (see dali/datalake.py and dali_dataspace_validate_dataset).
"""

import boto3
from botocore.client import Config
from fastapi import HTTPException

from config import (
    DATASPACE_S3_ACCESS_KEY,
    DATASPACE_S3_ENDPOINT_URL,
    DATASPACE_S3_REGION,
    DATASPACE_S3_SECRET_KEY,
)


def _client():
    if not DATASPACE_S3_ENDPOINT_URL or not DATASPACE_S3_ACCESS_KEY or not DATASPACE_S3_SECRET_KEY:
        raise HTTPException(status_code=503, detail="Data Lake S3 (DATASPACE_S3_*) not configured")
    return boto3.client(
        "s3",
        endpoint_url=DATASPACE_S3_ENDPOINT_URL,
        aws_access_key_id=DATASPACE_S3_ACCESS_KEY,
        aws_secret_access_key=DATASPACE_S3_SECRET_KEY,
        config=Config(signature_version="s3v4", s3={"addressing_style": "path"}),
        region_name=DATASPACE_S3_REGION,
    )


def upload_dataset_file(catalogue_id: str, dataset_id: str, filename: str, content: bytes) -> str:
    """
    Upload a submitted dataset file to <catalogue_id>/<dataset_id>/<filename>
    in the Data Lake, matching the object-key convention every DataOps DAG
    already assumes (see input_key = f"{dataset_id}/{asset_title}").
    Returns the object key.
    """
    key = f"{dataset_id}/{filename}"
    client = _client()
    try:
        client.put_object(Bucket=catalogue_id, Key=key, Body=content)
    except Exception as e:
        raise HTTPException(status_code=502, detail=f"Could not upload to Data Lake bucket '{catalogue_id}': {e}")
    return key