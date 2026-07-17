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
    in the Data Lake, matching the object-key convention the validate DAG
    resolves via an S3 prefix listing on dali:assetId (see
    dali.datalake.download_dataset and piveau_dataset_client.py's
    extension_for_media_type/add_distribution — `filename` here should be
    built the same way, i.e. "{asset_id}.{ext}", not an arbitrary original
    upload name). Returns the object key.
    """
    key = f"{dataset_id}/{filename}"
    client = _client()
    try:
        client.put_object(Bucket=catalogue_id, Key=key, Body=content)
    except Exception as e:
        raise HTTPException(status_code=502, detail=f"Could not upload to Data Lake bucket '{catalogue_id}': {e}")
    return key


def delete_objects_by_prefix(catalogue_id: str, prefix: str) -> list[str]:
    """Delete every object under `prefix` in the Data Lake bucket. Used both
    for a single distribution (prefix "{dataset_id}/{asset_id}." — matches
    the exact object regardless of extension, mirroring
    dali.datalake.download_dataset's own S3 prefix listing) and for a whole
    dataset (prefix "{dataset_id}/" — also sweeps up GX result files from
    dali.datalake.upload_results, which aren't tied to any one asset_id).
    Returns the keys that were actually deleted.
    """
    client = _client()
    try:
        keys = [obj["Key"] for page in client.get_paginator("list_objects_v2").paginate(
            Bucket=catalogue_id, Prefix=prefix
        ) for obj in page.get("Contents", [])]
        for i in range(0, len(keys), 1000):  # delete_objects caps at 1000 keys per call
            batch = keys[i:i + 1000]
            client.delete_objects(Bucket=catalogue_id, Delete={"Objects": [{"Key": k} for k in batch]})
    except Exception as e:
        raise HTTPException(status_code=502, detail=f"Could not delete objects under '{prefix}' in bucket '{catalogue_id}': {e}")
    return keys