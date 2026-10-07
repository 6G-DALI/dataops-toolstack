"""
Data Lake administration for the testbed registry: create a testbed's bucket and
a MinIO user whose policy is limited to that bucket.

Buckets go through boto3 (datalake_client). Users and policies are MinIO admin
operations that boto3 cannot do, so they go through minio_admin (MinIO's admin
API, called directly). Needs DATASPACE_S3_ADMIN_* (or a DATASPACE_S3_* key that
is an admin key).
"""

import re
import secrets

from fastapi import HTTPException

import datalake_client
from config import (
    DATASPACE_S3_ADMIN_ACCESS_KEY,
    DATASPACE_S3_ADMIN_SECRET_KEY,
    DATASPACE_S3_ENDPOINT_URL,
    DATASPACE_S3_REGION,
)
from minio_admin import MinioAdmin, MinioAdminError

_BUCKET_RE = re.compile(r"^[a-z0-9][a-z0-9.-]{1,61}[a-z0-9]$")


def valid_bucket_name(name: str) -> bool:
    return bool(_BUCKET_RE.match(name)) and ".." not in name


def ensure_bucket(bucket: str) -> str:
    """Create the bucket if it does not exist. Returns 'created' or 'exists'."""
    client = datalake_client._client()
    try:
        client.head_bucket(Bucket=bucket)
        return "exists"
    except Exception:
        pass
    try:
        client.create_bucket(Bucket=bucket)
    except Exception as e:
        raise HTTPException(status_code=502, detail=f"Could not create bucket '{bucket}': {e}")
    return "created"


def policy_document(bucket: str) -> dict:
    """What a testbed's data-lake key may do: read, write and list its own bucket, nothing else."""
    return {
        "Version": "2012-10-17",
        "Statement": [
            {"Effect": "Allow", "Action": ["s3:ListBucket", "s3:GetBucketLocation"],
             "Resource": [f"arn:aws:s3:::{bucket}"]},
            {"Effect": "Allow", "Action": ["s3:GetObject", "s3:PutObject"],
             "Resource": [f"arn:aws:s3:::{bucket}/*"]},
        ],
    }


def _admin() -> MinioAdmin:
    if not (DATASPACE_S3_ENDPOINT_URL and DATASPACE_S3_ADMIN_ACCESS_KEY and DATASPACE_S3_ADMIN_SECRET_KEY):
        raise HTTPException(status_code=503, detail="Data Lake admin credentials (DATASPACE_S3_ADMIN_*) not configured")
    return MinioAdmin(DATASPACE_S3_ENDPOINT_URL, DATASPACE_S3_ADMIN_ACCESS_KEY, DATASPACE_S3_ADMIN_SECRET_KEY,
                      region=DATASPACE_S3_REGION)


def create_scoped_user(slug: str, bucket: str) -> tuple[str, str]:
    """Create a MinIO user restricted to `bucket`. Returns (access_key, secret_key)."""
    access_key = f"tb-{slug}-{secrets.token_hex(4)}"[:40]
    secret_key = secrets.token_urlsafe(30)
    policy = f"dali-testbed-{slug}"
    admin = _admin()
    try:
        admin.add_canned_policy(policy, policy_document(bucket))
        admin.add_user(access_key, secret_key)
        admin.attach_policy(policy, access_key)
    except MinioAdminError as e:
        raise HTTPException(status_code=502, detail=f"MinIO admin call failed: {e}")
    return access_key, secret_key


def remove_user(access_key: str) -> None:
    try:
        _admin().remove_user(access_key)
    except MinioAdminError as e:
        raise HTTPException(status_code=502, detail=f"MinIO admin call failed: {e}")


def remove_policy(slug: str) -> None:
    """Drop the testbed's bucket-scoped policy. Best effort: a policy that is already gone is not an error."""
    try:
        _admin().remove_canned_policy(f"dali-testbed-{slug}")
    except (MinioAdminError, HTTPException):
        pass


def count_objects(bucket: str, limit: int = 10000) -> tuple[int, bool]:
    """(objects in the bucket, whether counting stopped at `limit`). (0, False) for a missing bucket."""
    client = datalake_client._client()
    count = 0
    try:
        for page in client.get_paginator("list_objects_v2").paginate(Bucket=bucket):
            count += len(page.get("Contents", []))
            if count >= limit:
                return count, True
    except client.exceptions.NoSuchBucket:
        return 0, False
    except Exception as e:
        raise HTTPException(status_code=502, detail=f"Could not list bucket '{bucket}': {e}")
    return count, False


def delete_bucket(bucket: str) -> dict:
    """Delete every object (and version) in the bucket, then the bucket itself. Irreversible."""
    client = datalake_client._client()
    deleted = 0
    try:
        client.head_bucket(Bucket=bucket)
    except Exception:
        return {"status": "not_found", "objects_deleted": 0}
    try:
        for page in client.get_paginator("list_object_versions").paginate(Bucket=bucket):
            doomed = [{"Key": v["Key"], "VersionId": v["VersionId"]}
                      for v in page.get("Versions", []) + page.get("DeleteMarkers", [])]
            for i in range(0, len(doomed), 1000):  # delete_objects caps at 1000 keys per call
                client.delete_objects(Bucket=bucket, Delete={"Objects": doomed[i:i + 1000], "Quiet": True})
            deleted += len(doomed)
        client.delete_bucket(Bucket=bucket)
    except Exception as e:
        raise HTTPException(status_code=502, detail=f"Could not delete bucket '{bucket}': {e}")
    return {"status": "deleted", "objects_deleted": deleted}
