"""
Data Lake administration for the testbed registry: create a testbed's bucket and
a MinIO user whose policy is limited to that bucket.

Buckets go through boto3 (datalake_client). Users and policies are MinIO admin
operations that boto3 cannot do, so they use the `mc` client (baked into the
image, see Dockerfile) - credentials passed through MC_HOST_<alias>, never a
config file on disk. Needs DATASPACE_S3_ADMIN_* (or a DATASPACE_S3_* key that is
an admin key).
"""

import json
import os
import re
import secrets
import subprocess
import tempfile
from urllib.parse import quote, urlparse

from fastapi import HTTPException

import datalake_client
from config import (
    DATASPACE_S3_ADMIN_ACCESS_KEY,
    DATASPACE_S3_ADMIN_SECRET_KEY,
    DATASPACE_S3_ENDPOINT_URL,
    MC_BINARY,
)

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


def _mc(*args: str) -> str:
    if not (DATASPACE_S3_ENDPOINT_URL and DATASPACE_S3_ADMIN_ACCESS_KEY and DATASPACE_S3_ADMIN_SECRET_KEY):
        raise HTTPException(status_code=503, detail="Data Lake admin credentials (DATASPACE_S3_ADMIN_*) not configured")
    u = urlparse(DATASPACE_S3_ENDPOINT_URL)
    host = (f"{u.scheme}://{quote(DATASPACE_S3_ADMIN_ACCESS_KEY, safe='')}:"
            f"{quote(DATASPACE_S3_ADMIN_SECRET_KEY, safe='')}@{u.netloc}")
    try:
        r = subprocess.run([MC_BINARY, "--json", *args], capture_output=True, text=True, timeout=30,
                           env={**os.environ, "MC_HOST_dl": host})
    except FileNotFoundError:
        raise HTTPException(status_code=503, detail=f"'{MC_BINARY}' (MinIO client) not found in the orchestrator image")
    if r.returncode != 0:
        raise HTTPException(status_code=502, detail=f"mc {' '.join(args[:3])} failed: {(r.stdout + r.stderr)[:400]}")
    return r.stdout


def create_scoped_user(slug: str, bucket: str) -> tuple[str, str]:
    """Create a MinIO user restricted to `bucket`. Returns (access_key, secret_key)."""
    access_key = f"tb-{slug}-{secrets.token_hex(4)}"[:40]
    secret_key = secrets.token_urlsafe(30)
    policy = f"dali-testbed-{slug}"
    with tempfile.TemporaryDirectory() as d:
        path = os.path.join(d, "policy.json")
        with open(path, "w") as f:
            json.dump(policy_document(bucket), f)
        _mc("admin", "policy", "create", "dl", policy, path)
    _mc("admin", "user", "add", "dl", access_key, secret_key)
    _mc("admin", "policy", "attach", "dl", policy, "--user", access_key)
    return access_key, secret_key


def remove_user(access_key: str) -> None:
    _mc("admin", "user", "remove", "dl", access_key)
