"""
Download links for the catalogue: a presigned GET URL for a distribution's file in the Data Lake.

A distribution's dcat:downloadURL is the file's raw S3 address (<endpoint>/<bucket>/<key>), which a browser cannot open:
the buckets are private. The catalogue's Download button asks here instead, with the user's token when signed in.

Who may download is whoever may read the dataset: the dataset is read from piveau with the caller's own token (or
anonymously), never with the orchestrator's API key, so piveau's access control decides. The URL must be one of that
dataset's dcat:downloadURL values, and its bucket the dataset's catalogue (each catalogue is a bucket of the same
name), so nothing else in the Data Lake can be signed through here.
"""

import posixpath
from urllib.parse import quote, unquote, urlsplit

import boto3
import httpx
from botocore.client import Config
from fastapi import APIRouter, HTTPException, Query, Request
from fastapi.concurrency import run_in_threadpool
from rdflib import Graph, URIRef
from rdflib.namespace import DCAT

from config import (
    DATALAKE_DOWNLOAD_ENDPOINT_URL,
    DATASPACE_S3_ACCESS_KEY,
    DATASPACE_S3_REGION,
    DATASPACE_S3_SECRET_KEY,
    DOWNLOAD_URL_TTL_SECONDS,
)
from piveau_dataset_client import PIVEAU_HUB_URL

router = APIRouter(prefix="/downloads", tags=["Downloads"])


def object_of(url: str, catalogue_id: str) -> str | None:
    """The object key a download URL points at in the catalogue's bucket, or None when it is not such a URL."""
    parts = urlsplit(url)
    if parts.scheme not in ("http", "https") or parts.query:
        return None
    path = unquote(parts.path).lstrip("/")
    bucket, _, key = path.partition("/")
    if bucket != catalogue_id or not key or key.endswith("/"):
        return None
    # No way out of the bucket, and nothing hidden (the connector's .datasets/ and .files/ mappings).
    if any(seg in ("", ".", "..") or seg.startswith(".") for seg in key.split("/")):
        return None
    return key


async def _visible_download_urls(dataset_id: str, catalogue_id: str, authorization: str | None) -> set[str]:
    """The dcat:downloadURL values of a dataset as the caller sees it in piveau; 404 when they may not see it."""
    if not PIVEAU_HUB_URL:
        raise HTTPException(status_code=503, detail="PIVEAU_HUB_URL not configured")
    headers = {"Accept": "text/turtle"}
    if authorization and authorization.lower().startswith("bearer "):
        headers["Authorization"] = authorization
    url = f"{PIVEAU_HUB_URL.rstrip('/')}/datasets/{quote(dataset_id, safe='')}"
    try:
        async with httpx.AsyncClient(timeout=20) as client:
            r = await client.get(url, params={"catalogue": catalogue_id}, headers=headers)
    except httpx.RequestError as e:
        raise HTTPException(status_code=502, detail=f"Could not reach the catalogue: {e}")
    if r.status_code == 401 and "Authorization" in headers:
        raise HTTPException(status_code=401, detail="The catalogue did not accept your sign-in; sign in again")
    if r.status_code in (401, 403, 404):
        raise HTTPException(status_code=404, detail="Dataset not found, or not visible to you")
    if r.status_code >= 400:
        raise HTTPException(status_code=502, detail=f"The catalogue answered {r.status_code}")
    g = Graph()
    try:
        g.parse(data=r.text, format="turtle")
    except Exception:
        raise HTTPException(status_code=502, detail="The catalogue's record could not be read")
    return {str(o) for o in g.objects(None, DCAT.downloadURL) if isinstance(o, URIRef)}


def _presign(bucket: str, key: str) -> str:
    if not DATALAKE_DOWNLOAD_ENDPOINT_URL or not DATASPACE_S3_ACCESS_KEY or not DATASPACE_S3_SECRET_KEY:
        raise HTTPException(status_code=503, detail="Data Lake downloads not configured (DATASPACE_S3_*)")
    s3 = boto3.client(
        "s3",
        endpoint_url=DATALAKE_DOWNLOAD_ENDPOINT_URL,
        aws_access_key_id=DATASPACE_S3_ACCESS_KEY,
        aws_secret_access_key=DATASPACE_S3_SECRET_KEY,
        config=Config(signature_version="s3v4", s3={"addressing_style": "path"}),
        region_name=DATASPACE_S3_REGION,
    )
    filename = posixpath.basename(key).replace('"', "")
    return s3.generate_presigned_url(
        "get_object",
        Params={"Bucket": bucket, "Key": key, "ResponseContentDisposition": f'attachment; filename="{filename}"'},
        ExpiresIn=DOWNLOAD_URL_TTL_SECONDS,
    )


@router.get("/presign")
async def presign_download(
    request: Request,
    catalogue: str = Query(..., description="The dataset's catalogue (its Data Lake bucket)"),
    dataset: str = Query(..., description="The dataset id"),
    url: str = Query(..., description="The distribution's dcat:downloadURL"),
):
    """A presigned URL for one distribution's file, valid for DOWNLOAD_URL_TTL_SECONDS.

    Works signed in or not: the dataset is read from the catalogue as the caller, so only a dataset they can see gives
    a link. 404 when the dataset is not visible to them or `url` is not one of its download URLs, 422 when `url` is not
    a file in the Data Lake (an external link opens as it is).
    """
    key = object_of(url, catalogue)
    if key is None:
        raise HTTPException(status_code=422, detail="Not a file in this catalogue's Data Lake bucket")
    if url not in await _visible_download_urls(dataset, catalogue, request.headers.get("authorization")):
        raise HTTPException(status_code=404, detail="No such download in this dataset")
    signed = await run_in_threadpool(_presign, catalogue, key)
    return {"url": signed, "expires_in": DOWNLOAD_URL_TTL_SECONDS, "filename": posixpath.basename(key)}
