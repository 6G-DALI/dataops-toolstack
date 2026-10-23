"""
What the s3-asset-monitor needs from the testbed registry: the Data Lake buckets to watch.

The monitor is a service, not a signed-in user, so this endpoint takes a shared token (X-Monitor-Token, compared with
MONITOR_API_TOKEN) instead of a Keycloak token. It answers 503 until the token is set, like the testbed endpoints do
without Keycloak.
"""

import hmac

from fastapi import APIRouter, Header, HTTPException

import testbed_store as store
from config import MONITOR_API_TOKEN

router = APIRouter(prefix="/monitor", tags=["Monitor"])


@router.get("/buckets")
def watched_buckets(x_monitor_token: str | None = Header(default=None)):
    """The Data Lake bucket of every provisioned testbed: what the monitor watches for new datasets and files.

    Requests, refused requests and testbeds whose provisioning has not completed are left out (their bucket may not
    exist yet), and so are the upload buckets of hosted testbeds, which the facility connector empties into the
    testbed's own bucket.
    """
    if not MONITOR_API_TOKEN:
        raise HTTPException(status_code=503, detail="MONITOR_API_TOKEN not configured; the monitor's bucket list is disabled")
    if x_monitor_token is None or not hmac.compare_digest(x_monitor_token.encode(), MONITOR_API_TOKEN.encode()):
        raise HTTPException(status_code=401, detail="Wrong or missing X-Monitor-Token")
    buckets = [t["bucket"] for t in store.list_testbeds() if t["status"] == "provisioned"]
    return {"buckets": buckets, "total": len(buckets)}
