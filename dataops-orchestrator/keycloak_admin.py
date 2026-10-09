"""Creates a testbed's Keycloak group (/<TESTBED_GROUP_PREFIX>/<slug>), whose members own that testbed.

Uses a confidential client with a service account (client-credentials grant) in the same realm the UI logs in to;
see README for the realm-management roles it needs. With no client configured the step is skipped, not failed:
the groups can then be created by hand in Keycloak.
"""

import httpx
from fastapi import HTTPException

from config import KEYCLOAK_ADMIN_CLIENT_ID, KEYCLOAK_ADMIN_CLIENT_SECRET, KEYCLOAK_ISSUER, TESTBED_GROUP_PREFIX


def configured() -> bool:
    return bool(KEYCLOAK_ISSUER and KEYCLOAK_ADMIN_CLIENT_ID and KEYCLOAK_ADMIN_CLIENT_SECRET)


def _admin_base() -> str:
    # https://host/auth/realms/dspace -> https://host/auth/admin/realms/dspace
    return KEYCLOAK_ISSUER.rstrip("/").replace("/realms/", "/admin/realms/")


def _fail(what: str, e: Exception) -> HTTPException:
    detail = f"{e.response.status_code} {e.response.text[:200]}" if isinstance(e, httpx.HTTPStatusError) else str(e)
    return HTTPException(status_code=502, detail=f"Keycloak: {what} failed: {detail}")


def _top_level_group(c: httpx.Client, base: str, headers: dict, name: str) -> str | None:
    r = c.get(f"{base}/groups", params={"search": name, "exact": "true"}, headers=headers)
    r.raise_for_status()
    return next((g["id"] for g in r.json() if g.get("name") == name and g.get("path") == f"/{name}"), None)


def ensure_group(slug: str) -> str:
    """Create /<prefix>/<slug> (and /<prefix> if needed). Returns 'created' or 'exists'."""
    base = _admin_base()
    prefix = TESTBED_GROUP_PREFIX.strip("/")
    try:
        with httpx.Client(timeout=15) as c:
            token = c.post(f"{KEYCLOAK_ISSUER.rstrip('/')}/protocol/openid-connect/token", data={
                "grant_type": "client_credentials", "client_id": KEYCLOAK_ADMIN_CLIENT_ID,
                "client_secret": KEYCLOAK_ADMIN_CLIENT_SECRET})
            token.raise_for_status()
            headers = {"Authorization": f"Bearer {token.json()['access_token']}"}

            if not prefix:  # top-level group named by slug
                r = c.post(f"{base}/groups", json={"name": slug}, headers=headers)
                if r.status_code == 409:
                    return "exists"
                r.raise_for_status()
                return "created"

            parent = _top_level_group(c, base, headers, prefix)
            if parent is None:
                r = c.post(f"{base}/groups", json={"name": prefix}, headers=headers)
                if r.status_code != 409:
                    r.raise_for_status()
                parent = _top_level_group(c, base, headers, prefix)
                if parent is None:
                    raise HTTPException(status_code=502, detail=f"Keycloak: group /{prefix} was not found after creating it")
            r = c.post(f"{base}/groups/{parent}/children", json={"name": slug}, headers=headers)
            if r.status_code == 409:
                return "exists"
            r.raise_for_status()
            return "created"
    except httpx.HTTPError as e:
        raise _fail(f"creating group /{prefix}/{slug}" if prefix else f"creating group {slug}", e)
