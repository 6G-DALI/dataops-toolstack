"""A testbed's Keycloak group (/<TESTBED_GROUP_PREFIX>/<slug>), whose members own that testbed: creating it and
managing its members.

Uses a confidential client with a service account (client-credentials grant) in the same realm the UI logs in to;
see the README for the realm-management roles it needs. With no client configured, creating the group is skipped
(the group can be made by hand in Keycloak) and the member endpoints answer 503.
"""

from contextlib import contextmanager
from urllib.parse import quote

import httpx
from fastapi import HTTPException

from config import KEYCLOAK_ADMIN_CLIENT_ID, KEYCLOAK_ADMIN_CLIENT_SECRET, KEYCLOAK_ISSUER, TESTBED_GROUP_PREFIX


def configured() -> bool:
    return bool(KEYCLOAK_ISSUER and KEYCLOAK_ADMIN_CLIENT_ID and KEYCLOAK_ADMIN_CLIENT_SECRET)


def require_configured() -> None:
    if not configured():
        raise HTTPException(status_code=503, detail="KEYCLOAK_ADMIN_CLIENT_ID/_SECRET not set: testbed groups cannot be managed from here")


def _admin_base() -> str:
    # https://host/auth/realms/dspace -> https://host/auth/admin/realms/dspace
    return KEYCLOAK_ISSUER.rstrip("/").replace("/realms/", "/admin/realms/")


def _fail(what: str, e: Exception) -> HTTPException:
    detail = f"{e.response.status_code} {e.response.text[:200]}" if isinstance(e, httpx.HTTPStatusError) else str(e)
    return HTTPException(status_code=502, detail=f"Keycloak: {what} failed: {detail}")


class _Admin:
    """An authenticated Keycloak admin API session."""

    def __init__(self, client: httpx.Client):
        self.c = client
        self.base = _admin_base()
        token = client.post(f"{KEYCLOAK_ISSUER.rstrip('/')}/protocol/openid-connect/token", data={
            "grant_type": "client_credentials", "client_id": KEYCLOAK_ADMIN_CLIENT_ID,
            "client_secret": KEYCLOAK_ADMIN_CLIENT_SECRET})
        token.raise_for_status()
        self.headers = {"Authorization": f"Bearer {token.json()['access_token']}"}

    def get(self, path: str, **params):
        return self.c.get(f"{self.base}{path}", params=params or None, headers=self.headers)

    def post(self, path: str, json: dict):
        return self.c.post(f"{self.base}{path}", json=json, headers=self.headers)

    def put(self, path: str):
        return self.c.put(f"{self.base}{path}", headers=self.headers)

    def delete(self, path: str):
        return self.c.delete(f"{self.base}{path}", headers=self.headers)

    def top_level_group(self, name: str) -> str | None:
        r = self.get("/groups", search=name, exact="true")
        r.raise_for_status()
        return next((g["id"] for g in r.json() if g.get("name") == name and g.get("path") == f"/{name}"), None)

    def group_id(self, slug: str) -> str | None:
        """The testbed group's id, or None if it does not exist."""
        prefix = TESTBED_GROUP_PREFIX.strip("/")
        r = self.get(f"/group-by-path/{quote(f'{prefix}/{slug}' if prefix else slug, safe='/')}")
        if r.status_code == 404:
            return None
        r.raise_for_status()
        return r.json()["id"]

    def create_group(self, slug: str) -> str:
        """Create /<prefix>/<slug> (and /<prefix> if needed). Returns 'created' or 'exists'."""
        prefix = TESTBED_GROUP_PREFIX.strip("/")
        if not prefix:  # top-level group named by slug
            r = self.post("/groups", {"name": slug})
        else:
            parent = self.top_level_group(prefix)
            if parent is None:
                r = self.post("/groups", {"name": prefix})
                if r.status_code != 409:
                    r.raise_for_status()
                parent = self.top_level_group(prefix)
                if parent is None:
                    raise HTTPException(status_code=502, detail=f"Keycloak: group /{prefix} was not found after creating it")
            r = self.post(f"/groups/{parent}/children", {"name": slug})
        if r.status_code == 409:
            return "exists"
        r.raise_for_status()
        return "created"


@contextmanager
def _session(what: str):
    try:
        with httpx.Client(timeout=15) as c:
            yield _Admin(c)
    except httpx.HTTPError as e:
        raise _fail(what, e)


def ensure_group(slug: str) -> str:
    with _session(f"creating the group for '{slug}'") as kc:
        return kc.create_group(slug)


def _member(user: dict) -> dict:
    name = " ".join(p for p in (user.get("firstName"), user.get("lastName")) if p)
    return {"id": user["id"], "username": user.get("username"), "email": user.get("email"),
            "name": name or None, "enabled": user.get("enabled", True)}


def list_members(slug: str) -> list[dict]:
    with _session(f"listing the members of '{slug}'") as kc:
        gid = kc.group_id(slug)
        if gid is None:
            return []
        r = kc.get(f"/groups/{gid}/members", max="500")
        r.raise_for_status()
        return sorted((_member(u) for u in r.json()), key=lambda m: (m["email"] or m["username"] or "").lower())


def add_member(slug: str, email: str) -> dict:
    """Add the Keycloak user with this e-mail address to the testbed's group (created if missing)."""
    with _session(f"adding {email} to '{slug}'") as kc:
        r = kc.get("/users", email=email, exact="true")
        r.raise_for_status()
        users = r.json()
        if not users:
            raise HTTPException(status_code=404, detail=f"No Keycloak user with the e-mail {email}. They need to sign in to a DALI application once first.")
        if len(users) > 1:
            raise HTTPException(status_code=409, detail=f"More than one Keycloak user has the e-mail {email}")
        gid = kc.group_id(slug)
        if gid is None:
            kc.create_group(slug)
            gid = kc.group_id(slug)
        if gid is None:
            raise HTTPException(status_code=502, detail=f"Keycloak: the group for '{slug}' was not found after creating it")
        r = kc.put(f"/users/{users[0]['id']}/groups/{gid}")
        r.raise_for_status()
        return _member(users[0])


def remove_member(slug: str, user_id: str) -> None:
    with _session(f"removing a member of '{slug}'") as kc:
        gid = kc.group_id(slug)
        if gid is None:
            raise HTTPException(status_code=404, detail=f"The group for '{slug}' does not exist")
        r = kc.delete(f"/users/{user_id}/groups/{gid}")
        if r.status_code != 404:
            r.raise_for_status()
