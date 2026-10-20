"""A testbed's Keycloak group (/<TESTBED_GROUP_PREFIX>/<slug>), whose members own that testbed: creating it and
managing its members. Also the access to the testbed's catalogue: the role groups piveau creates for it.

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

    def catalogue_group(self, catalogue_id: str) -> str | None:
        """Id of the group piveau created for the catalogue, found through its `id` attribute (its name is the
        catalogue's title at creation, and may have been renamed since)."""
        r = self.get("/groups", q=f"id:{catalogue_id}", briefRepresentation="false", max="20")
        r.raise_for_status()
        return next((g["id"] for g in r.json() if g.get("attributes", {}).get("id") == [catalogue_id]), None)

    def role_groups(self, group_id: str) -> dict[str, str]:
        r = self.get(f"/groups/{group_id}/children", max="100")
        r.raise_for_status()
        return {g["name"]: g["id"] for g in r.json()}

    def user_by_email(self, email: str) -> dict:
        r = self.get("/users", email=email, exact="true")
        r.raise_for_status()
        users = r.json()
        if not users:
            raise HTTPException(status_code=404, detail=f"No Keycloak user with the e-mail {email}. They need to sign in to a DALI application once first.")
        if len(users) > 1:
            raise HTTPException(status_code=409, detail=f"More than one Keycloak user has the e-mail {email}")
        return users[0]

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
        user = kc.user_by_email(email)
        gid = kc.group_id(slug)
        if gid is None:
            kc.create_group(slug)
            gid = kc.group_id(slug)
        if gid is None:
            raise HTTPException(status_code=502, detail=f"Keycloak: the group for '{slug}' was not found after creating it")
        r = kc.put(f"/users/{user['id']}/groups/{gid}")
        r.raise_for_status()
        return _member(user)


def remove_member(slug: str, user_id: str) -> None:
    with _session(f"removing a member of '{slug}'") as kc:
        gid = kc.group_id(slug)
        if gid is None:
            raise HTTPException(status_code=404, detail=f"The group for '{slug}' does not exist")
        r = kc.delete(f"/users/{user_id}/groups/{gid}")
        if r.status_code != 404:
            r.raise_for_status()


# --- access to the testbed's catalogue ----------------------------------------------------------------------------
# With piveau access control on, piveau creates a group for every catalogue (tagged with the attribute id=<catalogue
# id>) with role groups under it, and grants each role its scopes on the catalogue. Membership of those role groups
# is what lets a user see a private catalogue's datasets and write to it. Levels as in the OOP Data Lab API: roles
# accumulate, every level includes the viewer's read. "role: Organization Admin" is left alone.

VIEWER, EXPERT, PUBLISHER = "role: Data Viewer", "role: Data Expert", "role: Data Publisher"
MEMBER_ROLES = (VIEWER, EXPERT, PUBLISHER)
LEVEL_ROLES = {"reader": (VIEWER,), "contributor": (VIEWER, EXPERT), "publisher": (VIEWER, EXPERT, PUBLISHER)}
LEVELS = tuple(LEVEL_ROLES)


def level_of(roles: set[str]) -> str:
    """The highest level a user's role groups amount to."""
    if PUBLISHER in roles:
        return "publisher"
    if EXPERT in roles:
        return "contributor"
    return "reader"


def _catalogue_roles(kc: _Admin, catalogue_id: str) -> dict[str, str]:
    gid = kc.catalogue_group(catalogue_id)
    if gid is None:
        raise HTTPException(status_code=409, detail=(
            f"No Keycloak group for catalogue '{catalogue_id}': piveau creates it when the catalogue is created, "
            "with access control on"))
    roles = kc.role_groups(gid)
    missing = [n for n in MEMBER_ROLES if n not in roles]
    if missing:
        raise HTTPException(status_code=502, detail=f"Keycloak: the group of catalogue '{catalogue_id}' lacks {missing}")
    return roles


def list_catalogue_members(catalogue_id: str) -> list[dict] | None:
    """Everyone in the catalogue's role groups, each with the level they amount to. None when piveau has no group
    for the catalogue (access control off, or the catalogue was created before it)."""
    with _session(f"listing who can access catalogue '{catalogue_id}'") as kc:
        if kc.catalogue_group(catalogue_id) is None:
            return None
        roles = _catalogue_roles(kc, catalogue_id)
        users: dict[str, dict] = {}
        held: dict[str, set[str]] = {}
        for name in MEMBER_ROLES:
            r = kc.get(f"/groups/{roles[name]}/members", max="500")
            r.raise_for_status()
            for u in r.json():
                users[u["id"]] = u
                held.setdefault(u["id"], set()).add(name)
        members = [{**_member(u), "level": level_of(held[i])} for i, u in users.items()]
        return sorted(members, key=lambda m: (m["email"] or m["username"] or "").lower())


def set_catalogue_member(catalogue_id: str, level: str, *, user_id: str | None = None, email: str | None = None) -> dict:
    """Give a user (by id, or by e-mail address) `level` on the catalogue: join the role groups it needs, leave the
    others."""
    if level not in LEVEL_ROLES:
        raise HTTPException(status_code=422, detail=f"Level must be one of {', '.join(LEVELS)}")
    with _session(f"giving {email or user_id} {level} access to catalogue '{catalogue_id}'") as kc:
        if email:
            user = kc.user_by_email(email)
        else:
            r = kc.get(f"/users/{user_id}")
            if r.status_code == 404:
                raise HTTPException(status_code=404, detail="No such Keycloak user")
            r.raise_for_status()
            user = r.json()
        roles = _catalogue_roles(kc, catalogue_id)
        for name in MEMBER_ROLES:
            if name in LEVEL_ROLES[level]:
                kc.put(f"/users/{user['id']}/groups/{roles[name]}").raise_for_status()
            else:
                d = kc.delete(f"/users/{user['id']}/groups/{roles[name]}")
                if d.status_code != 404:
                    d.raise_for_status()
        return {**_member(user), "level": level}


def remove_catalogue_member(catalogue_id: str, user_id: str) -> None:
    """Take the user out of the catalogue's reader, contributor and publisher role groups."""
    with _session(f"removing access to catalogue '{catalogue_id}'") as kc:
        roles = _catalogue_roles(kc, catalogue_id)
        for name in MEMBER_ROLES:
            r = kc.delete(f"/users/{user_id}/groups/{roles[name]}")
            if r.status_code != 404:
                r.raise_for_status()
