"""
Keycloak bearer-token checks for the testbed registry.

The rest of the orchestrator is unauthenticated; the testbed endpoints hand out
credentials, so they are not. The caller must present a Keycloak access token
issued by KEYCLOAK_ISSUER. With KEYCLOAK_ISSUER unset they answer 503 rather
than running open. Two levels of access:

  * testbed admins carry the realm role TESTBED_ADMIN_ROLE: every testbed, and
    the registry-wide actions (register, provision, rotate keys, deregister);
  * testbed owners are members of the Keycloak group TESTBED_GROUP_PREFIX/<slug>
    (one group per testbed, delivered in the token's `groups` claim): that
    testbed only, and not the registry-wide actions.
"""

import jwt
from fastapi import Depends, HTTPException
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer

from config import KEYCLOAK_ISSUER, TESTBED_ADMIN_ROLE, TESTBED_GROUP_PREFIX

_bearer = HTTPBearer(auto_error=False)
_jwks: jwt.PyJWKClient | None = None


def _jwks_client() -> jwt.PyJWKClient:
    global _jwks
    if _jwks is None:
        _jwks = jwt.PyJWKClient(f"{KEYCLOAK_ISSUER.rstrip('/')}/protocol/openid-connect/certs")
    return _jwks


def current_claims(creds: HTTPAuthorizationCredentials | None = Depends(_bearer)) -> dict:
    """Returns the validated token claims (so callers can record who acted)."""
    if not KEYCLOAK_ISSUER:
        raise HTTPException(status_code=503, detail="KEYCLOAK_ISSUER not configured; testbed endpoints are disabled")
    if creds is None:
        raise HTTPException(status_code=401, detail="Missing bearer token")
    try:
        key = _jwks_client().get_signing_key_from_jwt(creds.credentials)
        return jwt.decode(
            creds.credentials, key.key, algorithms=["RS256"],
            issuer=KEYCLOAK_ISSUER.rstrip("/"),
            # Keycloak access tokens carry aud=account (or the client); the issuer
            # and the role/groups below are what we rely on, not the audience.
            options={"verify_aud": False},
        )
    except Exception as e:
        raise HTTPException(status_code=401, detail=f"Invalid token: {e}")


def is_testbed_admin(claims: dict) -> bool:
    return TESTBED_ADMIN_ROLE in claims.get("realm_access", {}).get("roles", [])


def owned_slugs(claims: dict) -> set[str]:
    """Slugs of the testbeds whose group the token's holder belongs to. Keycloak writes a group as its
    path ('/testbeds/kul') or, with 'full group path' off, as its name ('kul'); both are accepted."""
    prefix = TESTBED_GROUP_PREFIX.strip("/")
    slugs = set()
    for group in claims.get("groups", []):
        parts = [p for p in str(group).split("/") if p]
        if prefix:
            if len(parts) == 2 and parts[0] == prefix:
                slugs.add(parts[1])
            elif len(parts) == 1:
                slugs.add(parts[0])
        elif len(parts) == 1:
            slugs.add(parts[0])
    return slugs


def require_testbed_admin(claims: dict = Depends(current_claims)) -> dict:
    """Registry-wide actions: only the testbed admin role."""
    if not is_testbed_admin(claims):
        raise HTTPException(status_code=403, detail=f"Requires the '{TESTBED_ADMIN_ROLE}' role")
    return claims


def require_testbed_user(claims: dict = Depends(current_claims)) -> dict:
    """Any signed-in user; what they may see is decided per testbed (the list is filtered)."""
    return claims


def require_testbed_access(slug: str, claims: dict = Depends(current_claims)) -> dict:
    """One testbed: an admin, or a member of that testbed's group."""
    if is_testbed_admin(claims) or slug in owned_slugs(claims):
        return claims
    raise HTTPException(status_code=403, detail=f"No access to testbed '{slug}'")
