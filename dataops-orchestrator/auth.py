"""
Keycloak bearer-token check for the admin-only endpoints (the testbed registry).

The rest of the orchestrator is unauthenticated; the testbed endpoints hand out
credentials, so they are not. The caller must present a Keycloak access token
issued by KEYCLOAK_ISSUER carrying the realm role TESTBED_ADMIN_ROLE. With
KEYCLOAK_ISSUER unset they answer 503 rather than running open.
"""

import jwt
from fastapi import Depends, HTTPException
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer

from config import KEYCLOAK_ISSUER, TESTBED_ADMIN_ROLE

_bearer = HTTPBearer(auto_error=False)
_jwks: jwt.PyJWKClient | None = None


def _jwks_client() -> jwt.PyJWKClient:
    global _jwks
    if _jwks is None:
        _jwks = jwt.PyJWKClient(f"{KEYCLOAK_ISSUER.rstrip('/')}/protocol/openid-connect/certs")
    return _jwks


def require_testbed_admin(creds: HTTPAuthorizationCredentials | None = Depends(_bearer)) -> dict:
    """Returns the validated token claims (so callers can record who acted)."""
    if not KEYCLOAK_ISSUER:
        raise HTTPException(status_code=503, detail="KEYCLOAK_ISSUER not configured; testbed endpoints are disabled")
    if creds is None:
        raise HTTPException(status_code=401, detail="Missing bearer token")
    try:
        key = _jwks_client().get_signing_key_from_jwt(creds.credentials)
        claims = jwt.decode(
            creds.credentials, key.key, algorithms=["RS256"],
            issuer=KEYCLOAK_ISSUER.rstrip("/"),
            # Keycloak access tokens carry aud=account (or the client); the issuer
            # and the role below are what we rely on, not the audience.
            options={"verify_aud": False},
        )
    except Exception as e:
        raise HTTPException(status_code=401, detail=f"Invalid token: {e}")
    if TESTBED_ADMIN_ROLE not in claims.get("realm_access", {}).get("roles", []):
        raise HTTPException(status_code=403, detail=f"Requires the '{TESTBED_ADMIN_ROLE}' role")
    return claims
