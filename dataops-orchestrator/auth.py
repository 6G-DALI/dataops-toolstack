"""
Keycloak bearer-token checks for the orchestrator.

The caller must present a Keycloak access token issued by KEYCLOAK_ISSUER. With KEYCLOAK_ISSUER unset the
token-protected endpoints answer 503 rather than running open. Two levels of access to the testbed registry:

  * testbed admins carry the realm role TESTBED_ADMIN_ROLE: every testbed, and
    the registry-wide actions (register, provision, rotate keys, deregister);
  * testbed owners are members of the Keycloak group TESTBED_GROUP_PREFIX/<slug>
    (one group per testbed, delivered in the token's `groups` claim): that
    testbed only, and not the registry-wide actions.

The DataOps endpoints (dags, runs, tasks, datasets, services) need membership of DATAOPS_OPERATOR_GROUP. The
s3-asset-monitor's trigger of the validation DAG is also accepted with MONITOR_API_TOKEN (see
require_operator_or_monitor_token). /stats and /health stay public.

Every refusal is logged (WARNING; ERROR when the cause is on our side, such as Keycloak being unreachable) with the
method, the path, the user, the client the token was issued to and the caller's address, never the token; and its
response says what to fix: no token, an expired one, one from another issuer, a token without the `groups` claim (a
mapper is missing) or one whose user is simply not in the group (and which groups it has).

TODO (see the README's "Open items"):
  * check the token's audience / authorized party (`aud` / `azp`) against the clients that may call, so that a token
    the realm issued to another application is not enough (today the issuer and the group are what is checked);
  * give the s3-asset-monitor a Keycloak service account (client-credentials grant) and drop the shared
    X-Monitor-Token on the trigger endpoint.
"""

import hmac
import logging

import jwt
from fastapi import Depends, Header, HTTPException, Request
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer

from config import (
    DATAOPS_OPERATOR_GROUP, KEYCLOAK_ISSUER, MONITOR_API_TOKEN, MONITOR_TRIGGER_DAG_ID, TESTBED_ADMIN_ROLE,
    TESTBED_GROUP_PREFIX,
)

log = logging.getLogger(__name__)

if KEYCLOAK_ISSUER:
    log.info("Token-protected endpoints: Keycloak issuer %s; the DataOps endpoints need the group '%s'",
             KEYCLOAK_ISSUER, DATAOPS_OPERATOR_GROUP)
else:
    log.warning("KEYCLOAK_ISSUER is not set: every token-protected endpoint (DataOps and testbeds) answers 503")

_bearer = HTTPBearer(auto_error=False)
_jwks: jwt.PyJWKClient | None = None


def _jwks_client() -> jwt.PyJWKClient:
    global _jwks
    if _jwks is None:
        _jwks = jwt.PyJWKClient(f"{KEYCLOAK_ISSUER.rstrip('/')}/protocol/openid-connect/certs")
    return _jwks


def _unauthorized(detail: str) -> HTTPException:
    return HTTPException(status_code=401, detail=detail, headers={"WWW-Authenticate": 'Bearer realm="dataops"'})


def _unverified(creds: HTTPAuthorizationCredentials, claim: str) -> str:
    """One claim of a token that is being refused, read without verifying it, to say what was wrong with it."""
    try:
        return str(jwt.decode(creds.credentials, options={"verify_signature": False}).get(claim, "?"))
    except Exception:
        return "?"


def _decode(creds: HTTPAuthorizationCredentials | None) -> dict:
    if not KEYCLOAK_ISSUER:
        raise HTTPException(status_code=503, detail="KEYCLOAK_ISSUER is not configured on the orchestrator, so the "
                                                    "token-protected endpoints are disabled")
    if creds is None:
        raise _unauthorized("Missing bearer token: send 'Authorization: Bearer <Keycloak access token>'")
    try:
        key = _jwks_client().get_signing_key_from_jwt(creds.credentials)
        return jwt.decode(
            creds.credentials, key.key, algorithms=["RS256"],
            issuer=KEYCLOAK_ISSUER.rstrip("/"),
            # Keycloak access tokens carry aud=account (or the client); the issuer
            # and the role/groups below are what we rely on, not the audience.
            options={"verify_aud": False},
        )
    except jwt.ExpiredSignatureError:
        raise _unauthorized("Token expired: sign in again (or refresh the token)")
    except jwt.InvalidIssuerError:
        raise _unauthorized(f"Token issuer '{_unverified(creds, 'iss')}' is not the expected '{KEYCLOAK_ISSUER}'")
    except jwt.PyJWKClientConnectionError as e:
        # Our side, not the caller's: Keycloak's keys cannot be fetched, so no token can be checked.
        raise HTTPException(status_code=503, detail=f"Could not fetch Keycloak's signing keys from "
                                                    f"{KEYCLOAK_ISSUER.rstrip('/')}/protocol/openid-connect/certs: {e}")
    except jwt.PyJWKClientError as e:
        raise _unauthorized(f"The token's signature cannot be verified, no Keycloak key matches it: {e}")
    except Exception as e:
        raise _unauthorized(f"Invalid token: {e}")


def _caller(request: Request, claims: dict | None) -> str:
    who = (claims or {}).get("preferred_username") or (claims or {}).get("sub") or "anonymous"
    client = (claims or {}).get("azp") or (claims or {}).get("client_id") or "?"
    host = request.client.host if request.client else "?"
    return f"{who} (client {client}) from {host}"


def _refuse(request: Request, error: HTTPException, claims: dict | None = None):
    """Log the refusal (never the token) and raise it. A 5xx is our side of it: Keycloak or the configuration."""
    log.log(logging.ERROR if error.status_code >= 500 else logging.WARNING, "auth refused (%s): %s %s by %s: %s",
            error.status_code, request.method, request.url.path, _caller(request, claims), error.detail)
    raise error


def current_claims(request: Request, creds: HTTPAuthorizationCredentials | None = Depends(_bearer)) -> dict:
    """Returns the validated token claims (so callers can record who acted)."""
    try:
        return _decode(creds)
    except HTTPException as e:
        _refuse(request, e)


def claims_or_error(creds: HTTPAuthorizationCredentials | None = Depends(_bearer)) -> tuple[dict | None, HTTPException | None]:
    """The token's claims, or why it is not accepted: the DataOps routes log and raise the refusal themselves."""
    try:
        return _decode(creds), None
    except HTTPException as e:
        return None, e


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


# --- the DataOps endpoints --------------------------------------------------------------------------------------

def is_dataops_operator(claims: dict) -> bool:
    return any(str(g).strip("/") == DATAOPS_OPERATOR_GROUP for g in claims.get("groups", []))


def token_matches(given: str | None, expected: str) -> bool:
    """A shared token, compared in constant time; an unset (empty) token matches nothing."""
    return bool(expected) and given is not None and hmac.compare_digest(given.encode(), expected.encode())


def _not_an_operator(claims: dict) -> HTTPException:
    """Why a valid token is not an operator's: no groups claim at all (a mapper is missing) or not in the group."""
    who = claims.get("preferred_username") or claims.get("sub") or "unknown"
    client = claims.get("azp") or claims.get("client_id") or "?"
    if "groups" not in claims:
        return HTTPException(status_code=403, detail=(
            f"The token of '{who}' (client '{client}') has no 'groups' claim, so membership of "
            f"'{DATAOPS_OPERATOR_GROUP}' cannot be checked: add a Group Membership mapper to that client in Keycloak"))
    return HTTPException(status_code=403, detail=(
        f"'{who}' is not a member of the '{DATAOPS_OPERATOR_GROUP}' group (the token, issued to client '{client}', "
        f"has groups: {claims['groups'] or 'none'})"))


def require_dataops_operator(request: Request, result=Depends(claims_or_error)) -> dict:
    """Router dependency for the DataOps endpoints: a valid Keycloak token of a member of DATAOPS_OPERATOR_GROUP."""
    claims, error = result
    if error is None and not is_dataops_operator(claims):
        error = _not_an_operator(claims)
    if error is not None:
        _refuse(request, error, claims)
    return claims


def require_operator_or_monitor_token(dag_id: str, request: Request, result=Depends(claims_or_error),
                                      x_monitor_token: str | None = Header(default=None)) -> dict:
    """POST /dags/{dag_id}/trigger: an operator's token, or the s3-asset-monitor's X-Monitor-Token (the one it reads
    /monitor/buckets with), the latter only for MONITOR_TRIGGER_DAG_ID."""
    if x_monitor_token is None:
        return require_dataops_operator(request, result)
    if not MONITOR_API_TOKEN:
        _refuse(request, HTTPException(status_code=401, detail=(
            "X-Monitor-Token is not accepted: MONITOR_API_TOKEN is not set on the orchestrator")))
    if not token_matches(x_monitor_token, MONITOR_API_TOKEN):
        _refuse(request, HTTPException(status_code=401, detail=(
            "Wrong X-Monitor-Token: it must equal the orchestrator's MONITOR_API_TOKEN")))
    if dag_id != MONITOR_TRIGGER_DAG_ID:
        _refuse(request, HTTPException(status_code=403, detail=(
            f"X-Monitor-Token may only trigger '{MONITOR_TRIGGER_DAG_ID}', not '{dag_id}'")))
    return {"preferred_username": "s3-asset-monitor"}
