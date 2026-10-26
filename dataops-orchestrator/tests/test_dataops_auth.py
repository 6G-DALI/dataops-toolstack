"""The DataOps endpoints (dags, runs, tasks, datasets, services) need a Keycloak token of a dataops operator.

A refusal is logged and its response says what to fix. The s3-asset-monitor may also start its validation DAG with its
X-Monitor-Token. /stats and /health stay public.
"""
import logging
import time

import jwt
import pytest
from cryptography.hazmat.primitives.asymmetric import rsa
from fastapi.testclient import TestClient

import test_testbeds  # noqa: F401  (sets the Keycloak environment before main is imported)
import airflow_client as af
import auth
from main import app
from routers import stats

ISSUER = "https://kc.example/realms/dspace"
OPERATOR = {"preferred_username": "ann", "azp": "dataops-ui", "groups": ["/dataops-operators"]}
OTHER = {"preferred_username": "bob", "azp": "dataops-ui", "groups": ["/testbeds/kul"]}
MONITOR_TOKEN = "monitor-secret"
VALIDATION_DAG = "dali_dataspace_validate_dataset"

PROTECTED = [("GET", "/dags"), ("GET", "/dags/x"), ("GET", "/dags/x/runs"), ("GET", "/tasks"), ("GET", "/services"),
             ("GET", "/datasets"), ("POST", "/dags/x/trigger")]

_KEY = rsa.generate_private_key(public_exponent=65537, key_size=2048)


class _Signing:
    key = _KEY.public_key()


class _Jwks:
    """Stands for Keycloak's key set: every token is checked against the test key."""
    failure = None

    def get_signing_key_from_jwt(self, token):
        if self.failure:
            raise self.failure
        return _Signing()


def token(claims=None, issuer=ISSUER, expires_in=300):
    now = int(time.time())
    return jwt.encode({"iss": issuer, "iat": now, "exp": now + expires_in, **(claims or {})}, _KEY, algorithm="RS256")


@pytest.fixture
def api(monkeypatch):
    async def list_dags(limit=100):
        return {"dags": [], "total_entries": 0}

    async def trigger_dag(dag_id, conf):
        return {"dag_run_id": f"run-of-{dag_id}"}

    monkeypatch.setattr(af, "list_dags", list_dags)
    monkeypatch.setattr(af, "trigger_dag", trigger_dag)
    monkeypatch.setattr(auth, "KEYCLOAK_ISSUER", ISSUER)
    monkeypatch.setattr(auth, "MONITOR_API_TOKEN", MONITOR_TOKEN)
    monkeypatch.setattr(auth, "MONITOR_TRIGGER_DAG_ID", VALIDATION_DAG)
    jwks = _Jwks()
    monkeypatch.setattr(auth, "_jwks_client", lambda: jwks)
    c = TestClient(app, raise_server_exceptions=False)
    c.jwks = jwks

    def act_as(claims):
        """What the token says, as if Keycloak had signed it (None: no token)."""
        if claims is None:
            app.dependency_overrides.pop(auth.claims_or_error, None)
        else:
            app.dependency_overrides[auth.claims_or_error] = lambda: (claims, None)

    def bearer(claims=None, **kw):
        return {"Authorization": f"Bearer {token(claims, **kw)}"}

    c.act_as, c.bearer = act_as, bearer
    yield c
    app.dependency_overrides.clear()


def _auth_logs(caplog):
    return [r for r in caplog.records if r.name == "auth"]


# --- who gets in --------------------------------------------------------------------------------------------------

@pytest.mark.parametrize("method,path", PROTECTED)
def test_without_a_token_the_dataops_endpoints_are_refused(api, method, path):
    r = api.request(method, path)
    assert r.status_code == 401
    assert "Missing bearer token" in r.json()["detail"] and "Authorization: Bearer" in r.json()["detail"]
    assert r.headers["www-authenticate"].startswith("Bearer")


@pytest.mark.parametrize("method,path", PROTECTED)
def test_a_token_of_someone_who_is_not_an_operator_is_forbidden(api, method, path):
    api.act_as(OTHER)
    assert api.request(method, path).status_code == 403


def test_an_operator_is_let_through(api):
    api.act_as(OPERATOR)
    assert api.get("/dags").status_code == 200
    assert api.post("/dags/anything/trigger", json={}).json() == {"dag_run_id": "run-of-anything"}


@pytest.mark.parametrize("group", ["/dataops-operators", "dataops-operators"])
def test_the_operator_group_may_be_a_path_or_a_bare_name(api, group):
    api.act_as({"groups": [group]})
    assert api.get("/dags").status_code == 200


def test_a_real_signed_token_of_an_operator_is_accepted(api):
    assert api.get("/dags", headers=api.bearer(OPERATOR)).status_code == 200


# --- a refusal says what is wrong ------------------------------------------------------------------------------------

def test_a_user_outside_the_group_is_told_which_groups_the_token_has(api):
    r = api.get("/dags", headers=api.bearer(OTHER))
    assert r.status_code == 403
    detail = r.json()["detail"]
    assert "'bob'" in detail and "'dataops-operators'" in detail and "/testbeds/kul" in detail and "dataops-ui" in detail


def test_a_token_without_a_groups_claim_points_at_the_missing_mapper(api):
    r = api.get("/dags", headers=api.bearer({"preferred_username": "ann", "azp": "dataops-ui"}))
    assert r.status_code == 403
    assert "no 'groups' claim" in r.json()["detail"] and "Group Membership mapper" in r.json()["detail"]
    assert "dataops-ui" in r.json()["detail"]


def test_an_expired_token_says_so(api):
    r = api.get("/dags", headers=api.bearer(OPERATOR, expires_in=-60))
    assert r.status_code == 401 and "expired" in r.json()["detail"].lower()
    assert r.headers["www-authenticate"].startswith("Bearer")


def test_a_token_from_another_issuer_names_both(api):
    r = api.get("/dags", headers=api.bearer(OPERATOR, issuer="https://other.example/realms/x"))
    assert r.status_code == 401
    assert "https://other.example/realms/x" in r.json()["detail"] and ISSUER in r.json()["detail"]


def test_garbage_instead_of_a_token_is_an_invalid_token(api):
    r = api.get("/dags", headers={"Authorization": "Bearer not-a-jwt"})
    assert r.status_code == 401 and r.json()["detail"].startswith(("Invalid token", "The token's signature"))


def test_a_key_that_does_not_match_is_not_the_callers_fault_but_is_a_401(api):
    api.jwks.failure = jwt.PyJWKClientError("Unable to find a signing key that matches: kid")
    r = api.get("/dags", headers=api.bearer(OPERATOR))
    assert r.status_code == 401 and "no Keycloak key matches" in r.json()["detail"]


def test_keycloak_being_unreachable_is_a_503_that_names_the_keys_url(api, caplog):
    api.jwks.failure = jwt.PyJWKClientConnectionError("connection refused")
    with caplog.at_level(logging.WARNING, logger="auth"):
        r = api.get("/dags", headers=api.bearer(OPERATOR))
    assert r.status_code == 503
    assert f"{ISSUER}/protocol/openid-connect/certs" in r.json()["detail"]
    assert [x.levelname for x in _auth_logs(caplog)] == ["ERROR"]  # our side of it


def test_without_a_configured_issuer_everything_protected_is_503(api, monkeypatch):
    monkeypatch.setattr(auth, "KEYCLOAK_ISSUER", "")
    r = api.get("/dags", headers=api.bearer(OPERATOR))
    assert r.status_code == 503 and "KEYCLOAK_ISSUER is not configured" in r.json()["detail"]


# --- the log ------------------------------------------------------------------------------------------------------

def test_a_refusal_is_logged_with_who_where_and_why_but_not_the_token(api, caplog):
    headers = api.bearer(OTHER)
    with caplog.at_level(logging.WARNING, logger="auth"):
        api.get("/dags", headers=headers)
        api.get("/tasks")
    first, second = [r.getMessage() for r in _auth_logs(caplog)]
    assert "auth refused (403): GET /dags by bob (client dataops-ui) from" in first and "not a member" in first
    assert "auth refused (401): GET /tasks by anonymous (client ?) from" in second and "Missing bearer token" in second
    assert headers["Authorization"].split()[1] not in first + second


def test_an_operators_call_is_not_logged_as_a_refusal(api, caplog):
    with caplog.at_level(logging.WARNING, logger="auth"):
        api.get("/dags", headers=api.bearer(OPERATOR))
    assert _auth_logs(caplog) == []


def test_the_testbed_endpoints_log_their_refusals_too(api, caplog):
    with caplog.at_level(logging.WARNING, logger="auth"):
        assert api.get("/testbeds").status_code == 401
    assert any("auth refused (401): GET /testbeds" in r.getMessage() for r in _auth_logs(caplog))


# --- the s3-asset-monitor's trigger ----------------------------------------------------------------------------

def _trigger(api, dag=VALIDATION_DAG, token=MONITOR_TOKEN):
    return api.post(f"/dags/{dag}/trigger", json={"catalogue_id": "c"}, headers={"X-Monitor-Token": token})


def test_the_monitor_token_starts_the_validation_dag(api):
    assert _trigger(api).json() == {"dag_run_id": f"run-of-{VALIDATION_DAG}"}


def test_the_monitor_token_starts_nothing_else_and_says_which_dag(api):
    r = _trigger(api, dag="some_other_dag")
    assert r.status_code == 403
    assert VALIDATION_DAG in r.json()["detail"] and "some_other_dag" in r.json()["detail"]


def test_a_wrong_monitor_token_is_refused_even_for_an_operator(api):
    r = _trigger(api, token="wrong")
    assert r.status_code == 401 and "MONITOR_API_TOKEN" in r.json()["detail"]
    api.act_as(OPERATOR)
    assert _trigger(api, token="wrong").status_code == 401


def test_the_monitor_token_opens_nothing_but_the_trigger(api):
    for method, path in (("GET", "/dags"), ("GET", f"/dags/{VALIDATION_DAG}/runs"), ("GET", "/tasks")):
        assert api.request(method, path, headers={"X-Monitor-Token": MONITOR_TOKEN}).status_code == 401


def test_without_a_configured_monitor_token_the_header_is_not_accepted_and_the_response_says_so(api, monkeypatch):
    monkeypatch.setattr(auth, "MONITOR_API_TOKEN", "")
    for given in ("", "anything"):
        r = _trigger(api, token=given)
        assert r.status_code == 401 and "MONITOR_API_TOKEN is not set" in r.json()["detail"]


def test_an_operator_can_still_trigger_without_the_header(api):
    api.act_as(OPERATOR)
    assert api.post("/dags/anything/trigger", json={}).status_code == 200


# --- what stays open ---------------------------------------------------------------------------------------------

def test_health_and_stats_stay_public(api):
    assert api.get("/health").status_code == 200
    assert stats.router.dependencies == []  # no operator check on /stats or /stats/portal
