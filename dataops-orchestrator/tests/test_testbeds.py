import io
import os
import sys
import zipfile

import httpx
import pytest

sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))
os.environ["TESTBED_SECRET_KEY"] = "test-secret"
os.environ["KEYCLOAK_ISSUER"] = "https://kc.example/realms/dspace"

from fastapi.testclient import TestClient  # noqa: E402

import config  # noqa: E402
import datalake_admin  # noqa: E402
import piveau_catalogue_client  # noqa: E402
import testbed_store  # noqa: E402
from auth import current_claims  # noqa: E402
from main import app  # noqa: E402


@pytest.fixture
def client(tmp_path, monkeypatch):
    monkeypatch.setattr(testbed_store, "TESTBED_DB_PATH", str(tmp_path / "t.db"))
    monkeypatch.setattr(testbed_store, "TESTBED_SECRET_KEY", "test-secret")
    if testbed_store._PG:  # run against a real Postgres when DATABASE_URL is set
        with testbed_store._db() as c:
            c.execute("TRUNCATE testbeds, testbed_audit")
    claims = {"preferred_username": "tester", "realm_access": {"roles": ["testbed-admin"]}}
    app.dependency_overrides[current_claims] = lambda: claims  # tests change `client.claims` to act as someone else
    calls = []
    monkeypatch.setattr(datalake_admin, "ensure_bucket", lambda b: calls.append(("bucket", b)) or "created")
    monkeypatch.setattr(datalake_admin, "create_scoped_user",
                        lambda slug, b: calls.append(("user", slug, b)) or ("tb-kul-1", "s3cret"))
    monkeypatch.setattr(datalake_admin, "remove_user", lambda ak: calls.append(("rm", ak)))
    monkeypatch.setattr(piveau_catalogue_client, "ensure_catalogue",
                        lambda *a: calls.append(("cat", a[0])) or "created")
    c = TestClient(app)
    c.calls = calls
    c.claims = claims
    yield c
    app.dependency_overrides.clear()


def test_requires_auth_when_not_overridden():
    app.dependency_overrides.clear()
    assert TestClient(app).get("/testbeds").status_code == 401


def test_register_defaults_and_conflict(client):
    r = client.post("/testbeds", json={"slug": "kul", "name": "KU Leuven"})
    assert r.status_code == 201
    tb = r.json()
    assert tb["participant_id"] == "provider-kul" and tb["bucket"] == "6g-dali-kul"
    assert tb["dsp_url"] == "https://edc.kul.6gdali.eu/protocol" and tb["status"] == "draft"
    assert client.post("/testbeds", json={"slug": "kul", "name": "x"}).status_code == 409
    assert client.post("/testbeds", json={"slug": "Bad Slug", "name": "x"}).status_code == 422


def test_provision_is_idempotent_and_returns_secret_once(client):
    client.post("/testbeds", json={"slug": "kul", "name": "KU Leuven"})
    r1 = client.post("/testbeds/kul/provision").json()
    assert r1["issued_credentials"] == {"access_key": "tb-kul-1", "secret_key": "s3cret"}
    assert r1["testbed"]["status"] == "provisioned" and "secret" not in str(r1["testbed"]).replace("has_credentials", "")
    assert any("MONITOR_BUCKETS" in f for f in r1["followups"])
    r2 = client.post("/testbeds/kul/provision").json()
    assert r2["issued_credentials"] is None
    assert [c for c in client.calls if c[0] == "user"] == [("user", "kul", "6g-dali-kul")]
    assert testbed_store.get_s3_credentials("kul") == ("tb-kul-1", "s3cret")


def test_provision_reports_failed_step(client, monkeypatch):
    from fastapi import HTTPException
    client.post("/testbeds", json={"slug": "kul", "name": "KU Leuven"})

    def boom(*a):
        raise HTTPException(status_code=502, detail="piveau down")
    monkeypatch.setattr(piveau_catalogue_client, "ensure_catalogue", boom)
    r = client.post("/testbeds/kul/provision").json()
    assert r["testbed"]["steps"]["catalogue"]["status"] == "failed"
    assert r["testbed"]["status"] == "draft" and r["followups"] == []


def test_rotate_and_deregister(client):
    client.post("/testbeds", json={"slug": "kul", "name": "KU Leuven"})
    client.post("/testbeds/kul/provision")
    client.post("/testbeds/kul/credentials/rotate")
    assert ("rm", "tb-kul-1") in client.calls
    assert client.delete("/testbeds/kul").json()["status"] == "deregistered"
    assert client.get("/testbeds/kul").status_code == 404
    assert [e["action"] for e in testbed_store.audit_log("kul")][:2] == ["deregister", "rotate-credentials"]


def test_bundle_contents_and_stability(client):
    client.post("/testbeds", json={"slug": "kul", "name": "KU Leuven"})
    z1 = client.get("/testbeds/kul/bundle").content
    z2 = client.get("/testbeds/kul/bundle").content
    names = zipfile.ZipFile(io.BytesIO(z1)).namelist()
    assert "connector-kul/kul_connector.properties" in names and "connector-kul/nginx/edc.kul.6gdali.eu.conf" in names
    props = zipfile.ZipFile(io.BytesIO(z1)).read("connector-kul/kul_connector.properties").decode()
    assert "edc.catalog.ui.asset.admin.key=" in props and "edc.participant.id=provider-kul" in props and "edc.experiment.prefix=6g-dali-kul" in props
    assert zipfile.ZipFile(io.BytesIO(z1)).read("connector-kul/docker-compose.yaml") == \
        zipfile.ZipFile(io.BytesIO(z2)).read("connector-kul/docker-compose.yaml")
    nginx = zipfile.ZipFile(io.BytesIO(z1)).read("connector-kul/nginx/edc.kul.6gdali.eu.conf").decode()
    assert "map $uri $edc_backend" in nginx and "proxy_pass $edc_backend;" in nginx


def test_policy_is_scoped_to_bucket():
    doc = datalake_admin.policy_document("6g-dali-kul")
    resources = [r for s in doc["Statement"] for r in s["Resource"]]
    assert resources == ["arn:aws:s3:::6g-dali-kul", "arn:aws:s3:::6g-dali-kul/*"]


# --- asset discovery ------------------------------------------------------------

CATALOG_ONE = {
    "@type": "dcat:Catalog",
    "dcat:dataset": {
        "@id": "kul-experiments-1", "@type": "dcat:Dataset", "name": "KUL experiments",
        "odrl:hasPolicy": {"@id": "Y29udHJhY3Q=:a3VsLWV4cA==:abc", "@type": "odrl:Offer"},
    },
}
CATALOG_MANY = {
    "dcat:dataset": [
        {"@id": "a1", "odrl:hasPolicy": [{"@id": "offer-a1"}]},
        {"@id": "a2", "edc:name": "Second", "odrl:hasPolicy": {"@id": "offer-a2"}},
    ],
}


def test_parse_catalog_shapes():
    import edc_consumer_client as ec
    assert ec.parse_catalog(CATALOG_ONE) == [
        {"asset_id": "kul-experiments-1", "title": "KUL experiments", "offer_id": "Y29udHJhY3Q=:a3VsLWV4cA==:abc"}]
    assert [a["asset_id"] for a in ec.parse_catalog(CATALOG_MANY)] == ["a1", "a2"]
    assert ec.parse_catalog(CATALOG_MANY)[1]["title"] == "Second"
    assert ec.parse_catalog({"@type": "dcat:Catalog"}) == []  # nothing offered


def test_discover_stores_assets_and_records_connector(client, monkeypatch):
    import edc_consumer_client as ec
    client.post("/testbeds", json={"slug": "kul", "name": "KU Leuven"})
    monkeypatch.setattr(ec, "fetch_catalog", lambda url, pid: ec.parse_catalog(CATALOG_ONE))
    r = client.post("/testbeds/kul/assets/discover").json()
    assert r["offered"] == 1 and r["assets"][0]["asset_id"] == "kul-experiments-1"
    assert r["assets"][0]["status"] == "discovered" and r["assets"][0]["present"] is True
    assert client.get("/testbeds/kul").json()["steps"]["connector"]["status"] == "ok"

    # rediscovery keeps negotiation state, and flags assets that are no longer offered
    with testbed_store._db() as c:
        c.execute("UPDATE testbed_assets SET status='agreed', contract_agreement_id='ag-1' WHERE slug='kul'")
    monkeypatch.setattr(ec, "fetch_catalog", lambda url, pid: ec.parse_catalog(CATALOG_MANY))
    assets = {a["asset_id"]: a for a in client.post("/testbeds/kul/assets/discover").json()["assets"]}
    assert assets["kul-experiments-1"]["status"] == "agreed" and assets["kul-experiments-1"]["present"] is False
    assert assets["kul-experiments-1"]["contract_agreement_id"] == "ag-1"
    assert assets["a1"]["present"] is True and len(client.get("/testbeds/kul/assets").json()["assets"]) == 3


def test_discover_failure_is_recorded(client, monkeypatch):
    import edc_consumer_client as ec
    from fastapi import HTTPException
    client.post("/testbeds", json={"slug": "kul", "name": "KU Leuven"})

    def down(url, pid):
        raise HTTPException(status_code=502, detail="connection refused")
    monkeypatch.setattr(ec, "fetch_catalog", down)
    r = client.post("/testbeds/kul/assets/discover")
    assert r.status_code == 424 and r.json()["upstream_status"] == 502
    assert "refused" in r.json()["detail"]
    step = client.get("/testbeds/kul").json()["steps"]["connector"]
    assert step["status"] == "failed" and "refused" in step["detail"]


def test_fetch_catalog_request_shape(monkeypatch):
    import edc_consumer_client as ec
    seen = {}

    def fake_post(url, json=None, headers=None, timeout=None):
        seen.update(url=url, body=json, headers=headers)
        return httpx.Response(200, json=CATALOG_ONE)
    monkeypatch.setattr(ec, "EDC_PROVIDER_MANAGEMENT_URL", "https://edc.example/management")
    monkeypatch.setattr(ec, "EDC_API_KEY", "k")
    monkeypatch.setattr(ec.httpx, "post", fake_post)
    assert ec.fetch_catalog("https://edc.kul/protocol", "provider-kul")[0]["asset_id"] == "kul-experiments-1"
    assert seen["url"] == "https://edc.example/management/v3/catalog/request" and seen["headers"] == {"X-Api-Key": "k"}
    assert seen["body"]["counterPartyAddress"] == "https://edc.kul/protocol" and seen["body"]["@type"] == "CatalogRequest"


# --- transfer lookup ---------------------------------------------------------------

KUL_URL = "https://edc.kul.6gdali.eu/protocol"
TRANSFERS = [
    {"@id": "t-old", "state": "COMPLETED", "contractId": "ag-0", "counterPartyAddress": KUL_URL, "stateTimestamp": 100},
    {"@id": "t-live", "edc:state": "STARTED", "contractId": "ag-1", "counterPartyAddress": KUL_URL + "/",
     "transferType": "PiveauData-PUSH", "stateTimestamp": 200},
    {"@id": "t-other", "state": "STARTED", "contractId": "ag-9",
     "counterPartyAddress": "https://edc.isi.6gdali.eu/protocol", "stateTimestamp": 300},
]


def test_parse_and_choose_transfers():
    import edc_consumer_client as ec
    parsed = ec.parse_transfers(TRANSFERS, KUL_URL)
    assert [t["transfer_id"] for t in parsed] == ["t-live", "t-old"]  # other testbed dropped, newest first
    assert parsed[0]["active"] is True and parsed[0]["transfer_type"] == "PiveauData-PUSH"
    assert ec.best_transfer(parsed)["transfer_id"] == "t-live"
    assert ec.best_transfer([{"state": "COMPLETED", "transfer_id": "x", "active": False, "state_timestamp": 1},
                             {"state": "REQUESTED", "transfer_id": "y", "active": False, "state_timestamp": 0}])["transfer_id"] == "y"
    assert ec.best_transfer([]) is None
    assert ec.parse_transfers([{"@id": "n", "state": "STARTED"}], KUL_URL)[0]["transfer_id"] == "n"  # no address: kept


def test_find_transfers_request_shape(monkeypatch):
    import edc_consumer_client as ec
    seen = {}

    def fake_post(url, json=None, headers=None, timeout=None):
        seen.update(url=url, body=json)
        return httpx.Response(200, json=TRANSFERS)
    monkeypatch.setattr(ec, "EDC_PROVIDER_MANAGEMENT_URL", "https://edc.example/management")
    monkeypatch.setattr(ec.httpx, "post", fake_post)
    assert len(ec.find_transfers("kul-experiments-1", KUL_URL)) == 2
    assert seen["url"].endswith("/v3/transferprocesses/request")
    assert seen["body"]["filterExpression"] == [{"operandLeft": "assetId", "operator": "=", "operandRight": "kul-experiments-1"}]


def test_find_transfer_endpoint_marks_asset_transferring(client, monkeypatch):
    import edc_consumer_client as ec
    client.post("/testbeds", json={"slug": "kul", "name": "KU Leuven", "dsp_url": KUL_URL})
    monkeypatch.setattr(ec, "fetch_catalog", lambda u, p: ec.parse_catalog(CATALOG_ONE))
    client.post("/testbeds/kul/assets/discover")

    monkeypatch.setattr(ec, "find_transfers", lambda a, u: ec.parse_transfers(TRANSFERS, u))
    r = client.post("/testbeds/kul/assets/kul-experiments-1/transfers/find").json()
    assert r["active"] is True and r["asset"]["transfer_state"] == "STARTED" and r["asset"]["status"] == "transferring"
    assert r["asset"]["transfer_id"] == "t-live" and r["asset"]["contract_agreement_id"] == "ag-1"

    monkeypatch.setattr(ec, "find_transfers", lambda a, u: [])  # transfer gone
    r = client.post("/testbeds/kul/assets/kul-experiments-1/transfers/find").json()
    assert r["active"] is False and r["asset"]["transfer_state"] is None and r["asset"]["transfer_checked_at"]
    assert client.post("/testbeds/kul/assets/nope/transfers/find").status_code == 404


def test_old_asset_table_gets_new_columns(tmp_path, monkeypatch):
    import sqlite3
    path = str(tmp_path / "old.db")
    raw = sqlite3.connect(path)
    raw.execute("""CREATE TABLE testbed_assets (slug TEXT NOT NULL, asset_id TEXT NOT NULL, title TEXT, offer_id TEXT,
        status TEXT NOT NULL DEFAULT 'discovered', present INTEGER NOT NULL DEFAULT 1, contract_agreement_id TEXT,
        transfer_id TEXT, discovered_at TEXT, last_seen_at TEXT, PRIMARY KEY (slug, asset_id))""")
    raw.commit()
    raw.close()
    monkeypatch.setattr(testbed_store, "TESTBED_DB_PATH", path)
    assert testbed_store.list_assets("x") == []  # opening the db runs the migration
    cols = {r[1] for r in sqlite3.connect(path).execute("PRAGMA table_info(testbed_assets)")}
    assert {"transfer_state", "transfer_checked_at"} <= cols


def _act_as_owner(client, *groups):
    client.claims.clear()
    client.claims.update({"preferred_username": "owner", "realm_access": {"roles": []}, "groups": list(groups)})


def test_owner_sees_and_works_with_only_their_testbed(client):
    for slug in ("kul", "isi"):
        client.post("/testbeds", json={"slug": slug, "name": slug.upper()})
    _act_as_owner(client, "/testbeds/kul")

    assert [t["slug"] for t in client.get("/testbeds").json()["testbeds"]] == ["kul"]
    assert client.get("/testbeds/kul").status_code == 200
    assert client.get("/testbeds/kul/assets").status_code == 200
    assert client.get("/testbeds/kul/audit").status_code == 200
    assert client.get("/testbeds/kul/bundle").status_code == 200

    for path in ("", "/assets", "/audit", "/bundle"):
        assert client.get(f"/testbeds/isi{path}").status_code == 403
    assert client.post("/testbeds/isi/assets/discover").status_code == 403
    assert client.post("/testbeds/isi/assets/a1/negotiate").status_code == 403
    assert client.post("/testbeds/isi/assets/a1/transfers/start").status_code == 403


def test_owner_cannot_use_registry_wide_actions(client):
    client.post("/testbeds", json={"slug": "kul", "name": "KU Leuven"})
    _act_as_owner(client, "/testbeds/kul")
    assert client.post("/testbeds", json={"slug": "new", "name": "New"}).status_code == 403
    assert client.post("/testbeds/kul/provision").status_code == 403
    assert client.post("/testbeds/kul/credentials/rotate").status_code == 403
    assert client.delete("/testbeds/kul").status_code == 403
    assert client.get("/testbeds/kul").status_code == 200  # still theirs to see


def test_user_without_a_testbed_group_sees_nothing(client):
    client.post("/testbeds", json={"slug": "kul", "name": "KU Leuven"})
    _act_as_owner(client, "/other/kul", "/testbeds/kul/extra", "/testbeds")
    assert client.get("/testbeds").json() == {"testbeds": [], "total": 0}
    assert client.get("/testbeds/kul").status_code == 403


def test_owner_group_may_be_a_bare_name_and_admin_sees_all(client):
    for slug in ("kul", "isi"):
        client.post("/testbeds", json={"slug": slug, "name": slug.upper()})
    _act_as_owner(client, "kul")  # Keycloak "full group path" off
    assert [t["slug"] for t in client.get("/testbeds").json()["testbeds"]] == ["kul"]
    client.claims.update({"realm_access": {"roles": ["testbed-admin"]}})
    assert client.get("/testbeds").json()["total"] == 2


# --- Keycloak group per testbed -------------------------------------------------------------------------

import httpx  # noqa: E402
import keycloak_admin  # noqa: E402


def _keycloak(monkeypatch, existing=(), child_conflict=False):
    """Stands in for Keycloak: records the calls, `existing` are the top-level groups already there."""
    seen, groups = [], {n: f"id-{n}" for n in existing}

    def handler(request: httpx.Request) -> httpx.Response:
        path = request.url.path
        seen.append((request.method, path))
        if path.endswith("/openid-connect/token"):
            return httpx.Response(200, json={"access_token": "tok"})
        assert request.headers["authorization"] == "Bearer tok"
        if request.method == "GET":
            name = request.url.params["search"]
            return httpx.Response(200, json=[{"id": groups[name], "name": name, "path": f"/{name}"}] if name in groups else [])
        if path.endswith("/children"):
            return httpx.Response(409 if child_conflict else 201)
        name = request.read().decode().split('"')[3]
        groups[name] = f"id-{name}"
        return httpx.Response(201)

    real = httpx.Client
    monkeypatch.setattr(keycloak_admin.httpx, "Client", lambda **kw: real(transport=httpx.MockTransport(handler), **kw))
    for name, value in (("KEYCLOAK_ISSUER", "https://kc.example/auth/realms/dspace"), ("KEYCLOAK_ADMIN_CLIENT_ID", "orch"),
                        ("KEYCLOAK_ADMIN_CLIENT_SECRET", "s"), ("TESTBED_GROUP_PREFIX", "testbeds")):
        monkeypatch.setattr(keycloak_admin, name, value)
    return seen


def test_group_created_under_the_prefix_group(monkeypatch):
    seen = _keycloak(monkeypatch)
    assert keycloak_admin.ensure_group("kul") == "created"
    assert ("POST", "/auth/admin/realms/dspace/groups") in seen  # the /testbeds parent was missing
    assert ("POST", "/auth/admin/realms/dspace/groups/id-testbeds/children") in seen


def test_group_that_already_exists_is_not_an_error(monkeypatch):
    _keycloak(monkeypatch, existing=("testbeds",), child_conflict=True)
    assert keycloak_admin.ensure_group("kul") == "exists"


def test_provision_records_the_access_step(client, monkeypatch):
    client.post("/testbeds", json={"slug": "kul", "name": "KU Leuven"})
    monkeypatch.setattr(keycloak_admin, "configured", lambda: False)
    steps = client.post("/testbeds/kul/provision").json()["testbed"]["steps"]
    assert steps["access"]["status"] == "skipped"
    assert client.post("/testbeds/kul/provision").json()["testbed"]["status"] == "provisioned"  # skipped does not block

    monkeypatch.setattr(keycloak_admin, "configured", lambda: True)
    monkeypatch.setattr(keycloak_admin, "ensure_group", lambda slug: "created")
    assert client.post("/testbeds/kul/provision").json()["testbed"]["steps"]["access"]["status"] == "ok"
