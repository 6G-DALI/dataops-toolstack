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
from auth import require_testbed_admin  # noqa: E402
from main import app  # noqa: E402


@pytest.fixture
def client(tmp_path, monkeypatch):
    monkeypatch.setattr(testbed_store, "TESTBED_DB_PATH", str(tmp_path / "t.db"))
    monkeypatch.setattr(testbed_store, "TESTBED_SECRET_KEY", "test-secret")
    if testbed_store._PG:  # run against a real Postgres when DATABASE_URL is set
        with testbed_store._db() as c:
            c.execute("TRUNCATE testbeds, testbed_audit")
    app.dependency_overrides[require_testbed_admin] = lambda: {"preferred_username": "tester"}
    calls = []
    monkeypatch.setattr(datalake_admin, "ensure_bucket", lambda b: calls.append(("bucket", b)) or "created")
    monkeypatch.setattr(datalake_admin, "create_scoped_user",
                        lambda slug, b: calls.append(("user", slug, b)) or ("tb-kul-1", "s3cret"))
    monkeypatch.setattr(datalake_admin, "remove_user", lambda ak: calls.append(("rm", ak)))
    monkeypatch.setattr(piveau_catalogue_client, "ensure_catalogue",
                        lambda *a: calls.append(("cat", a[0])) or "created")
    c = TestClient(app)
    c.calls = calls
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
    assert r.status_code == 502
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
