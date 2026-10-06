import io
import os
import sys
import zipfile

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
