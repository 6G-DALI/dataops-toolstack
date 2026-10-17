"""piveau access control v2: access levels on writes, private testbed catalogues, and reads with the caller's token."""
import asyncio
import os
import sys

import pytest

sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))
os.environ["TESTBED_SECRET_KEY"] = "test-secret"
os.environ["KEYCLOAK_ISSUER"] = "https://kc.example/realms/dspace"

from fastapi import HTTPException  # noqa: E402
from fastapi.testclient import TestClient  # noqa: E402

import config  # noqa: E402
import datalake_admin  # noqa: E402
import piveau_access  # noqa: E402
import piveau_catalogue_client  # noqa: E402
import piveau_dataset_client as pdc  # noqa: E402
import testbed_store  # noqa: E402
from auth import current_claims  # noqa: E402
from main import app  # noqa: E402

AR = "http://publications.europa.eu/resource/authority/access-right/"


def dataset_turtle(right: str | None) -> str:
    rights = f"    dct:accessRights <{AR}{right}> ;\n" if right else ""
    return ("@prefix dcat: <http://www.w3.org/ns/dcat#> . @prefix dct: <http://purl.org/dc/terms/> .\n"
            "<https://dataspace.6gdali.eu/datasets/x> a dcat:Dataset ;\n" + rights + '    dct:title "x"@en .\n')


@pytest.fixture
def ac(monkeypatch):
    """Access control on, with a stubbed catalogue level per catalogue id."""
    monkeypatch.setattr(config, "PIVEAU_ACCESS_CONTROL", True)
    levels = {"pub": "public", "priv": "internal"}
    monkeypatch.setattr(piveau_access, "catalogue_level", lambda c: levels[c])
    return levels


# --- mapping -------------------------------------------------------------------------------------------------------

def test_access_rights_map_to_levels():
    assert piveau_access.level_of("PUBLIC") == "public"
    assert piveau_access.level_of(AR + "RESTRICTED") == "restricted"
    assert piveau_access.level_of(AR + "NON_PUBLIC") == "internal"
    assert piveau_access.level_of(None) == "public"
    assert piveau_access.level_of("NON-PUBLIC") == "internal"  # unknown: never public


def test_stricter_level_wins():
    assert piveau_access.stricter("public", "internal") == "internal"
    assert piveau_access.stricter("restricted", "public") == "restricted"


def test_rights_are_read_from_a_dataset_or_catalogue_record():
    assert piveau_access.rights_in(dataset_turtle("NON_PUBLIC")) == AR + "NON_PUBLIC"
    assert piveau_access.rights_in(dataset_turtle(None)) is None
    assert piveau_access.rights_in("not turtle {") is None
    private_cat = piveau_catalogue_client._turtle("c", "T", "D", "P", private=True)
    public_cat = piveau_catalogue_client._turtle("c", "T", "D", "P")
    assert piveau_access.rights_in(private_cat) == AR + "NON_PUBLIC"
    assert piveau_access.rights_in(public_cat) == AR + "PUBLIC"


# --- writes --------------------------------------------------------------------------------------------------------

def test_no_access_level_is_sent_while_access_control_is_off(monkeypatch):
    monkeypatch.setattr(config, "PIVEAU_ACCESS_CONTROL", False)
    assert asyncio.run(piveau_access.write_params("priv", dataset_turtle("NON_PUBLIC"))) == {}


@pytest.mark.parametrize("catalogue,right,level", [
    ("pub", None, "public"), ("pub", "PUBLIC", "public"), ("pub", "RESTRICTED", "restricted"),
    ("pub", "NON_PUBLIC", "internal"), ("priv", "PUBLIC", "internal"), ("priv", None, "internal"),
])
def test_a_dataset_gets_the_stricter_of_its_catalogue_and_its_own_rights(ac, catalogue, right, level):
    params = asyncio.run(piveau_access.write_params(catalogue, dataset_turtle(right)))
    assert params == {"accessLevel": level, "publicationStatus": "published"}


def test_a_floor_replaces_the_records_own_rights(ac):
    # Services state RESTRICTED but are written public, unless their catalogue is private.
    assert asyncio.run(piveau_access.write_params("pub", dataset_turtle("RESTRICTED"), floor="public"))["accessLevel"] == "public"
    assert asyncio.run(piveau_access.write_params("priv", dataset_turtle("RESTRICTED"), floor="public"))["accessLevel"] == "internal"


class _Recorder:
    """Stands in for httpx.AsyncClient and records requests."""
    calls: list = []

    def __init__(self, *a, headers=None, **kw):
        self.headers = headers or {}

    async def __aenter__(self):
        return self

    async def __aexit__(self, *a):
        return False

    async def put(self, url, content=None, headers=None, params=None):
        _Recorder.calls.append(("PUT", url, params))
        return _Resp()

    async def get(self, url, params=None, **kw):
        _Recorder.calls.append(("GET", url, dict(self.headers)))
        return _Resp({"result": {"results": [], "count": 0}})


class _Resp:
    def __init__(self, body=None):
        self._body = body or {}
        self.status_code = 200

    def raise_for_status(self):
        pass

    def json(self):
        return self._body


def test_creating_a_dataset_sends_its_access_level(ac, monkeypatch):
    monkeypatch.setattr(pdc, "PIVEAU_HUB_URL", "http://hub")
    monkeypatch.setattr(pdc, "PIVEAU_API_KEY", "k")
    monkeypatch.setattr(pdc.httpx, "AsyncClient", _Recorder)
    _Recorder.calls = []
    asyncio.run(pdc._put_dataset_turtle("x", "priv", dataset_turtle("PUBLIC")))
    assert _Recorder.calls == [("PUT", "http://hub/datasets/x?catalogue=priv",
                                {"accessLevel": "internal", "publicationStatus": "published"})]


def test_catalogue_level_fails_closed(monkeypatch):
    monkeypatch.setattr(pdc, "PIVEAU_HUB_URL", "http://127.0.0.1:9")  # nothing listens
    monkeypatch.setattr(pdc, "PIVEAU_API_KEY", "k")
    piveau_access._catalogue_levels.pop("unknown", None)
    with pytest.raises(HTTPException) as e:
        piveau_access.catalogue_level("unknown")
    assert e.value.status_code == 502


# --- private testbed catalogues ------------------------------------------------------------------------------------

@pytest.fixture
def client(tmp_path, monkeypatch):
    monkeypatch.setattr(testbed_store, "TESTBED_DB_PATH", str(tmp_path / "t.db"))
    monkeypatch.setattr(testbed_store, "TESTBED_SECRET_KEY", "test-secret")
    if testbed_store._PG:
        with testbed_store._db() as c:
            c.execute("TRUNCATE testbeds, testbed_audit")
    app.dependency_overrides[current_claims] = lambda: {"preferred_username": "t", "realm_access": {"roles": ["testbed-admin"]}}
    calls = []
    monkeypatch.setattr(datalake_admin, "ensure_bucket", lambda b: "created")
    monkeypatch.setattr(datalake_admin, "create_scoped_user", lambda slug, b: ("ak", "sk"))
    monkeypatch.setattr(piveau_catalogue_client, "ensure_catalogue", lambda *a: calls.append(a) or "created")
    c = TestClient(app)
    c.calls = calls
    yield c
    app.dependency_overrides.clear()


def test_private_catalogue_is_refused_without_access_control(client, monkeypatch):
    monkeypatch.setattr(config, "PIVEAU_ACCESS_CONTROL", False)
    r = client.post("/testbeds", json={"slug": "kul", "name": "KU Leuven", "private": True})
    assert r.status_code == 422 and "PIVEAU_ACCESS_CONTROL" in r.json()["detail"]


def test_testbeds_are_public_by_default(client):
    assert client.post("/testbeds", json={"slug": "kul", "name": "KU Leuven"}).json()["visibility"] == "public"
    client.post("/testbeds/kul/provision")
    assert client.calls[-1][-1] is False  # ensure_catalogue(..., private=False)


def test_a_private_testbed_gets_a_private_catalogue(client, ac):
    tb = client.post("/testbeds", json={"slug": "kul", "name": "KU Leuven", "private": True}).json()
    assert tb["visibility"] == "private"
    r = client.post("/testbeds/kul/provision").json()
    assert client.calls[-1][-1] is True and r["testbed"]["steps"]["catalogue"]["status"] == "ok"


def test_a_private_testbed_with_an_existing_public_catalogue_fails_its_catalogue_step(client, ac, monkeypatch):
    monkeypatch.setattr(piveau_catalogue_client, "ensure_catalogue", lambda *a: "exists")
    ac["6g-dali-kul"] = "public"
    client.post("/testbeds", json={"slug": "kul", "name": "KU Leuven", "private": True})
    step = client.post("/testbeds/kul/provision").json()["testbed"]["steps"]["catalogue"]
    assert step["status"] == "failed" and "cannot be changed" in step["detail"]


# --- reads with the caller's token -----------------------------------------------------------------------------------

def test_listings_forward_the_callers_token_only_with_access_control(monkeypatch):
    import piveau_client
    monkeypatch.setattr(piveau_client.httpx, "AsyncClient", _Recorder)
    c = TestClient(app)
    for on, expected in ((False, {}), (True, {"Authorization": "Bearer user-token"})):
        monkeypatch.setattr(config, "PIVEAU_ACCESS_CONTROL", on)
        _Recorder.calls = []
        c.get("/datasets/catalogues", headers={"Authorization": "Bearer user-token"})
        assert _Recorder.calls[0][2] == expected
    _Recorder.calls = []
    c.get("/datasets/catalogues")  # anonymous caller: anonymous read
    assert _Recorder.calls[0][2] == {}
