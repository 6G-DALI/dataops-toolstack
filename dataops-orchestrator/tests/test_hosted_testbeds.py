"""Testbeds hosted by the facility connector: an upload bucket and key instead of a connector of their own, and a
facility-to-facility transfer of the upload bucket into the testbed's Data Lake bucket, started by provisioning."""
import asyncio

import pytest

import datalake_admin
import edc_client
import edc_consumer_client as ec
from routers import testbeds as router
from test_testbeds import client  # noqa: F401  (fixture)

FACILITY = "https://edc.facility.example/protocol"


@pytest.fixture
def facility(client, monkeypatch):  # noqa: F811
    """The facility connector: offers one distribution asset of its own besides each registered upload asset."""
    import piveau_dataset_client as pdc
    monkeypatch.setattr(pdc, "PIVEAU_HUB_URL", "https://piveau.example/")
    monkeypatch.setattr(pdc, "PIVEAU_API_KEY", "pk")
    monkeypatch.setattr(router, "DATALAKE_PUBLIC_ENDPOINT_URL", "https://lake.example")
    monkeypatch.setattr(router, "FACILITY_DSP_URL", FACILITY)
    monkeypatch.setattr(router, "FACILITY_PARTICIPANT_ID", "provider-facility")
    state = {"assets": {"some-distribution": "x"}, "negotiations": [], "transfers": [], "running": [], "ended": [],
             "deleted": []}

    def register(aid, bucket, title):
        if aid in state["assets"]:
            return "exists"
        state["assets"][aid] = bucket
        return "created"

    async def delete(aid):
        state["deleted"].append(aid)
        return {"status": "deleted", "asset_id": aid}

    monkeypatch.setattr(edc_client, "register_upload_asset", register)
    monkeypatch.setattr(edc_client, "delete_asset", delete)
    monkeypatch.setattr(ec, "fetch_catalog", lambda url, pid: [
        {"asset_id": a, "title": a, "offer_id": f"offer-{a}"} for a in state["assets"]] if (url, pid) == (
        FACILITY, "provider-facility") else [])
    monkeypatch.setattr(ec, "start_negotiation", lambda *a: state["negotiations"].append(a) or "neg-1")
    monkeypatch.setattr(ec, "wait_negotiation",
                        lambda n: {"negotiation_id": n, "state": "FINALIZED", "agreement_id": "ag-1"})
    monkeypatch.setattr(ec, "find_transfers", lambda aid, url: [
        {"transfer_id": "t-1", "state": "STARTED", "active": True, "contract_id": "ag-1"}] if state["running"] else [])
    monkeypatch.setattr(ec, "start_transfer", lambda **kw: state["transfers"].append(kw) or state["running"].append(1) or "t-1")
    monkeypatch.setattr(ec, "get_transfer_state", lambda t: "STARTED")
    monkeypatch.setattr(ec, "terminate_transfer", lambda t, reason: state["ended"].append(t) or True)
    return state


def test_register_a_hosted_testbed(client, monkeypatch):  # noqa: F811
    import testbed_store
    monkeypatch.setattr(testbed_store, "DATALAKE_PUBLIC_ENDPOINT_URL", "https://lake.example")
    tb = client.post("/testbeds", json={"slug": "pamak", "name": "PAMAK", "hosted": True}).json()
    assert tb["hosting"] == "facility" and tb["upload_bucket"] == f"{tb['bucket']}-upload"
    assert tb["participant_id"] == "hosted-pamak" and tb["upload_endpoint"] == "https://lake.example"
    own = client.post("/testbeds", json={"slug": "kul", "name": "KU Leuven"}).json()
    assert own["hosting"] == "own" and own["upload_bucket"] is None and own["upload_endpoint"] is None
    r = client.post("/testbeds", json={"slug": "isi", "name": "ISI", "hosted": True, "dsp_url": "https://edc.isi/protocol"})
    assert r.status_code == 422


def test_provisioning_a_hosted_testbed_starts_the_facility_transfer(client, facility):  # noqa: F811
    tb = client.post("/testbeds", json={"slug": "pamak", "name": "PAMAK", "hosted": True}).json()
    r = client.post("/testbeds/pamak/provision").json()
    steps = r["testbed"]["steps"]
    assert {k: steps[k]["status"] for k in router.HOSTED_STEPS} == dict.fromkeys(router.HOSTED_STEPS, "ok")
    assert r["testbed"]["status"] == "provisioned" and steps["transfer"]["detail"] == "started"

    up = r["issued_upload_credentials"]
    assert up["bucket"] == tb["upload_bucket"] and up["endpoint"] == "https://lake.example"
    assert up["access_key"].startswith("tb-pamak-upload-") and up["secret_key"]
    assert r["testbed"]["upload_access_key"] == up["access_key"] and up["secret_key"] not in str(r["testbed"])
    assert ("user", "pamak", tb["upload_bucket"], "upload") in client.calls

    # The facility connector is the provider, negotiating and transferring with itself.
    assert facility["assets"][tb["upload_bucket"]] == tb["upload_bucket"]
    assert facility["negotiations"][0][:3] == (FACILITY, "provider-facility", tb["upload_bucket"])
    sent = facility["transfers"][0]
    assert (sent["dsp_url"], sent["provider_id"], sent["asset_id"]) == (FACILITY, "provider-facility", tb["upload_bucket"])
    assert sent["bucket"] == tb["bucket"] and sent["access_key"] == "tb-kul-1"  # written with the Data Lake key
    # Only the testbed's own asset is listed, not the facility's other assets.
    assert [a["asset_id"] for a in client.get("/testbeds/pamak/assets").json()["assets"]] == [tb["upload_bucket"]]

    # Re-running is safe: no second key, no second transfer.
    again = client.post("/testbeds/pamak/provision").json()
    assert again["issued_upload_credentials"] is None and len(facility["transfers"]) == 1
    assert again["testbed"]["steps"]["transfer"]["detail"] == "running"


def test_the_upload_key_is_rotated_and_shown_once(client, facility):  # noqa: F811
    client.post("/testbeds", json={"slug": "pamak", "name": "PAMAK", "hosted": True})
    first = client.post("/testbeds/pamak/provision").json()["issued_upload_credentials"]
    new = client.post("/testbeds/pamak/upload-credentials/rotate").json()
    assert new["access_key"] != first["access_key"] and new["secret_key"]
    assert ("rm", first["access_key"]) in client.calls
    assert client.get("/testbeds/pamak").json()["upload_access_key"] == new["access_key"]
    assert "rotate-upload-key" in [e["action"] for e in client.get("/testbeds/pamak/audit").json()["entries"]]

    client.post("/testbeds", json={"slug": "kul", "name": "KU Leuven"})
    assert client.post("/testbeds/kul/upload-credentials/rotate").status_code == 409
    assert client.get("/testbeds/pamak/bundle").status_code == 409  # no connector to deploy


def test_deregistering_a_hosted_testbed_cleans_up_the_facility_connector(client, facility, monkeypatch):  # noqa: F811
    monkeypatch.setattr(datalake_admin, "remove_policy", lambda slug, purpose="": client.calls.append(("rmpol", purpose)))
    tb = client.post("/testbeds", json={"slug": "pamak", "name": "PAMAK", "hosted": True}).json()
    up = client.post("/testbeds/pamak/provision").json()["issued_upload_credentials"]
    r = client.delete("/testbeds/pamak").json()
    assert r["results"]["upload_key"]["status"] == "removed" and ("rm", up["access_key"]) in client.calls
    assert ("rmpol", "upload") in client.calls
    assert facility["ended"] == ["t-1"] and facility["deleted"] == [tb["upload_bucket"]]
    assert r["results"]["asset"]["status"] == "deleted" and not any("upload asset" in f for f in r["followups"])


def test_a_hosted_testbed_has_no_dsp_endpoint_to_edit(client):  # noqa: F811
    client.post("/testbeds", json={"slug": "pamak", "name": "PAMAK", "hosted": True})
    assert client.patch("/testbeds/pamak", json={"dsp_url": "https://edc.pamak/protocol"}).status_code == 422


def test_upload_asset_request_shape(monkeypatch):
    import httpx
    seen = []

    def handler(request):
        seen.append((request.url.path, request.headers.get("x-api-key"), request.read()))
        return httpx.Response(200, json={})
    real = httpx.Client
    monkeypatch.setattr(edc_client.httpx, "Client", lambda **kw: real(transport=httpx.MockTransport(handler), **kw))
    for name, value in (("EDC_PROVIDER_MANAGEMENT_URL", "http://edc/management"), ("EDC_API_KEY", "k"),
                        ("DATASPACE_S3_ENDPOINT_URL", "http://lake:9000"), ("DATASPACE_S3_ACCESS_KEY", "svc"),
                        ("DATASPACE_S3_SECRET_KEY", "svc-secret")):
        monkeypatch.setattr(edc_client, name, value)
    assert edc_client.register_upload_asset("6g-dali-pamak-upload", "6g-dali-pamak-upload", "PAMAK uploads") == "created"
    path, key, body = seen[-1]
    assert path == "/management/v3/assets" and key == "k"
    assert b'"type":"6GDaliTestbedExperiments"' in body.replace(b" ", b"")
    assert b'"bucketName":"6g-dali-pamak-upload"' in body.replace(b" ", b"") and b"svc-secret" in body


def test_delete_asset_sends_the_api_key(monkeypatch):
    import httpx
    seen = []
    real = httpx.AsyncClient
    monkeypatch.setattr(edc_client.httpx, "AsyncClient", lambda **kw: real(
        transport=httpx.MockTransport(lambda r: seen.append(r.headers.get("x-api-key")) or httpx.Response(204)), **kw))
    monkeypatch.setattr(edc_client, "EDC_PROVIDER_MANAGEMENT_URL", "http://edc/management")
    monkeypatch.setattr(edc_client, "EDC_API_KEY", "k")
    assert asyncio.run(edc_client.delete_asset("a"))["status"] == "deleted" and seen == ["k"]
