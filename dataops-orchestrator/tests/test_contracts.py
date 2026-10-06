"""Contract negotiation, agreement lookup and transfer start for a testbed's assets."""
import httpx
from fastapi import HTTPException

from test_testbeds import CATALOG_ONE, KUL_URL, TRANSFERS, client  # noqa: F401  (client is a fixture)

OFFER = "Y29udHJhY3Q=:a3VsLWV4cA==:abc"


def _discovered(client, monkeypatch, provision=False):
    import edc_consumer_client as ec
    client.post("/testbeds", json={"slug": "kul", "name": "KU Leuven", "dsp_url": KUL_URL})
    monkeypatch.setattr(ec, "fetch_catalog", lambda u, p: ec.parse_catalog(CATALOG_ONE))
    client.post("/testbeds/kul/assets/discover")
    if provision:
        client.post("/testbeds/kul/provision")
    return ec


def _agreed(client, monkeypatch):
    ec = _discovered(client, monkeypatch, provision=True)
    monkeypatch.setattr(ec, "start_negotiation", lambda *a: "neg-1")
    monkeypatch.setattr(ec, "wait_negotiation",
                        lambda n: {"negotiation_id": n, "state": "FINALIZED", "agreement_id": "ag-1"})
    client.post("/testbeds/kul/assets/kul-experiments-1/negotiate")
    return ec


def test_parse_agreements_newest_first_and_provider_filter():
    import edc_consumer_client as ec
    items = [{"@id": "old", "providerId": "provider-kul", "contractSigningDate": 100},
             {"@id": "new", "providerId": "provider-kul", "contractSigningDate": 200},
             {"@id": "other", "providerId": "provider-isi", "contractSigningDate": 300}]
    assert [a["agreement_id"] for a in ec.parse_agreements(items, "provider-kul")] == ["new", "old"]


def test_find_reports_and_stores_the_negotiated_contract(client, monkeypatch):
    ec = _discovered(client, monkeypatch)
    monkeypatch.setattr(ec, "find_agreements",
                        lambda a, p: [{"agreement_id": "ag-7", "provider_id": p, "signing_date": 1}])
    monkeypatch.setattr(ec, "get_agreement_negotiation",
                        lambda ag: {"negotiation_id": "neg-7", "state": "FINALIZED", "agreement_id": ag})
    monkeypatch.setattr(ec, "find_transfers", lambda a, u: [])
    r = client.post("/testbeds/kul/assets/kul-experiments-1/transfers/find").json()
    assert r["agreement"] == {"agreement_id": "ag-7", "negotiation_id": "neg-7", "negotiation_state": "FINALIZED"}
    assert r["asset"]["status"] == "agreed" and r["asset"]["contract_agreement_id"] == "ag-7"
    assert client.get("/testbeds/kul/assets").json()["assets"][0]["negotiation_state"] == "FINALIZED"


def test_find_still_works_when_contract_lookup_fails(client, monkeypatch):
    ec = _discovered(client, monkeypatch)

    def boom(*a):
        raise HTTPException(status_code=502, detail="x")
    monkeypatch.setattr(ec, "find_agreements", boom)
    monkeypatch.setattr(ec, "find_transfers", lambda a, u: ec.parse_transfers(TRANSFERS, u))
    r = client.post("/testbeds/kul/assets/kul-experiments-1/transfers/find")
    assert r.status_code == 200 and r.json()["asset"]["contract_agreement_id"] == "ag-1"  # taken from the transfer


def test_negotiate_flow(client, monkeypatch):
    ec = _discovered(client, monkeypatch)
    started = []
    monkeypatch.setattr(ec, "start_negotiation", lambda *a: started.append(a) or "neg-1")
    monkeypatch.setattr(ec, "wait_negotiation",
                        lambda n: {"negotiation_id": n, "state": "FINALIZED", "agreement_id": "ag-1"})
    r = client.post("/testbeds/kul/assets/kul-experiments-1/negotiate").json()
    assert r["result"] == "agreed" and r["asset"]["status"] == "agreed"
    assert r["agreement"] == {"agreement_id": "ag-1", "negotiation_id": "neg-1", "negotiation_state": "FINALIZED"}
    assert started == [(KUL_URL, "provider-kul", "kul-experiments-1", OFFER)]
    again = client.post("/testbeds/kul/assets/kul-experiments-1/negotiate").json()
    assert again["result"] == "already_agreed" and len(started) == 1


def test_negotiate_failure_and_in_progress(client, monkeypatch):
    ec = _discovered(client, monkeypatch)
    starts = []
    monkeypatch.setattr(ec, "start_negotiation", lambda *a: starts.append(1) or f"neg-{len(starts)}")
    monkeypatch.setattr(ec, "wait_negotiation",
                        lambda n: {"negotiation_id": n, "state": "REQUESTED", "agreement_id": None})
    r = client.post("/testbeds/kul/assets/kul-experiments-1/negotiate").json()
    assert r["result"] == "in_progress" and r["asset"]["status"] == "negotiating"
    client.post("/testbeds/kul/assets/kul-experiments-1/negotiate")  # continues, does not start another
    assert len(starts) == 1

    monkeypatch.setattr(ec, "wait_negotiation",
                        lambda n: {"negotiation_id": n, "state": "TERMINATED", "agreement_id": None})
    assert client.post("/testbeds/kul/assets/kul-experiments-1/negotiate").json()["result"] == "failed"
    client.post("/testbeds/kul/assets/kul-experiments-1/negotiate")  # terminated: a fresh attempt is allowed
    assert len(starts) == 2


def test_negotiate_needs_an_offer(client, monkeypatch):
    ec = _discovered(client, monkeypatch)
    monkeypatch.setattr(ec, "fetch_catalog", lambda u, p: [])
    client.post("/testbeds/kul/assets/discover")  # the asset is no longer offered
    assert client.post("/testbeds/kul/assets/kul-experiments-1/negotiate").status_code == 409


def test_start_transfer_preconditions(client, monkeypatch):
    import piveau_dataset_client as pdc
    monkeypatch.setattr(pdc, "PIVEAU_HUB_URL", "https://piveau.example")
    monkeypatch.setattr(pdc, "PIVEAU_API_KEY", "pk")
    _discovered(client, monkeypatch)  # discovered, not provisioned, no contract
    r = client.post("/testbeds/kul/assets/kul-experiments-1/transfers/start")
    assert r.status_code == 409 and "Data Lake key" in r.json()["detail"]
    client.post("/testbeds/kul/provision")
    r = client.post("/testbeds/kul/assets/kul-experiments-1/transfers/start")
    assert r.status_code == 409 and "contract" in r.json()["detail"]


def test_start_transfer_uses_scoped_key_and_blocks_duplicates(client, monkeypatch):
    import piveau_dataset_client as pdc
    from routers import testbeds as router
    monkeypatch.setattr(pdc, "PIVEAU_HUB_URL", "https://piveau.example/")
    monkeypatch.setattr(pdc, "PIVEAU_API_KEY", "pk")
    monkeypatch.setattr(router, "DATALAKE_PUBLIC_ENDPOINT_URL", "http://lake:9000")
    ec = _agreed(client, monkeypatch)
    sent = {}
    monkeypatch.setattr(ec, "find_transfers", lambda a, u: [])
    monkeypatch.setattr(ec, "start_transfer", lambda **kw: sent.update(kw) or "t-new")
    monkeypatch.setattr(ec, "get_transfer_state", lambda t: "STARTED")
    r = client.post("/testbeds/kul/assets/kul-experiments-1/transfers/start").json()
    assert r["result"] == "started" and r["asset"]["status"] == "transferring" and r["asset"]["transfer_id"] == "t-new"
    assert (sent["access_key"], sent["secret_key"], sent["bucket"]) == ("tb-kul-1", "s3cret", "6g-dali-kul")
    assert sent["endpoint"] == "http://lake:9000" and sent["piveau_url"] == "https://piveau.example/datasets"
    assert sent["agreement_id"] == "ag-1" and sent["provider_id"] == "provider-kul" and sent["dsp_url"] == KUL_URL

    monkeypatch.setattr(ec, "find_transfers", lambda a, u: ec.parse_transfers(TRANSFERS, u))  # one already STARTED
    sent.clear()
    r = client.post("/testbeds/kul/assets/kul-experiments-1/transfers/start").json()
    assert r["result"] == "already_running" and sent == {}


def test_contract_and_transfer_request_shapes(monkeypatch):
    import edc_consumer_client as ec
    seen = []

    def fake(method, url, json=None, headers=None, timeout=None):
        seen.append((method, url, json))
        return httpx.Response(200, json={"@id": "x-1"})
    monkeypatch.setattr(ec, "EDC_PROVIDER_MANAGEMENT_URL", "https://edc.example/management")
    monkeypatch.setattr(ec.httpx, "request", fake)
    assert ec.start_negotiation(KUL_URL, "provider-kul", "a1", "offer-1") == "x-1"
    method, url, body = seen[-1]
    assert (method, url) == ("POST", "https://edc.example/management/v3/contractnegotiations")
    assert body["policy"]["@id"] == "offer-1" and body["policy"]["odrl:target"] == {"@id": "a1"}
    assert body["providerId"] == "provider-kul"
    ec.start_transfer(dsp_url=KUL_URL, provider_id="provider-kul", asset_id="a1", agreement_id="ag", endpoint="http://l",
                      bucket="b", access_key="ak", secret_key="sk", piveau_url="https://p/datasets",
                      piveau_api_key="k")
    body = seen[-1][2]
    assert body["transferType"] == "PiveauData-PUSH" and body["contractId"] == "ag"
    assert body["connectorId"] == "provider-kul"
    assert body["dataDestination"]["type"] == "PiveauData" and body["dataDestination"]["accessKey"] == "ak"


def test_unreachable_connector_names_the_url_and_is_logged(monkeypatch, caplog):
    import logging
    import pytest
    import edc_consumer_client as ec
    monkeypatch.setattr(ec, "EDC_PROVIDER_MANAGEMENT_URL", "http://6gdali-facility-edc:20001/management")

    def refuse(*a, **k):
        raise httpx.ConnectError("[Errno -2] Name or service not known")
    monkeypatch.setattr(ec.httpx, "request", refuse)
    monkeypatch.setattr(ec.httpx, "post", refuse)
    for call in (lambda: ec.start_negotiation(KUL_URL, "provider-kul", "a", "o"),
                 lambda: ec.fetch_catalog(KUL_URL, "provider-kul"),
                 lambda: ec.find_transfers("a", KUL_URL)):
        caplog.clear()
        with caplog.at_level(logging.WARNING, logger="edc_consumer_client"):
            with pytest.raises(HTTPException) as e:
                call()
        assert "http://6gdali-facility-edc:20001/management" in e.value.detail
        assert "Name or service not known" in e.value.detail and "ConnectError" in e.value.detail
        assert "6gdali-facility-edc" in caplog.text


def test_a_rejected_request_reports_the_connectors_answer(monkeypatch):
    import pytest
    import edc_consumer_client as ec
    monkeypatch.setattr(ec, "EDC_PROVIDER_MANAGEMENT_URL", "http://c/management")
    monkeypatch.setattr(ec.httpx, "request", lambda *a, **k: httpx.Response(400, text='[{"message":"bad policy"}]'))
    with pytest.raises(HTTPException) as e:
        ec.start_negotiation(KUL_URL, "provider-kul", "a", "o")
    assert "Contract negotiation failed: 400" in e.value.detail and "bad policy" in e.value.detail
