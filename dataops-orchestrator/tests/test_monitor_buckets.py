"""The bucket list the s3-asset-monitor reads instead of a MONITOR_BUCKETS variable."""
from routers import monitor, testbeds
from test_testbeds import client  # noqa: F401  (client is a fixture)

TOKEN = "monitor-secret"


def _ask(client, token=TOKEN):
    return client.get("/monitor/buckets", headers={"X-Monitor-Token": token} if token is not None else {})


def test_the_list_is_disabled_until_a_token_is_configured(client, monkeypatch):
    monkeypatch.setattr(monitor, "MONITOR_API_TOKEN", "")
    assert _ask(client).status_code == 503


def test_it_needs_the_token(client, monkeypatch):
    monkeypatch.setattr(monitor, "MONITOR_API_TOKEN", TOKEN)
    assert _ask(client, None).status_code == 401
    assert _ask(client, "wrong").status_code == 401
    assert _ask(client).status_code == 200


def test_only_provisioned_testbeds_are_listed(client, monkeypatch):
    monkeypatch.setattr(monitor, "MONITOR_API_TOKEN", TOKEN)
    for slug in ("kul", "isi", "sparks"):
        client.post("/testbeds", json={"slug": slug, "name": slug})
    client.post("/testbeds/kul/provision")      # provisioned
    client.post("/testbeds/sparks/provision")   # provisioned, hosted below
    client.claims.clear()
    client.claims.update({"preferred_username": "ann", "sub": "u-ann", "email": "ann@kul.be"})
    assert client.post("/testbeds/requests", json={"slug": "wanted", "name": "Wanted"}).status_code == 201

    assert _ask(client).json() == {"buckets": ["6g-dali-kul", "6g-dali-sparks"], "total": 2}  # isi: draft, wanted: request


def test_a_hosted_testbed_contributes_its_data_bucket_not_its_upload_bucket(client, monkeypatch):
    monkeypatch.setattr(monitor, "MONITOR_API_TOKEN", TOKEN)
    client.post("/testbeds", json={"slug": "sparks", "name": "Sparks", "hosted": True})
    client.post("/testbeds/sparks/provision")
    buckets = _ask(client).json()["buckets"]
    assert "6g-dali-sparks-upload" not in buckets
    assert buckets in ([], ["6g-dali-sparks"])  # listed once its hosted steps have all succeeded


def test_the_manual_followup_goes_away_once_the_monitor_follows_the_registry(client, monkeypatch):
    client.post("/testbeds", json={"slug": "kul", "name": "KU Leuven"})
    monkeypatch.setattr(testbeds, "MONITOR_API_TOKEN", TOKEN)
    assert client.post("/testbeds/kul/provision").json()["followups"] == []
