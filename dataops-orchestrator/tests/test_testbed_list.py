"""The testbed list carries each testbed's assets, for the lifecycle levels the list shows."""
import testbed_store
from test_testbeds import client  # noqa: F401  (client is a fixture)


def _slugs(response):
    return [t["slug"] for t in response.json()["testbeds"]]


def test_each_testbed_comes_with_its_own_assets(client):
    for slug in ("kul", "isi"):
        client.post("/testbeds", json={"slug": slug, "name": slug.upper()})
    testbed_store.sync_assets("kul", [{"asset_id": "a1", "title": "A1"}, {"asset_id": "a2", "title": "A2"}])
    testbed_store.record_contract("kul", "a1", "n1", "FINALIZED", "ag-1")

    by_slug = {t["slug"]: t for t in client.get("/testbeds").json()["testbeds"]}
    assert [a["asset_id"] for a in by_slug["kul"]["assets"]] == ["a1", "a2"]
    assert by_slug["kul"]["assets"][0]["contract_agreement_id"] == "ag-1"
    assert by_slug["kul"]["assets"][0]["present"] is True
    assert by_slug["isi"]["assets"] == []


def test_an_owner_gets_only_their_testbeds_assets(client):
    for slug in ("kul", "isi"):
        client.post("/testbeds", json={"slug": slug, "name": slug.upper()})
        testbed_store.sync_assets(slug, [{"asset_id": f"{slug}-a", "title": slug}])
    client.claims.clear()
    client.claims.update({"preferred_username": "ann", "sub": "u-ann", "groups": ["/testbeds/kul"]})

    listed = client.get("/testbeds").json()["testbeds"]
    assert [t["slug"] for t in listed] == ["kul"]
    assert [a["asset_id"] for a in listed[0]["assets"]] == ["kul-a"]
