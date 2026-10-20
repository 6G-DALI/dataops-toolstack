"""The datasets of a testbed's catalogue: listing them and editing their fields and visibility in piveau."""
import httpx
import pytest
from rdflib import Graph
from rdflib.namespace import DCAT, DCTERMS

import piveau_access
import piveau_dataset_client as pdc
import testbed_datasets as td
from test_testbeds import _act_as_owner, client  # noqa: F401  (client is a fixture)

DS = "exp-1"
TURTLE = """@prefix dcat: <http://www.w3.org/ns/dcat#> . @prefix dct: <http://purl.org/dc/terms/> .
<https://dataspace.6gdali.eu/datasets/exp-1> a dcat:Dataset ;
  dct:title "Edge AI inference"@en ; dct:description "Three experiments"@en ;
  dcat:keyword "5G"@en ; dct:license <https://creativecommons.org/licenses/by/4.0/> ;
  dct:accessRights <http://publications.europa.eu/resource/authority/access-right/PUBLIC> ;
  dct:modified "2026-10-09" ;
  dcat:distribution <https://dataspace.6gdali.eu/distributions/d1> .
<https://dataspace.6gdali.eu/distributions/d1> a dcat:Distribution ; dct:title "run-1.csv"@en ;
  <https://dali-project.eu/ns#assetId> "run-1" .
"""


@pytest.fixture
def hub(monkeypatch):
    """A fake piveau-hub-repo holding one dataset; it keeps the access level apart, in the catalogue record."""
    state = {"ttl": {DS: TURTLE}, "level": {DS: "public"}, "puts": []}

    def handler(request: httpx.Request) -> httpx.Response:
        path, q = request.url.path, request.url.params
        assert request.headers["x-api-key"] == "key"
        if path == "/datasets" and request.method == "GET":
            return httpx.Response(200, json=list(state["ttl"]))
        ds = path.split("/")[2]
        if ds not in state["ttl"]:
            return httpx.Response(404)
        if path.endswith("/record"):
            return httpx.Response(200, text=f"<r> <https://piveau.eu/ns/voc#accessLevel> "
                                            f"<https://piveau.eu/ns/voc#{state['level'][ds]}> .")
        if request.method == "PUT":
            state["puts"].append(dict(q))
            state["ttl"][ds] = request.content.decode()
            if "accessLevel" in q:
                state["level"][ds] = q["accessLevel"]
            return httpx.Response(204)
        return httpx.Response(200, text=state["ttl"][ds])

    transport = httpx.MockTransport(handler)
    monkeypatch.setattr(td.httpx, "request", lambda method, url, **kw: httpx.Client(transport=transport).request(
        method, url, **{k: v for k, v in kw.items() if k != "timeout"}))
    monkeypatch.setattr(pdc, "PIVEAU_HUB_URL", "https://hub.example")
    monkeypatch.setattr(pdc, "PIVEAU_API_KEY", "key")
    monkeypatch.setattr(piveau_access, "enabled", lambda: True)
    monkeypatch.setattr(piveau_access, "catalogue_level", lambda c: "public")
    return state


def test_list_reads_fields_level_and_files(hub):
    [d] = td.list_datasets("6g-dali-pamak")
    assert (d["id"], d["title"], d["access_level"], d["access_rights"], d["distributions"]) == (
        DS, "Edge AI inference", "public", "PUBLIC", 1)
    assert d["keywords"] == ["5G"] and d["license"] == "https://creativecommons.org/licenses/by/4.0/"


def test_an_edit_keeps_the_files_and_sets_visibility(hub):
    d = td.update("6g-dali-pamak", DS, {"title": " New title ", "keywords": ["nitos", "5G", "nitos", " "],
                                         "access_level": "internal"})
    assert d["title"] == "New title" and d["keywords"] == ["5G", "nitos"]
    assert d["access_level"] == "internal" and d["access_rights"] == "NON_PUBLIC"
    assert d["distributions"] == 1  # the whole graph went back: the distribution is still there
    g = Graph().parse(data=hub["ttl"][DS], format="turtle")
    assert len(list(g.subjects(None, DCAT.Distribution))) == 1
    assert hub["puts"][-1] == {"catalogue": "6g-dali-pamak", "accessLevel": "internal", "publicationStatus": "published"}

    d = td.update("6g-dali-pamak", DS, {"description": "Only this"})
    assert d["description"] == "Only this" and d["title"] == "New title" and d["access_level"] == "internal"
    assert "accessLevel" not in hub["puts"][-1]  # without a visibility change the level is left alone

    d = td.update("6g-dali-pamak", DS, {"license": ""})
    assert d["license"] == "" and not list(Graph().parse(data=hub["ttl"][DS], format="turtle").objects(None, DCTERMS.license))


def test_edits_that_are_refused(hub, monkeypatch):
    with pytest.raises(Exception) as e:
        td.update("6g-dali-pamak", DS, {"title": "  "})
    assert e.value.status_code == 422
    with pytest.raises(Exception) as e:
        td.update("6g-dali-pamak", DS, {"license": "CC-BY-4.0"})
    assert e.value.status_code == 422
    monkeypatch.setattr(piveau_access, "catalogue_level", lambda c: "internal")
    assert td.allowed_levels("6g-dali-pamak") == ("public", "restricted", "internal")
    # An administrator may publish a dataset of a private catalogue.
    assert td.update("6g-dali-pamak", DS, {"access_level": "public"})["access_level"] == "public"
    with pytest.raises(Exception) as e:
        td.update("6g-dali-pamak", DS, {"access_level": "secret"})
    assert e.value.status_code == 422
    hub["puts"].clear()
    monkeypatch.setattr(piveau_access, "enabled", lambda: False)
    with pytest.raises(Exception) as e:
        td.update("6g-dali-pamak", DS, {"access_level": "internal"})
    assert e.value.status_code == 409
    assert not hub["puts"]
    with pytest.raises(Exception) as e:
        td.update("6g-dali-pamak", "nope", {"title": "x"})
    assert e.value.status_code == 404


def test_testbed_endpoints(client, hub):  # noqa: F811
    cat = client.post("/testbeds", json={"slug": "pamak", "name": "PAMAK"}).json()["catalogue_id"]
    client.post("/testbeds", json={"slug": "kul", "name": "KU Leuven"})
    r = client.get("/testbeds/pamak/datasets").json()
    assert r["catalogue_id"] == cat and r["levels"] == ["public", "restricted", "internal"] and r["total"] == 1
    assert r["default_level"] == "internal"
    one = client.get(f"/testbeds/pamak/datasets/{DS}").json()
    assert one["dataset"]["title"] == "Edge AI inference" and one["levels"] == r["levels"]
    assert client.get("/testbeds/pamak/datasets/nope").status_code == 404

    _act_as_owner(client, "/testbeds/pamak")  # the testbed's own administrators may edit
    edited = client.patch(f"/testbeds/pamak/datasets/{DS}", json={"title": "Owner edit", "access_level": "restricted"})
    assert edited.status_code == 200 and edited.json()["access_level"] == "restricted"
    assert client.patch(f"/testbeds/kul/datasets/{DS}", json={"title": "x"}).status_code == 403
    assert client.patch(f"/testbeds/pamak/datasets/{DS}", json={}).status_code == 422
    assert client.patch(f"/testbeds/pamak/datasets/{DS}", json={"access_level": "secret"}).status_code == 422
    entries = client.get("/testbeds/pamak/audit").json()["entries"]
    assert any(e["action"] == "edit-dataset" and "access_level" in e["detail"] for e in entries)
