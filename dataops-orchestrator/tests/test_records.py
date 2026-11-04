"""The catalogue's linked-data view: a record's RDF, read from piveau as the caller."""
import httpx

from routers import records
from test_testbeds import client  # noqa: F401  (client is a fixture)

DS = "99782f7a-f3ea-4d7a-a916-3cb43f0244ce"
TURTLE = f"<https://dataspace.6gdali.eu/set/data/{DS}> a <http://www.w3.org/ns/dcat#Dataset> .\n"


class _Piveau:
    """Stands in for piveau-hub-repo: private records answer 403 without a token."""

    def __init__(self, status=200, private=False):
        self.status, self.private, self.calls = status, private, []

    def install(self, monkeypatch):
        piveau = self

        def handler(request: httpx.Request) -> httpx.Response:
            piveau.calls.append(request)
            if piveau.private and "authorization" not in request.headers:
                return httpx.Response(403)
            return httpx.Response(piveau.status, text=TURTLE, headers={"content-type": request.headers["accept"] + "; charset=utf-8"})

        real = httpx.AsyncClient
        monkeypatch.setattr(records.httpx, "AsyncClient",
                            lambda **kw: real(transport=httpx.MockTransport(handler), **kw))
        monkeypatch.setattr(records, "PIVEAU_HUB_URL", "http://piveau-repo:8080")


def test_a_private_record_is_read_with_the_callers_token(client, monkeypatch):
    piveau = _Piveau(private=True)
    piveau.install(monkeypatch)

    r = client.get(f"/records/{DS}", params={"format": "ttl", "catalogue": "6g-dali-hesso"},
                   headers={"Authorization": "Bearer t-ann"})

    assert r.status_code == 200, r.text
    assert r.text == TURTLE
    assert r.headers["content-type"].startswith("text/turtle")
    assert r.headers["cache-control"] == "private, no-store"
    sent = piveau.calls[0]
    assert sent.headers["authorization"] == "Bearer t-ann"  # as the caller, not with the orchestrator's key
    assert "x-api-key" not in sent.headers
    assert sent.url.params["catalogue"] == "6g-dali-hesso"


def test_the_format_picks_the_serialisation(client, monkeypatch):
    piveau = _Piveau()
    piveau.install(monkeypatch)

    r = client.get(f"/records/{DS}", params={"format": "jsonld"})

    assert r.status_code == 200
    assert piveau.calls[0].headers["accept"] == "application/ld+json"
    assert r.headers["content-type"].startswith("application/ld+json")


def test_a_record_the_caller_cannot_see_is_not_found(client, monkeypatch):
    _Piveau(private=True).install(monkeypatch)
    assert client.get(f"/records/{DS}").status_code == 404


def test_a_token_piveau_rejects_asks_for_a_new_sign_in(client, monkeypatch):
    _Piveau(status=401).install(monkeypatch)
    assert client.get(f"/records/{DS}", headers={"Authorization": "Bearer stale"}).status_code == 401


def test_an_unknown_format_is_refused(client, monkeypatch):
    piveau = _Piveau()
    piveau.install(monkeypatch)
    assert client.get(f"/records/{DS}", params={"format": "pdf"}).status_code == 422
    assert piveau.calls == []
