"""A user asks for a testbed; an administrator approves it and the requester becomes its owner."""
import pytest
from fastapi import HTTPException

import keycloak_admin
from test_testbeds import client  # noqa: F401  (client is a fixture)

ADMIN = {"preferred_username": "root", "sub": "u-admin", "realm_access": {"roles": ["testbed-admin"]}}
ANN = {"preferred_username": "ann", "sub": "u-ann", "email": "ann@kul.be"}
BOB = {"preferred_username": "bob", "sub": "u-bob", "email": "bob@kul.be"}


def _as(client, claims):
    client.claims.clear()
    client.claims.update(claims)


def _request(client, slug="kul", **extra):
    return client.post("/testbeds/requests", json={"slug": slug, "name": "KU Leuven", **extra})


def _ann_requests(client, **extra):
    _as(client, ANN)
    return _request(client, **extra)


def test_a_request_is_a_testbed_row_that_provisions_nothing(client):
    _as(client, ANN)
    r = _request(client, organisation="KUL", note="Wireless lab")
    assert r.status_code == 201
    tb = r.json()
    assert tb["status"] == "requested" and tb["requested_by_email"] == "ann@kul.be"
    assert tb["request_note"] == "Wireless lab" and tb["contact_email"] == "ann@kul.be"
    assert tb["bucket"] == "6g-dali-kul" and tb["participant_id"] == "provider-kul"  # reserved, not created
    assert client.calls == []
    _as(client, ADMIN)
    assert client.get("/testbeds/kul/audit").json()["entries"][0]["action"] == "request"


@pytest.mark.parametrize("slug", ["kul-leuven", "1kul", "Kul", "ku", "k" * 33, "kul_1", "kul.be", "-kul"])
def test_a_slug_is_lowercase_letters_and_digits_starting_with_a_letter(client, slug):
    _as(client, ANN)
    assert _request(client, slug=slug).status_code == 422
    _as(client, ADMIN)
    assert client.post("/testbeds", json={"slug": slug, "name": "x"}).status_code == 422


@pytest.mark.parametrize("slug", ["kul", "kul2", "k" * 32, "eurecom5g"])
def test_valid_slugs_are_accepted(client, slug):
    _as(client, ANN)
    assert _request(client, slug=slug).status_code == 201


def test_requests_are_checked_like_registrations(client):
    _as(client, ANN)
    assert _request(client, slug="Bad Slug").status_code == 422
    assert _request(client, hosted=True, dsp_url="https://x.example/protocol").status_code == 422
    assert _request(client).status_code == 201
    _as(client, BOB)
    assert _request(client).status_code == 409  # the slug is taken by the pending request


def test_administrators_may_request_too_and_an_email_is_not_needed(client):
    _as(client, {**ADMIN, "email": "root@kul.be"})
    assert _request(client).status_code == 201
    _as(client, {"preferred_username": "nomail", "sub": "u-x"})
    r = _request(client, slug="other")
    assert r.status_code == 201 and r.json()["contact_email"] is None


def test_pending_requests_are_capped_per_user(client):
    _as(client, ANN)
    for slug in ("aaa", "bbb", "ccc"):
        assert _request(client, slug=slug).status_code == 201
    assert _request(client, slug="ddd").status_code == 429


def test_the_requester_sees_their_request_and_nobody_else_does(client):
    _ann_requests(client)
    assert [t["slug"] for t in client.get("/testbeds").json()["testbeds"]] == ["kul"]
    assert client.get("/testbeds/kul").json()["status"] == "requested"

    _as(client, BOB)
    assert client.get("/testbeds").json()["total"] == 0
    assert client.get("/testbeds/kul").status_code == 403

    _as(client, ADMIN)
    assert client.get("/testbeds").json()["total"] == 1


def test_a_requester_can_do_nothing_else_with_the_request(client):
    _ann_requests(client)
    for path in ("/assets", "/audit", "/bundle"):
        assert client.get(f"/testbeds/kul{path}").status_code == 403
    for path in ("/provision", "/approve", "/reject"):
        assert client.post(f"/testbeds/kul{path}").status_code == 403


def test_a_request_cannot_be_provisioned_before_it_is_approved(client):
    _ann_requests(client)
    _as(client, ADMIN)
    assert client.post("/testbeds/kul/provision").status_code == 409
    assert client.calls == []


def test_approving_makes_the_requester_the_owner_and_provisions(client, monkeypatch):
    added = []
    monkeypatch.setattr(keycloak_admin, "configured", lambda: True)
    monkeypatch.setattr(keycloak_admin, "add_member_by_id", lambda slug, user_id: added.append((slug, user_id)))
    monkeypatch.setattr(keycloak_admin, "ensure_group", lambda slug: "exists")
    _ann_requests(client)

    _as(client, ADMIN)
    r = client.post("/testbeds/kul/approve", json={"note": "Welcome"})
    assert r.status_code == 200
    assert added == [("kul", "u-ann")]  # by Keycloak user id: no e-mail lookup to go wrong
    assert r.json()["testbed"]["status"] == "provisioned" and r.json()["testbed"]["decision_note"] == "Welcome"
    assert ("bucket", "6g-dali-kul") in client.calls and ("user", "kul", "6g-dali-kul") in client.calls
    assert client.post("/testbeds/kul/approve").status_code == 409  # only once

    # Ann is now in the group, as Keycloak would put it in her token: she owns it and can use it.
    _as(client, {**ANN, "groups": ["/testbeds/kul"]})
    assert client.get("/testbeds/kul").status_code == 200
    assert client.get("/testbeds/kul/bundle").status_code == 200
    assert client.post("/testbeds/kul/provision").status_code == 403  # still the administrators' action


def test_an_approval_that_cannot_add_the_owner_changes_nothing(client, monkeypatch):
    def keycloak_down(slug, user_id):
        raise HTTPException(status_code=502, detail="Keycloak: adding the user failed")
    monkeypatch.setattr(keycloak_admin, "configured", lambda: True)
    monkeypatch.setattr(keycloak_admin, "add_member_by_id", keycloak_down)
    _ann_requests(client)

    _as(client, ADMIN)
    assert client.post("/testbeds/kul/approve").status_code == 424
    assert client.get("/testbeds/kul").json()["status"] == "requested"
    assert client.calls == []


def test_without_a_keycloak_client_the_approval_says_to_add_the_owner_by_hand(client, monkeypatch):
    monkeypatch.setattr(keycloak_admin, "configured", lambda: False)
    _ann_requests(client)
    _as(client, ADMIN)
    r = client.post("/testbeds/kul/approve")
    assert r.status_code == 200
    assert any("ann@kul.be" in f for f in r.json()["followups"])


def test_a_rejected_request_keeps_its_reason_and_cannot_be_approved(client):
    _ann_requests(client)
    _as(client, ADMIN)
    r = client.post("/testbeds/kul/reject", json={"note": "Use the KUL testbed"})
    assert r.status_code == 200 and r.json()["status"] == "rejected"
    assert client.post("/testbeds/kul/approve").status_code == 409
    assert client.post("/testbeds/kul/reject").status_code == 409

    _as(client, ANN)
    tb = client.get("/testbeds/kul").json()
    assert tb["status"] == "rejected" and tb["decision_note"] == "Use the KUL testbed"


def test_withdrawing_frees_the_slug_and_only_for_requests(client, monkeypatch):
    _ann_requests(client)
    _as(client, BOB)
    assert client.delete("/testbeds/kul/request").status_code == 403
    _as(client, ANN)
    assert client.delete("/testbeds/kul/request").json()["status"] == "withdrawn"
    assert client.get("/testbeds").json()["total"] == 0
    assert _request(client).status_code == 201  # the slug is free again

    monkeypatch.setattr(keycloak_admin, "configured", lambda: False)
    _as(client, ADMIN)
    client.post("/testbeds/kul/approve")
    _as(client, {**ANN, "groups": ["/testbeds/kul"]})
    assert client.delete("/testbeds/kul/request").status_code == 403  # approved: no longer a request
    _as(client, ADMIN)
    assert client.delete("/testbeds/kul/request").status_code == 409
    assert client.delete("/testbeds/none/request").status_code == 404


@pytest.mark.parametrize("status", ["requested", "rejected"])
def test_a_request_is_visible_to_its_requester_in_either_state(client, status):
    _ann_requests(client)
    if status == "rejected":
        _as(client, ADMIN)
        client.post("/testbeds/kul/reject")
        _as(client, ANN)
    assert client.get("/testbeds/kul").json()["status"] == status
