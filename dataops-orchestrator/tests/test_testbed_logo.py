"""A testbed's logo as an uploaded file: kept by the orchestrator, served publicly, and its address set in piveau."""
import pytest
from fastapi.testclient import TestClient

import logo_image
import piveau_catalogue_client
from main import app
from routers import testbeds
from test_testbeds import _act_as_owner, client  # noqa: F401  (client is a fixture)

BASE = "https://dataops.example/api"
PNG = b"\x89PNG\r\n\x1a\n" + b"\x00" * 64
PNG2 = b"\x89PNG\r\n\x1a\n" + b"\x01" * 64
JPEG = b"\xff\xd8\xff\xe0" + b"\x00" * 64
GIF = b"GIF89a" + b"\x00" * 64
WEBP = b"RIFF\x00\x00\x00\x00WEBPVP8 " + b"\x00" * 64


@pytest.fixture
def logos(client, monkeypatch):
    monkeypatch.setattr(testbeds, "ORCHESTRATOR_PUBLIC_URL", BASE)
    sent = []
    monkeypatch.setattr(piveau_catalogue_client, "set_catalogue_logo",
                        lambda catalogue, url: sent.append((catalogue, url)) or "set")
    client.post("/testbeds", json={"slug": "kul", "name": "KU Leuven"})
    client.sent = sent
    return client


def _upload(c, data=PNG, name="logo.png", slug="kul", ctype="image/png"):
    return c.post(f"/testbeds/{slug}/logo", files={"file": (name, data, ctype)})


def test_sniff_reads_the_type_from_the_bytes():
    assert logo_image.sniff(PNG) == "image/png"
    assert logo_image.sniff(JPEG) == "image/jpeg"
    assert logo_image.sniff(GIF) == "image/gif"
    assert logo_image.sniff(WEBP) == "image/webp"
    assert logo_image.sniff(b"<svg xmlns='http://www.w3.org/2000/svg'/>") is None
    assert logo_image.sniff(b"just text") is None
    assert logo_image.sniff(b"") is None


def test_an_uploaded_logo_gets_a_public_address_that_is_set_in_piveau(logos):
    r = _upload(logos)
    assert r.status_code == 200
    url = r.json()["logo_url"]
    assert url.startswith(f"{BASE}/testbeds/kul/logo?v=") and len(url.split("?v=")[1]) == 12
    assert logos.sent == [("6g-dali-kul", url)]
    assert logos.get("/testbeds/kul").json()["logo_url"] == url


@pytest.mark.parametrize("data,ctype", [(PNG, "image/png"), (JPEG, "image/jpeg"), (GIF, "image/gif"), (WEBP, "image/webp")])
def test_every_allowed_format_is_stored_and_served_as_it_is(logos, data, ctype):
    assert _upload(logos, data).status_code == 200
    served = TestClient(app).get("/testbeds/kul/logo")  # no token: the catalogue's visitors load it
    assert served.status_code == 200 and served.content == data and served.headers["content-type"] == ctype


def test_the_public_response_cannot_be_sniffed_or_run_and_is_cacheable(logos):
    _upload(logos)
    h = TestClient(app).get("/testbeds/kul/logo").headers
    assert h["x-content-type-options"] == "nosniff"
    assert "default-src 'none'" in h["content-security-policy"] and "sandbox" in h["content-security-policy"]
    assert h["cache-control"].startswith("public") and h["etag"]


def test_a_repeated_request_with_the_etag_gets_304(logos):
    _upload(logos)
    public = TestClient(app)
    etag = public.get("/testbeds/kul/logo").headers["etag"]
    r = public.get("/testbeds/kul/logo", headers={"If-None-Match": etag})
    assert r.status_code == 304 and r.content == b""
    assert public.get("/testbeds/kul/logo", headers={"If-None-Match": '"other"'}).status_code == 200


def test_a_new_upload_changes_the_address_so_caches_do_not_hide_it(logos):
    first = _upload(logos, PNG).json()["logo_url"]
    second = _upload(logos, PNG2).json()["logo_url"]
    assert first != second
    assert TestClient(app).get("/testbeds/kul/logo").content == PNG2
    assert logos.sent[-1] == ("6g-dali-kul", second)


def test_the_file_must_really_be_an_image_whatever_it_is_called(logos):
    assert _upload(logos, b"<svg xmlns='http://www.w3.org/2000/svg'/>", "logo.svg", ctype="image/svg+xml").status_code == 415
    assert _upload(logos, b"MZ\x90\x00", "logo.png").status_code == 415   # an exe named .png
    assert _upload(logos, b"", "logo.png").status_code == 415
    assert logos.sent == []
    assert TestClient(app).get("/testbeds/kul/logo").status_code == 404


def test_a_logo_over_the_limit_is_refused(logos):
    assert _upload(logos, PNG + b"\x00" * logo_image.MAX_LOGO_BYTES).status_code == 413
    assert _upload(logos, PNG + b"\x00" * (logo_image.MAX_LOGO_BYTES - len(PNG))).status_code == 200  # exactly the limit


def test_without_a_public_url_an_upload_says_what_to_set(logos, monkeypatch):
    monkeypatch.setattr(testbeds, "ORCHESTRATOR_PUBLIC_URL", "")
    r = _upload(logos)
    # the registry answers a 5xx of its own as 424, with the original status in upstream_status
    assert r.status_code == 424 and r.json()["upstream_status"] == 503
    assert "ORCHESTRATOR_PUBLIC_URL" in r.json()["detail"]
    assert TestClient(app).get("/testbeds/kul/logo").status_code == 404  # nothing was stored


def test_a_piveau_failure_keeps_the_logo_and_says_so(logos, monkeypatch):
    from fastapi import HTTPException

    def down(catalogue, url):
        raise HTTPException(status_code=502, detail="piveau down")
    monkeypatch.setattr(piveau_catalogue_client, "set_catalogue_logo", down)
    r = _upload(logos)
    assert r.status_code == 424 and r.json()["upstream_status"] == 502 and "The logo is saved" in r.json()["detail"]
    assert logos.get("/testbeds/kul").json()["logo_url"].startswith(f"{BASE}/testbeds/kul/logo?v=")
    assert TestClient(app).get("/testbeds/kul/logo").status_code == 200


def test_removing_the_logo_clears_it_everywhere(logos):
    _upload(logos)
    r = logos.delete("/testbeds/kul/logo")
    assert r.status_code == 200 and r.json()["logo_url"] is None
    assert logos.sent[-1] == ("6g-dali-kul", None)
    assert TestClient(app).get("/testbeds/kul/logo").status_code == 404
    assert logos.delete("/testbeds/kul/logo").status_code == 404  # nothing left to remove


def test_a_logo_pasted_as_a_url_replaces_an_uploaded_one(logos):
    _upload(logos)
    r = logos.patch("/testbeds/kul", json={"logo_url": "https://example.org/logo.png"})
    assert r.status_code == 200 and r.json()["logo_url"] == "https://example.org/logo.png"
    assert TestClient(app).get("/testbeds/kul/logo").status_code == 404
    assert logos.delete("/testbeds/kul/logo").status_code == 404


def test_saving_the_same_hosted_address_again_keeps_the_uploaded_file(logos):
    url = _upload(logos).json()["logo_url"]
    logos.patch("/testbeds/kul", json={"logo_url": url})
    assert TestClient(app).get("/testbeds/kul/logo").status_code == 200


def test_the_owner_can_set_and_remove_their_logo_but_not_another_testbeds(logos):
    logos.post("/testbeds", json={"slug": "isi", "name": "ISI"})
    _act_as_owner(logos, "/testbeds/kul")
    assert _upload(logos).status_code == 200
    assert logos.delete("/testbeds/kul/logo").status_code == 200
    assert _upload(logos, slug="isi").status_code == 403
    assert logos.delete("/testbeds/isi/logo").status_code == 403


def test_someone_without_access_cannot_upload(logos):
    _act_as_owner(logos, "/other/group")
    assert _upload(logos).status_code == 403


def test_a_testbed_without_a_logo_has_no_public_image(logos):
    assert TestClient(app).get("/testbeds/kul/logo").status_code == 404
    assert TestClient(app).get("/testbeds/nobody/logo").status_code == 404


def test_deregistering_a_testbed_removes_its_logo(logos):
    _upload(logos)
    assert logos.delete("/testbeds/kul").status_code == 200
    assert TestClient(app).get("/testbeds/kul/logo").status_code == 404
