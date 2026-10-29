"""Presigned download links: only for a Data Lake file of a dataset the caller can see in piveau."""
from urllib.parse import parse_qs, urlsplit

from routers import downloads
from test_testbeds import client  # noqa: F401  (client is a fixture)

CAT = "6g-dali-hesso"
DS = "d37424ff-20bf-45f5-a574-753bcafe35a9"
FILE = f"http://sparks-prod07.sparkworks.net:9000/{CAT}/{DS}/9a7e0000-1111-4222-8333-444455556666.pcap"
TURTLE = f"""
@prefix dcat: <http://www.w3.org/ns/dcat#> .
<https://dataspace.6gdali.eu/set/data/{DS}> a dcat:Dataset ; dcat:distribution <https://d/1> .
<https://d/1> a dcat:Distribution ; dcat:downloadURL <{FILE}> .
"""


class _Piveau:
    """Stands in for piveau: answers as it would for the token it is given."""

    def __init__(self, status=200, needs_token=False):
        self.status, self.needs_token, self.seen = status, needs_token, []

    async def __call__(self, dataset_id, catalogue_id, authorization):
        self.seen.append(authorization)
        if self.needs_token and not authorization:
            raise downloads.HTTPException(status_code=404, detail="Dataset not found, or not visible to you")
        return {FILE}


def _ask(client, url=FILE, token=None):
    headers = {"Authorization": f"Bearer {token}"} if token else {}
    return client.get("/downloads/presign", params={"catalogue": CAT, "dataset": DS, "url": url}, headers=headers)


def _configure(monkeypatch, piveau):
    monkeypatch.setattr(downloads, "_visible_download_urls", piveau)
    monkeypatch.setattr(downloads, "DATALAKE_DOWNLOAD_ENDPOINT_URL", "http://sparks-prod07.sparkworks.net:9000")
    monkeypatch.setattr(downloads, "DATASPACE_S3_ACCESS_KEY", "key")
    monkeypatch.setattr(downloads, "DATASPACE_S3_SECRET_KEY", "secret")


def test_a_visible_file_gets_a_short_lived_signed_link(client, monkeypatch):
    piveau = _Piveau()
    _configure(monkeypatch, piveau)

    r = _ask(client, token="t-ann")

    assert r.status_code == 200, r.text
    body = r.json()
    signed = urlsplit(body["url"])
    assert f"{signed.scheme}://{signed.netloc}{signed.path}" == FILE
    query = parse_qs(signed.query)
    assert query["X-Amz-Expires"] == ["900"] and "X-Amz-Signature" in query
    assert query["response-content-disposition"] == ['attachment; filename="9a7e0000-1111-4222-8333-444455556666.pcap"']
    assert piveau.seen == ["Bearer t-ann"]  # read as the caller, not with the orchestrator's key


def test_a_dataset_the_caller_cannot_see_gives_no_link(client, monkeypatch):
    _configure(monkeypatch, _Piveau(needs_token=True))
    assert _ask(client).status_code == 404


def test_only_a_download_url_of_that_dataset_is_signed(client, monkeypatch):
    _configure(monkeypatch, _Piveau())
    other = f"http://sparks-prod07.sparkworks.net:9000/{CAT}/{DS}/someone-elses.csv"
    assert _ask(client, url=other).status_code == 404


def test_nothing_outside_the_catalogues_bucket_is_signed(client, monkeypatch):
    _configure(monkeypatch, _Piveau())
    for url in (
        f"http://sparks-prod07.sparkworks.net:9000/6g-dali-other/{DS}/x.csv",   # another bucket
        f"http://sparks-prod07.sparkworks.net:9000/{CAT}/.files/{DS}/x.csv",    # the connector's mappings
        f"http://sparks-prod07.sparkworks.net:9000/{CAT}/{DS}/../x.csv",
        "https://zenodo.org/records/1/files/x.csv",                             # not the Data Lake
    ):
        assert _ask(client, url=url).status_code == 422, url


def test_the_key_comes_from_the_url_path():
    assert downloads.object_of(FILE, CAT) == f"{DS}/9a7e0000-1111-4222-8333-444455556666.pcap"
    assert downloads.object_of(f"http://h:9000/{CAT}/a%20b/c.csv", CAT) == "a b/c.csv"
    assert downloads.object_of(f"http://h:9000/{CAT}/", CAT) is None
    assert downloads.object_of(f"http://h:9000/{CAT}/a.csv?x=1", CAT) is None
