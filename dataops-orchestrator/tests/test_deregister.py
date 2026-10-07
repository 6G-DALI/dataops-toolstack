"""Deregistering a testbed, optionally deleting its bucket and catalogue too."""
import httpx
import pytest
from fastapi import HTTPException

import datalake_admin
import minio_admin
import piveau_catalogue_client
import testbed_store
from test_testbeds import client  # noqa: F401  (client is a fixture)


@pytest.fixture
def world(client, monkeypatch):
    """A provisioned testbed 'kul', with every external system faked and every call recorded."""
    calls = client.calls  # the shared fixture already records bucket / key / catalogue calls here
    monkeypatch.setattr(datalake_admin, "remove_policy", lambda slug: calls.append(("policy", slug)))
    monkeypatch.setattr(piveau_catalogue_client, "count_datasets", lambda c: 7)
    monkeypatch.setattr(piveau_catalogue_client, "delete_catalogue",
                        lambda c: calls.append(("catalogue", c)) or {"status": "deleted"})
    monkeypatch.setattr(datalake_admin, "count_objects", lambda b: (120, False))
    monkeypatch.setattr(datalake_admin, "delete_bucket",
                        lambda b: calls.append(("bucket", b)) or {"status": "deleted", "objects_deleted": 120})
    client.post("/testbeds", json={"slug": "kul", "name": "KU Leuven"})
    client.post("/testbeds/kul/provision")
    calls.clear()
    return client


def test_default_deregister_keeps_bucket_and_catalogue(world):
    r = world.delete("/testbeds/kul").json()
    assert r["status"] == "deregistered" and r["results"]["key"] == {"status": "removed"} and r["followups"] == []
    assert ("rm", "tb-kul-1") in world.calls and ("policy", "kul") in world.calls  # key and policy are cleaned up
    assert [c for c in world.calls if c[0] in ("catalogue", "bucket")] == []
    assert world.get("/testbeds/kul").status_code == 404


def test_deleting_bucket_or_catalogue_needs_the_slug_as_confirmation(world):
    for params in ("delete_bucket=true", "delete_catalogue=true", "delete_bucket=true&confirm=other"):
        r = world.delete(f"/testbeds/kul?{params}")
        assert r.status_code == 409 and "confirm=kul" in r.json()["detail"]
    assert world.get("/testbeds/kul").status_code == 200  # nothing was touched
    assert world.calls == []


def test_delete_both_with_confirmation(world):
    r = world.delete("/testbeds/kul?delete_bucket=true&delete_catalogue=true&confirm=kul").json()
    assert r["results"]["catalogue"] == {"status": "deleted"}
    assert r["results"]["bucket"] == {"status": "deleted", "objects_deleted": 120}
    assert ("catalogue", "6g-dali-kul") in world.calls and ("bucket", "6g-dali-kul") in world.calls
    assert any("MONITOR_BUCKETS" in f for f in r["followups"]) and any("EDC assets" in f for f in r["followups"])
    assert world.get("/testbeds/kul").status_code == 404


def test_only_the_catalogue(world):
    r = world.delete("/testbeds/kul?delete_catalogue=true&confirm=kul").json()
    assert "bucket" not in r["results"] and ("bucket", "6g-dali-kul") not in world.calls
    assert not any("MONITOR_BUCKETS" in f for f in r["followups"])


def test_a_failed_step_keeps_the_testbed_registered_and_can_be_retried(world, monkeypatch):
    def boom(c):
        raise HTTPException(status_code=502, detail="piveau refused")
    monkeypatch.setattr(piveau_catalogue_client, "delete_catalogue", boom)
    r = world.delete("/testbeds/kul?delete_bucket=true&delete_catalogue=true&confirm=kul")
    assert r.status_code == 424 and "piveau refused" in r.json()["detail"] and "retry" in r.json()["detail"]
    assert world.get("/testbeds/kul").status_code == 200  # still registered
    assert ("bucket", "6g-dali-kul") in world.calls       # the other step still ran
    assert testbed_store.get_s3_credentials("kul") is None  # and the key is already gone, so a retry will not redo it

    monkeypatch.setattr(piveau_catalogue_client, "delete_catalogue", lambda c: {"status": "not_found"})
    r = world.delete("/testbeds/kul?delete_bucket=true&delete_catalogue=true&confirm=kul").json()
    assert r["status"] == "deregistered" and r["results"]["key"] == {"status": "none"}
    assert world.get("/testbeds/kul").status_code == 404


def test_deletion_preview(world):
    with testbed_store._db() as c:
        c.execute("INSERT INTO testbed_assets (slug, asset_id, status, present, transfer_state) "
                  "VALUES ('kul','a1','transferring',1,'STARTED')")
    p = world.get("/testbeds/kul/deletion-preview").json()
    assert (p["datasets"], p["objects"], p["bucket"], p["catalogue_id"]) == (7, 120, "6g-dali-kul", "6g-dali-kul")
    assert p["running_transfers"] == ["a1"]


def test_preview_reports_each_system_separately(world, monkeypatch):
    def down(c):
        raise HTTPException(status_code=502, detail="search is down")
    monkeypatch.setattr(piveau_catalogue_client, "count_datasets", down)
    p = world.get("/testbeds/kul/deletion-preview").json()
    assert p["datasets"] is None and "search is down" in p["datasets_error"] and p["objects"] == 120


class FakeS3:
    """Just enough of boto3's S3 client for delete_bucket / count_objects."""
    class exceptions:
        class NoSuchBucket(Exception):
            pass

    def __init__(self, keys, versioned_marker=False):
        self.keys, self.deleted, self.bucket_deleted, self.versioned_marker = list(keys), [], False, versioned_marker

    def head_bucket(self, Bucket):
        return {}

    def get_paginator(self, name):
        outer = self

        class P:
            def paginate(self, Bucket):
                if name == "list_objects_v2":
                    return [{"Contents": [{"Key": k} for k in outer.keys[:2]]}, {"Contents": [{"Key": k} for k in outer.keys[2:]]}]
                versions = [{"Key": k, "VersionId": "null"} for k in outer.keys]
                markers = [{"Key": "gone", "VersionId": "m1"}] if outer.versioned_marker else []
                return [{"Versions": versions, "DeleteMarkers": markers}]
        return P()

    def delete_objects(self, Bucket, Delete):
        self.deleted += [o["Key"] for o in Delete["Objects"]]

    def delete_bucket(self, Bucket):
        self.bucket_deleted = True


def test_delete_bucket_empties_it_then_removes_it(monkeypatch):
    s3 = FakeS3(["a/metadata.json", "a/1.csv", "b/2.csv"], versioned_marker=True)
    monkeypatch.setattr(datalake_admin.datalake_client, "_client", lambda: s3)
    assert datalake_admin.count_objects("b") == (3, False)
    assert datalake_admin.count_objects("b", limit=2) == (2, True)
    assert datalake_admin.delete_bucket("b") == {"status": "deleted", "objects_deleted": 4}
    assert sorted(s3.deleted) == ["a/1.csv", "a/metadata.json", "b/2.csv", "gone"] and s3.bucket_deleted


def test_delete_missing_bucket_is_not_an_error(monkeypatch):
    class Missing(FakeS3):
        def head_bucket(self, Bucket):
            raise RuntimeError("404")
    monkeypatch.setattr(datalake_admin.datalake_client, "_client", lambda: Missing([]))
    assert datalake_admin.delete_bucket("gone") == {"status": "not_found", "objects_deleted": 0}


def test_delete_catalogue_requests(monkeypatch):
    import piveau_dataset_client as pdc
    monkeypatch.setattr(pdc, "PIVEAU_HUB_URL", "https://piveau.example/")
    monkeypatch.setattr(pdc, "PIVEAU_API_KEY", "pk")
    seen = []
    answers = iter([200, 404, 500])
    monkeypatch.setattr(piveau_catalogue_client.httpx, "delete",
                        lambda url, headers=None, timeout=None: seen.append((url, headers)) or httpx.Response(next(answers), text="x"))
    assert piveau_catalogue_client.delete_catalogue("c1") == {"status": "deleted"}
    assert seen[0] == ("https://piveau.example/catalogues/c1", {"X-API-Key": "pk"})
    assert piveau_catalogue_client.delete_catalogue("c1") == {"status": "not_found"}
    with pytest.raises(HTTPException):
        piveau_catalogue_client.delete_catalogue("c1")


def test_minio_remove_policy_request(monkeypatch):
    seen = []
    monkeypatch.setattr(minio_admin.httpx, "request", lambda m, u, **k: seen.append((m, u)) or httpx.Response(200))
    minio_admin.MinioAdmin("http://minio:9000", "AK", "SK").remove_canned_policy("dali-testbed-kul")
    assert seen == [("DELETE", "http://minio:9000/minio/admin/v3/remove-canned-policy?name=dali-testbed-kul")]
