import json
import os
import struct
import sys
from urllib.parse import parse_qs, urlparse

import httpx
import pytest
from argon2.low_level import Type, hash_secret_raw
from cryptography.hazmat.primitives.ciphers.aead import AESGCM

sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

import minio_admin  # noqa: E402
from minio_admin import MinioAdmin, MinioAdminError, encrypt_data  # noqa: E402


def decrypt_data(password: str, blob: bytes) -> bytes:
    """Follows madmin.DecryptData + sio-go's DecryptReader for a single-fragment stream."""
    salt, aead_id, nonce, rest = blob[:32], blob[32], blob[33:41], blob[41:]
    assert aead_id == 0x00  # argon2id + AES-256-GCM
    key = hash_secret_raw(password.encode(), salt, time_cost=1, memory_cost=64 * 1024, parallelism=4,
                          hash_len=32, type=Type.ID)
    aead = AESGCM(key)
    tag = aead.encrypt(nonce + struct.pack("<I", 0), b"", b"")
    return aead.decrypt(nonce + struct.pack("<I", 1), rest, b"\x80" + tag)


def test_encrypt_round_trip_and_layout():
    blob = encrypt_data("admin-secret", b'{"secretKey":"x","status":"enabled"}')
    assert decrypt_data("admin-secret", blob) == b'{"secretKey":"x","status":"enabled"}'
    assert len(blob) == 32 + 1 + 8 + len(b'{"secretKey":"x","status":"enabled"}') + 16
    with pytest.raises(Exception):
        decrypt_data("wrong", blob)
    assert encrypt_data("admin-secret", b"a") != encrypt_data("admin-secret", b"a")  # fresh salt and nonce


@pytest.fixture
def calls(monkeypatch):
    seen = []

    def fake(method, url, content=b"", headers=None, timeout=None):
        seen.append((method, url, content, headers))
        return httpx.Response(200)
    monkeypatch.setattr(minio_admin.httpx, "request", fake)
    return seen


def test_requests_match_madmin(calls):
    admin = MinioAdmin("http://minio:9000/", "AK", "SK")
    admin.add_canned_policy("p", {"Version": "2012-10-17"})
    admin.add_user("tb-kul", "usersecret")
    admin.attach_policy("p", "tb-kul")
    admin.remove_user("tb-kul")

    (m1, u1, b1, h1), (m2, u2, b2, _), (m3, u3, _, _), (m4, u4, _, _) = calls
    assert (m1, urlparse(u1).path, parse_qs(urlparse(u1).query)) == ("PUT", "/minio/admin/v3/add-canned-policy", {"name": ["p"]})
    assert json.loads(b1) == {"Version": "2012-10-17"}
    assert (m2, urlparse(u2).path, parse_qs(urlparse(u2).query)) == ("PUT", "/minio/admin/v3/add-user", {"accessKey": ["tb-kul"]})
    assert json.loads(decrypt_data("SK", b2)) == {"secretKey": "usersecret", "status": "enabled"}
    assert parse_qs(urlparse(u3).query) == {"policyName": ["p"], "userOrGroup": ["tb-kul"], "isGroup": ["false"]}
    assert (m4, urlparse(u4).path) == ("DELETE", "/minio/admin/v3/remove-user")
    auth = {k.lower(): v for k, v in h1.items()}
    assert auth["authorization"].startswith("AWS4-HMAC-SHA256 Credential=AK/") and "/us-east-1/s3/aws4_request" in auth["authorization"]
    assert "x-amz-content-sha256" in auth["authorization"]  # signed header


def test_non_200_raises(monkeypatch):
    monkeypatch.setattr(minio_admin.httpx, "request", lambda *a, **k: httpx.Response(403, text="AccessDenied"))
    with pytest.raises(MinioAdminError, match="403"):
        MinioAdmin("http://minio:9000", "AK", "SK").remove_user("x")
