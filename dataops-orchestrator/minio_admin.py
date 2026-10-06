"""
Minimal MinIO admin API client: just what the testbed registry needs to give a
testbed its own bucket-scoped credentials (policy + user + attach, and removal).

Replaces shelling out to the `mc` binary, which MinIO no longer distributes in a
form we can build an image from. Written against madmin-go v3
(github.com/minio/madmin-go/v3):

  PUT    /minio/admin/v3/add-canned-policy?name=<policy>              body: policy JSON
  PUT    /minio/admin/v3/add-user?accessKey=<ak>                      body: encrypted JSON
  PUT    /minio/admin/v3/set-user-or-group-policy?policyName=..&userOrGroup=..&isGroup=false
  DELETE /minio/admin/v3/remove-user?accessKey=<ak>

Requests are AWS SigV4-signed (service "s3") with the admin key. The add-user
body is encrypted with the *admin secret key* (madmin.EncryptData), which is the
only non-obvious part and is implemented in encrypt_data below.
"""

import hashlib
import json
import os
import struct
from urllib.parse import urlencode

import httpx
from argon2.low_level import Type, hash_secret_raw
from botocore.auth import SigV4Auth
from botocore.awsrequest import AWSRequest
from botocore.credentials import Credentials
from cryptography.hazmat.primitives.ciphers.aead import AESGCM

_ADMIN_PREFIX = "/minio/admin/v3"

# madmin-go encrypt.go: argon2id time=1, memory=64 MiB, threads=4; AEAD id 0x00 = argon2id + AES-256-GCM.
_ARGON2_TIME, _ARGON2_MEMORY_KIB, _ARGON2_THREADS = 1, 64 * 1024, 4
_AEAD_ARGON2ID_AESGCM = 0x00
# sio-go DARE stream: 16 KiB fragments. Everything we encrypt is far smaller (one fragment).
_FRAGMENT = 1 << 14


class MinioAdminError(Exception):
    pass


def _derive_key(password: str, salt: bytes) -> bytes:
    return hash_secret_raw(
        password.encode(), salt, time_cost=_ARGON2_TIME, memory_cost=_ARGON2_MEMORY_KIB,
        parallelism=_ARGON2_THREADS, hash_len=32, type=Type.ID,
    )


def encrypt_data(password: str, data: bytes) -> bytes:
    """madmin.EncryptData: salt(32) | aead id(1) | nonce(8) | sio-go (DARE 2.0) stream.

    The stream's first step derives a 16-byte tag from an empty message at sequence 0
    (EncryptWriter does this to bind the associated data); that tag is appended to the
    one-byte fragment flag to form every fragment's associated data. The (single, final)
    fragment is sealed at sequence 1 with flag 0x80.
    """
    if len(data) > _FRAGMENT:
        raise ValueError("payload larger than one DARE fragment is not supported")
    salt, nonce = os.urandom(32), os.urandom(8)
    aead = AESGCM(_derive_key(password, salt))
    tag = aead.encrypt(nonce + struct.pack("<I", 0), b"", b"")
    ciphertext = aead.encrypt(nonce + struct.pack("<I", 1), data, b"\x80" + tag)
    return salt + bytes([_AEAD_ARGON2ID_AESGCM]) + nonce + ciphertext


class MinioAdmin:
    def __init__(self, endpoint: str, access_key: str, secret_key: str, region: str = "us-east-1", timeout: float = 30):
        self.endpoint = endpoint.rstrip("/")
        self.access_key, self.secret_key, self.region, self.timeout = access_key, secret_key, region, timeout

    def _call(self, method: str, op: str, query: dict, body: bytes = b"") -> None:
        url = f"{self.endpoint}{_ADMIN_PREFIX}/{op}?{urlencode(query)}"
        request = AWSRequest(method=method, url=url, data=body,
                             headers={"X-Amz-Content-Sha256": hashlib.sha256(body).hexdigest()})
        SigV4Auth(Credentials(self.access_key, self.secret_key), "s3", self.region).add_auth(request)
        try:
            response = httpx.request(method, url, content=body, headers=dict(request.headers.items()), timeout=self.timeout)
        except httpx.RequestError as e:
            raise MinioAdminError(f"could not reach MinIO at {self.endpoint}: {e}")
        if response.status_code != 200:
            raise MinioAdminError(f"{op} -> {response.status_code} {response.text[:300]}")

    def add_canned_policy(self, name: str, policy: dict) -> None:
        self._call("PUT", "add-canned-policy", {"name": name}, json.dumps(policy).encode())

    def add_user(self, access_key: str, secret_key: str) -> None:
        payload = json.dumps({"secretKey": secret_key, "status": "enabled"}).encode()
        self._call("PUT", "add-user", {"accessKey": access_key}, encrypt_data(self.secret_key, payload))

    def attach_policy(self, policy_name: str, access_key: str) -> None:
        self._call("PUT", "set-user-or-group-policy",
                   {"policyName": policy_name, "userOrGroup": access_key, "isGroup": "false"})

    def remove_user(self, access_key: str) -> None:
        self._call("DELETE", "remove-user", {"accessKey": access_key})
