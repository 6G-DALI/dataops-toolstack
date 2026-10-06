"""
Registry of testbeds (Postgres via DATABASE_URL; a SQLite file when unset, for
local development and tests), plus the secret handling around it.

One row per testbed. The per-testbed data-lake secret key is stored encrypted
(Fernet, key derived from TESTBED_SECRET_KEY) because the orchestrator needs it
again later, when it starts that testbed's transfer on the central connector.
Nothing else secret is stored: the passwords in a generated connector bundle are
derived on demand from the master secret (see derive_secret) so a regenerated
bundle matches the one the operator already deployed.
"""

import base64
import hashlib
import hmac
import json
import os
import sqlite3
import threading
from contextlib import contextmanager
from datetime import datetime, timezone

from cryptography.fernet import Fernet
from fastapi import HTTPException

from config import DATABASE_URL, TESTBED_DB_PATH, TESTBED_SECRET_KEY

_lock = threading.Lock()

_COLUMNS = (
    "slug", "name", "organisation", "contact_email", "participant_id", "ids_id",
    "experiment_prefix", "bucket", "catalogue_id", "dsp_url", "produced_by_iri",
    "status", "steps", "s3_access_key", "s3_secret_enc", "created_at", "updated_at", "created_by",
)


def _now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def _require_secret() -> bytes:
    if not TESTBED_SECRET_KEY:
        raise HTTPException(status_code=503, detail="TESTBED_SECRET_KEY not configured; testbed endpoints are disabled")
    return TESTBED_SECRET_KEY.encode()


def _fernet() -> Fernet:
    return Fernet(base64.urlsafe_b64encode(hashlib.sha256(b"fernet:" + _require_secret()).digest()))


def encrypt(plain: str) -> str:
    return _fernet().encrypt(plain.encode()).decode()


def decrypt(token: str) -> str:
    return _fernet().decrypt(token.encode()).decode()


def derive_secret(*parts: str, length: int = 24) -> str:
    """Stable per-(testbed, purpose) password, e.g. derive_secret(slug, 'rustfs')."""
    mac = hmac.new(_require_secret(), ":".join(parts).encode(), hashlib.sha256).digest()
    return base64.b32encode(mac).decode().rstrip("=").lower()[:length]


_PG = bool(DATABASE_URL)
_schema_ready = False

_DDL = (
    """CREATE TABLE IF NOT EXISTS testbeds (
        slug TEXT PRIMARY KEY, name TEXT NOT NULL, organisation TEXT, contact_email TEXT,
        participant_id TEXT UNIQUE NOT NULL, ids_id TEXT, experiment_prefix TEXT UNIQUE NOT NULL,
        bucket TEXT UNIQUE NOT NULL, catalogue_id TEXT UNIQUE NOT NULL, dsp_url TEXT NOT NULL,
        produced_by_iri TEXT, status TEXT NOT NULL, steps TEXT NOT NULL DEFAULT '{}',
        s3_access_key TEXT, s3_secret_enc TEXT, created_at TEXT, updated_at TEXT, created_by TEXT)""",
    """CREATE TABLE IF NOT EXISTS testbed_assets (
        slug TEXT NOT NULL, asset_id TEXT NOT NULL, title TEXT, offer_id TEXT,
        status TEXT NOT NULL DEFAULT 'discovered', present INTEGER NOT NULL DEFAULT 1,
        contract_agreement_id TEXT, transfer_id TEXT,
        discovered_at TEXT, last_seen_at TEXT, PRIMARY KEY (slug, asset_id))""",
    """CREATE TABLE IF NOT EXISTS testbed_audit (
        id {id_type}, ts TEXT NOT NULL, slug TEXT NOT NULL,
        actor TEXT, action TEXT NOT NULL, detail TEXT)""",
)


def _integrity_errors() -> tuple:
    if _PG:
        import psycopg
        return (psycopg.errors.IntegrityError,)
    return (sqlite3.IntegrityError,)


class _Conn:
    """Runs `?`-placeholder SQL on either backend."""

    def __init__(self, raw):
        self.raw = raw

    def execute(self, sql: str, params=()):
        return self.raw.execute(sql.replace("?", "%s") if _PG else sql, params)


@contextmanager
def _db():
    global _schema_ready
    if _PG:
        import psycopg
        from psycopg.rows import dict_row
        raw = psycopg.connect(DATABASE_URL, row_factory=dict_row)
    else:
        _lock.acquire()
        os.makedirs(os.path.dirname(TESTBED_DB_PATH) or ".", exist_ok=True)
        raw = sqlite3.connect(TESTBED_DB_PATH)
        raw.row_factory = sqlite3.Row
    conn = _Conn(raw)
    try:
        if not _schema_ready or not _PG:
            id_type = "BIGSERIAL PRIMARY KEY" if _PG else "INTEGER PRIMARY KEY AUTOINCREMENT"
            for ddl in _DDL:
                conn.execute(ddl.format(id_type=id_type) if "{id_type}" in ddl else ddl)
            _schema_ready = _PG
        yield conn
        raw.commit()
    finally:
        raw.close()
        if not _PG:
            _lock.release()


def _public(row: sqlite3.Row) -> dict:
    d = {k: row[k] for k in _COLUMNS if k != "s3_secret_enc"}
    d["steps"] = json.loads(d["steps"] or "{}")
    d["has_credentials"] = bool(row["s3_secret_enc"])
    return d


def audit(slug: str, actor: str | None, action: str, detail: str = "") -> None:
    with _db() as c:
        c.execute("INSERT INTO testbed_audit (ts, slug, actor, action, detail) VALUES (?,?,?,?,?)",
                  (_now(), slug, actor, action, detail))


def list_testbeds() -> list[dict]:
    with _db() as c:
        return [_public(r) for r in c.execute("SELECT * FROM testbeds ORDER BY slug")]


def get_testbed(slug: str) -> dict | None:
    with _db() as c:
        r = c.execute("SELECT * FROM testbeds WHERE slug=?", (slug,)).fetchone()
        return _public(r) if r else None


def create_testbed(values: dict, actor: str | None) -> dict:
    now = _now()
    row = {**values, "status": "draft", "steps": "{}", "s3_access_key": None, "s3_secret_enc": None,
           "created_at": now, "updated_at": now, "created_by": actor}
    try:
        with _db() as c:
            c.execute(f"INSERT INTO testbeds ({','.join(_COLUMNS)}) VALUES ({','.join('?' * len(_COLUMNS))})",
                      [row.get(k) for k in _COLUMNS])
    except _integrity_errors() as e:
        raise HTTPException(status_code=409, detail=f"Testbed identity already in use: {e}")
    audit(values["slug"], actor, "register", values["participant_id"])
    return get_testbed(values["slug"])


def update_testbed(slug: str, **fields) -> dict:
    fields["updated_at"] = _now()
    if "steps" in fields:
        fields["steps"] = json.dumps(fields["steps"])
    sets = ",".join(f"{k}=?" for k in fields)
    with _db() as c:
        c.execute(f"UPDATE testbeds SET {sets} WHERE slug=?", [*fields.values(), slug])
    return get_testbed(slug)


def delete_testbed(slug: str) -> None:
    with _db() as c:
        c.execute("DELETE FROM testbed_assets WHERE slug=?", (slug,))
        c.execute("DELETE FROM testbeds WHERE slug=?", (slug,))


def list_assets(slug: str) -> list[dict]:
    with _db() as c:
        rows = c.execute("SELECT * FROM testbed_assets WHERE slug=? ORDER BY asset_id", (slug,)).fetchall()
    return [{**{k: r[k] for k in r.keys()}, "present": bool(r["present"])} for r in rows]


def sync_assets(slug: str, offered: list[dict]) -> list[dict]:
    """Record what the testbed's catalogue offers. New assets are stored as 'discovered'; known ones
    keep their negotiation/transfer state and just get their title and offer refreshed. Assets that
    are no longer offered are kept (they may have agreements) but flagged present=false."""
    now = _now()
    with _db() as c:
        for a in offered:
            c.execute(
                """INSERT INTO testbed_assets (slug, asset_id, title, offer_id, status, present, discovered_at, last_seen_at)
                   VALUES (?,?,?,?, 'discovered', 1, ?, ?)
                   ON CONFLICT (slug, asset_id) DO UPDATE SET
                     title=excluded.title, offer_id=excluded.offer_id, present=1, last_seen_at=excluded.last_seen_at""",
                (slug, a["asset_id"], a.get("title"), a.get("offer_id"), now, now),
            )
        offered_ids = {a["asset_id"] for a in offered}
        for row in c.execute("SELECT asset_id FROM testbed_assets WHERE slug=?", (slug,)).fetchall():
            if row["asset_id"] not in offered_ids:
                c.execute("UPDATE testbed_assets SET present=0 WHERE slug=? AND asset_id=?", (slug, row["asset_id"]))
    return list_assets(slug)


def get_s3_credentials(slug: str) -> tuple[str, str] | None:
    """(access_key, secret_key) for the testbed's scoped data-lake user, or None."""
    with _db() as c:
        r = c.execute("SELECT s3_access_key, s3_secret_enc FROM testbeds WHERE slug=?", (slug,)).fetchone()
    if not r or not r["s3_secret_enc"]:
        return None
    return r["s3_access_key"], decrypt(r["s3_secret_enc"])


def set_s3_credentials(slug: str, access_key: str | None, secret: str | None) -> None:
    with _db() as c:
        c.execute("UPDATE testbeds SET s3_access_key=?, s3_secret_enc=?, updated_at=? WHERE slug=?",
                  (access_key, encrypt(secret) if secret else None, _now(), slug))


def audit_log(slug: str, limit: int = 100) -> list[dict]:
    with _db() as c:
        return [dict(r) for r in c.execute(
            "SELECT ts, actor, action, detail FROM testbed_audit WHERE slug=? ORDER BY id DESC LIMIT ?", (slug, limit))]
