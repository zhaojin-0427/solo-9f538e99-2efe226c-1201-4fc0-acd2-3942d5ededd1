"""Schema migration: databases from earlier releases upgrade in place."""

from __future__ import annotations

import sqlite3
from pathlib import Path

from fastapi.testclient import TestClient

from app import store
from app.config import Settings
from app.main import create_app

# The versions table exactly as shipped in the initial release: no
# supersedes_version column and no subscription tables.
LEGACY_SCHEMA = """
CREATE TABLE versions (
    version        INTEGER PRIMARY KEY,
    day            TEXT    NOT NULL,
    status         TEXT    NOT NULL CHECK (status IN ('ready')),
    published_at   TEXT    NOT NULL,
    width          INTEGER NOT NULL,
    height         INTEGER NOT NULL,
    crs_wkt        TEXT    NOT NULL,
    crs_label      TEXT    NOT NULL,
    transform      TEXT    NOT NULL,
    bounds         TEXT    NOT NULL,
    res_x          REAL    NOT NULL,
    res_y          REAL    NOT NULL,
    dtype          TEXT    NOT NULL,
    nodata         INTEGER NOT NULL,
    sha256         TEXT    NOT NULL,
    size_bytes     INTEGER NOT NULL,
    block_meta     TEXT    NOT NULL
);
CREATE INDEX versions_day_idx ON versions(day);
CREATE TABLE state (key TEXT PRIMARY KEY, value TEXT NOT NULL);
INSERT INTO state (key, value) VALUES ('current_version', '0');
"""


def test_legacy_database_migrates_and_keeps_working(tmp_path: Path):
    settings = Settings(data_dir=tmp_path / "data")
    settings.ensure_directories()
    legacy = store.connect(settings)
    try:
        legacy.executescript(LEGACY_SCHEMA)
        legacy.execute(
            "INSERT INTO versions (version, day, status, published_at, width, height, "
            "crs_wkt, crs_label, transform, bounds, res_x, res_y, dtype, nodata, "
            "sha256, size_bytes, block_meta) VALUES "
            "(1, '2026-01-01', 'ready', '2026-01-01T00:00:00Z', 1, 1, 'wkt', 'EPSG:4326', "
            "'[]', '[]', 0.01, 0.01, 'int16', -32768, 'abc', 3, '[]')"
        )
        legacy.execute("UPDATE state SET value = '1' WHERE key = 'current_version'")
        columns_before = {row["name"] for row in legacy.execute("PRAGMA table_info(versions)")}
        assert "supersedes_version" not in columns_before
        legacy_tables = {
            row["name"] for row in legacy.execute("SELECT name FROM sqlite_master WHERE type='table'")
        }
        assert "subscribers" not in legacy_tables
    finally:
        legacy.close()

    # First application start must migrate without dropping any data.
    with TestClient(create_app(settings)) as client:
        health = client.get("/healthz")
        assert health.status_code == 200
        assert health.json()["current_version"] == 1

        version = client.get("/api/v1/mosaics/1")
        assert version.status_code == 200
        assert version.json()["version"]["supersedes_version"] is None

        # The subscription API is available on the migrated database.
        registered = client.post("/api/v1/subscriptions", json={"name": "migrated"})
        assert registered.status_code == 201
        sid, secret = registered.json()["subscriber_id"], registered.json()["secret"]
        headers = {"X-Subscriber-Id": sid, "X-Subscriber-Secret": secret}
        status = client.get("/api/v1/subscriptions/me", headers=headers)
        assert status.json()["last_ack_version"] == 0

    # Migration is idempotent: a second start on the same files works too.
    with TestClient(create_app(settings)) as client:
        assert client.get("/healthz").json()["current_version"] == 1
        probe = sqlite3.connect(settings.db_path)
        probe.row_factory = sqlite3.Row
        columns = {row["name"] for row in probe.execute("PRAGMA table_info(versions)")}
        probe.close()
        assert "supersedes_version" in columns
