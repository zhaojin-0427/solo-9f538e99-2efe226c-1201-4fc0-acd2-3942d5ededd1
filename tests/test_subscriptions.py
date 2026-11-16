"""Subscription API: ordered delivery, leases, confirmations, durability.

These tests exercise the hydrology-model ingest contract:

* one notification per call, strictly in publication order starting at 0;
* repeat claims return the same credential until the 60 s lease expires;
* only the active token of the next version can advance progress;
* re-claims after expiry invalidate the old token;
* a missing image blocks the subscriber at that version without skipping;
* restarts and concurrent publications never lose or reorder versions;
* subscribers are fully isolated from each other.
"""

from __future__ import annotations

import time
from pathlib import Path

import numpy as np
import pytest
from fastapi.testclient import TestClient

from app import store
from app.config import Settings
from app.main import create_app
from app.rasters import NODATA
from tests.conftest import make_geotiff, submit_payload

A_TILE = np.array([[1, 2], [3, 4]], dtype=np.int16)
B_TILE = np.array([[NODATA, 2], [NODATA, 4]], dtype=np.int16)


def _publish(client, *, expected_version: int, day: str = "2026-01-01", **kwargs):
    data, files = submit_payload(
        block_a=make_geotiff(A_TILE),
        block_b=make_geotiff(B_TILE, x0=0.01),
        expected_version=expected_version,
        day=day,
        **kwargs,
    )
    response = client.post("/api/v1/mosaics", data=data, files=files)
    assert response.status_code == 201, response.text
    return response.json()


def _register(client, name: str | None = "hydro-model"):
    response = client.post("/api/v1/subscriptions", json={"name": name})
    assert response.status_code == 201, response.text
    body = response.json()
    assert body["last_ack_version"] == 0
    return body["subscriber_id"], body["secret"]


def _headers(identity):
    sid, secret = identity
    return {"X-Subscriber-Id": sid, "X-Subscriber-Secret": secret}


def _claim(client, identity):
    return client.post("/api/v1/subscriptions/claims", headers=_headers(identity))


def _confirm(client, identity, *, version: int, token: str):
    return client.post(
        "/api/v1/subscriptions/confirmations",
        headers=_headers(identity),
        json={"version": version, "claim_token": token},
    )


@pytest.fixture()
def short_lease_settings(tmp_path: Path) -> Settings:
    return Settings(data_dir=tmp_path / "data", claim_lease_seconds=0.25)


@pytest.fixture()
def short_client(short_lease_settings: Settings):
    with TestClient(create_app(short_lease_settings)) as test_client:
        yield test_client


@pytest.fixture()
def restartable_short_client(short_lease_settings: Settings):
    clients = []

    def factory():
        test_client = TestClient(create_app(short_lease_settings))
        test_client.__enter__()
        clients.append(test_client)
        return test_client

    yield factory
    for test_client in clients:
        test_client.__exit__(None, None, None)


def _backdate_lease(settings: Settings, subscriber_id: str, *, age_seconds: float) -> None:
    """Force the open lease of a subscriber into the past (expiry simulation)."""

    conn = store.connect(settings)
    try:
        row = conn.execute(
            "SELECT lease_expires_at FROM subscription_claims WHERE subscriber_id = ?",
            (subscriber_id,),
        ).fetchone()
        assert row is not None
        conn.execute(
            "UPDATE subscription_claims SET lease_expires_at = ? WHERE subscriber_id = ?",
            (float(row["lease_expires_at"]) - age_seconds, subscriber_id),
        )
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# Registration and authentication
# ---------------------------------------------------------------------------


def test_registration_starts_at_version_zero(client):
    response = client.post("/api/v1/subscriptions", json={"name": "ingest-1"})
    assert response.status_code == 201, response.text
    body = response.json()
    assert body["lease_seconds"] == 60.0
    sid, secret = body["subscriber_id"], body["secret"]
    assert sid.startswith("sub_")
    assert len(secret) >= 32
    status = client.get("/api/v1/subscriptions/me", headers=_headers((sid, secret)))
    assert status.status_code == 200
    assert status.json() == {
        "subscriber_id": sid,
        "last_ack_version": 0,
        "latest_published_version": 0,
        "pending_versions": 0,
    }


def test_credentials_are_required_and_checked(client):
    sid, secret = _register(client)
    assert client.post("/api/v1/subscriptions/claims").status_code == 401
    assert (
        client.post(
            "/api/v1/subscriptions/claims",
            headers={"X-Subscriber-Id": "sub_does_not_exist", "X-Subscriber-Secret": secret},
        ).status_code
        == 401
    )
    assert (
        client.post(
            "/api/v1/subscriptions/claims",
            headers={"X-Subscriber-Id": sid, "X-Subscriber-Secret": "wrong-secret"},
        ).status_code
        == 401
    )
    detail = client.post(
        "/api/v1/subscriptions/claims",
        headers={"X-Subscriber-Id": sid, "X-Subscriber-Secret": "wrong-secret"},
    ).json()["detail"]
    assert detail["code"] == "invalid_credentials"


# ---------------------------------------------------------------------------
# Ordered claim / confirm flow
# ---------------------------------------------------------------------------


def test_claim_before_any_publication_is_404(client):
    identity = _register(client)
    response = _claim(client, identity)
    assert response.status_code == 404
    assert response.json()["detail"]["code"] == "no_notification"
    # No lease row must be created by a failed claim.
    conn = store.connect(client.app.state.settings)
    try:
        count = conn.execute("SELECT COUNT(*) AS n FROM subscription_claims").fetchone()["n"]
    finally:
        conn.close()
    assert count == 0


def test_full_ordered_delivery_flow(client):
    identity = _register(client)
    first = _publish(client, expected_version=0, day="2026-01-01")
    second = _publish(client, expected_version=1, day="2026-01-02")

    notification = _claim(client, identity)
    assert notification.status_code == 200, notification.text
    note = notification.json()
    assert note["version"] == 1
    assert note["day"] == "2026-01-01"
    assert note["replaced_version"] is None
    assert note["sha256"] == first["image"]["sha256"]
    assert note["download_url"] == "/api/v1/mosaics/1/download"
    assert note["claim"]["token"]
    assert note["claim"]["leased_at"] < note["claim"]["lease_expires_at"]

    # The advertised fixed URL serves exactly the hashed bytes.
    downloaded = client.get(note["download_url"])
    assert downloaded.status_code == 200
    assert downloaded.headers["etag"].strip('"') == note["sha256"]

    # Before confirmation, the same version and credential keep coming back.
    again = _claim(client, identity).json()
    assert again["version"] == 1
    assert again["claim"]["token"] == note["claim"]["token"]

    confirmed = _confirm(client, identity, version=1, token=note["claim"]["token"])
    assert confirmed.status_code == 200, confirmed.text
    body = confirmed.json()
    assert body["confirmed"] is True
    assert body["repeated"] is False
    assert body["last_ack_version"] == 1
    assert body["latest_published_version"] == 2
    assert body["pending_versions"] == 1

    next_note = _claim(client, identity).json()
    assert next_note["version"] == 2
    assert next_note["day"] == "2026-01-02"
    assert next_note["replaced_version"] is None
    assert next_note["sha256"] == second["image"]["sha256"]


def test_replaced_version_points_at_the_superseded_same_day_image(client):
    identity = _register(client)
    _publish(client, expected_version=0, day="2026-01-01")
    _publish(client, expected_version=1, day="2026-01-02")
    republished = _publish(client, expected_version=2, day="2026-01-01")
    assert republished["supersedes_version"] == 1

    first = _claim(client, identity).json()
    assert first["replaced_version"] is None
    _confirm(client, identity, version=1, token=first["claim"]["token"])
    second = _claim(client, identity).json()
    assert second["version"] == 2 and second["replaced_version"] is None
    _confirm(client, identity, version=2, token=second["claim"]["token"])
    third = _claim(client, identity).json()
    # Version 3 replaces the previous image of the same observation day (v1).
    assert third["version"] == 3
    assert third["day"] == "2026-01-01"
    assert third["replaced_version"] == 1


def test_repeated_confirmation_is_idempotent_and_never_skips(client):
    identity = _register(client)
    _publish(client, expected_version=0)
    _publish(client, expected_version=1)
    note = _claim(client, identity).json()

    first = _confirm(client, identity, version=1, token=note["claim"]["token"])
    assert first.status_code == 200
    assert first.json()["repeated"] is False

    duplicate = _confirm(client, identity, version=1, token=note["claim"]["token"])
    assert duplicate.status_code == 200
    payload = duplicate.json()
    assert payload["repeated"] is True
    assert payload["last_ack_version"] == 1

    # Progress did not jump: the next claim still serves version 2, not 3.
    assert _claim(client, identity).json()["version"] == 2


def test_out_of_order_confirmation_is_rejected(client):
    identity = _register(client)
    for i in range(2):
        _publish(client, expected_version=i)
    note = _claim(client, identity).json()
    # Skipping ahead to version 2 without confirming version 1.
    response = _confirm(client, identity, version=2, token=note["claim"]["token"])
    assert response.status_code == 409
    detail = response.json()["detail"]
    assert detail["code"] == "invalid_claim_token"
    assert detail["expected_version"] == 1
    assert client.get("/api/v1/subscriptions/me", headers=_headers(identity)).json()[
        "last_ack_version"
    ] == 0


def test_confirmation_without_claim_is_rejected(client):
    identity = _register(client)
    _publish(client, expected_version=0)
    response = _confirm(client, identity, version=1, token="x" * 40)
    assert response.status_code == 409
    assert response.json()["detail"]["code"] == "no_active_claim"


# ---------------------------------------------------------------------------
# Leases
# ---------------------------------------------------------------------------


def test_repeated_claims_return_the_same_credential_while_lease_open(client):
    identity = _register(client)
    _publish(client, expected_version=0)
    first = _claim(client, identity).json()
    for _ in range(3):
        response = _claim(client, identity)
        assert response.status_code == 200
        note = response.json()
        assert note["version"] == 1
        assert note["claim"]["token"] == first["claim"]["token"]
        assert note["claim"]["lease_expires_at"] == first["claim"]["lease_expires_at"]


def test_lease_expiry_allows_reclaim_with_a_new_token(short_client, short_lease_settings):
    identity = _register(short_client)
    _publish(short_client, expected_version=0)
    first = _claim(short_client, identity).json()

    # Still inside the short lease: identical credential.
    assert _claim(short_client, identity).json()["claim"]["token"] == first["claim"]["token"]

    time.sleep(0.3)
    renewed = _claim(short_client, identity)
    assert renewed.status_code == 200
    second = renewed.json()
    assert second["version"] == 1
    assert second["claim"]["token"] != first["claim"]["token"]

    # The old credential is now dead and cannot advance progress.
    stale = _confirm(short_client, identity, version=1, token=first["claim"]["token"])
    assert stale.status_code == 409
    assert stale.json()["detail"]["code"] == "claim_superseded"
    assert (
        short_client.get("/api/v1/subscriptions/me", headers=_headers(identity)).json()[
            "last_ack_version"
        ]
        == 0
    )

    # The fresh credential completes the version normally.
    done = _confirm(short_client, identity, version=1, token=second["claim"]["token"])
    assert done.status_code == 200
    assert done.json()["last_ack_version"] == 1


def test_expired_unrenewed_lease_cannot_confirm(short_client):
    identity = _register(short_client)
    _publish(short_client, expected_version=0)
    note = _claim(short_client, identity).json()

    time.sleep(0.3)
    # The token has simply expired without a re-claim.  It must not advance
    # progress; the subscriber has to re-claim and use the fresh credential.
    stale = _confirm(short_client, identity, version=1, token=note["claim"]["token"])
    assert stale.status_code == 409
    assert stale.json()["detail"]["code"] == "lease_expired"
    assert (
        short_client.get("/api/v1/subscriptions/me", headers=_headers(identity)).json()[
            "last_ack_version"
        ]
        == 0
    )

    renewed = _claim(short_client, identity).json()
    assert renewed["claim"]["token"] != note["claim"]["token"]
    assert _confirm(
        short_client, identity, version=1, token=renewed["claim"]["token"]
    ).status_code == 200


def test_unknown_token_never_advances_progress(client):
    identity = _register(client)
    _publish(client, expected_version=0)
    note = _claim(client, identity).json()
    response = _confirm(client, identity, version=1, token="a" * len(note["claim"]["token"]))
    assert response.status_code == 409
    assert response.json()["detail"]["code"] == "invalid_claim_token"
    assert client.get("/api/v1/subscriptions/me", headers=_headers(identity)).json()[
        "last_ack_version"
    ] == 0


# ---------------------------------------------------------------------------
# Availability: stop at the version whose file is missing
# ---------------------------------------------------------------------------


def test_claim_stops_at_unavailable_image_and_resumes_afterwards(
    short_client, short_lease_settings
):
    identity = _register(short_client)
    _publish(short_client, expected_version=0)
    _publish(short_client, expected_version=1)

    # Subscriber takes version 1 and confirms ingestion.
    first = _claim(short_client, identity).json()
    assert first["version"] == 1
    assert _confirm(
        short_client, identity, version=1, token=first["claim"]["token"]
    ).status_code == 200

    # The file of the next version vanishes (e.g. volume hiccup): the claim
    # must stop on version 2 instead of skipping to version 3, and no lease
    # may be opened.
    image = short_lease_settings.versions_dir / "v2.tif"
    original_bytes = image.read_bytes()
    image.unlink()
    blocked = _claim(short_client, identity)
    assert blocked.status_code == 503
    detail = blocked.json()["detail"]
    assert detail["code"] == "image_unavailable"
    assert detail["version"] == 2

    conn = store.connect(short_lease_settings)
    try:
        row = conn.execute(
            "SELECT version, status FROM subscription_claims WHERE subscriber_id = ?",
            (identity[0],),
        ).fetchone()
    finally:
        conn.close()
    assert row["status"] == "confirmed"
    assert row["version"] == 1

    # Retrying while the file is gone keeps failing at the same version.
    assert _claim(short_client, identity).status_code == 503

    # Once the file is back, the very same version becomes claimable.
    image.write_bytes(original_bytes)
    recovered = _claim(short_client, identity)
    assert recovered.status_code == 200
    note = recovered.json()
    assert note["version"] == 2
    assert _confirm(
        short_client, identity, version=2, token=note["claim"]["token"]
    ).status_code == 200


def test_blocking_version_does_not_leak_later_versions(short_client, short_lease_settings):
    identity = _register(short_client)
    for i in range(3):
        _publish(short_client, expected_version=i)
    first = _claim(short_client, identity).json()
    _confirm(short_client, identity, version=1, token=first["claim"]["token"])

    (short_lease_settings.versions_dir / "v2.tif").unlink()
    # Even though version 3 exists and its file is present, the subscriber is
    # held at version 2.
    response = _claim(short_client, identity)
    assert response.status_code == 503
    assert response.json()["detail"]["version"] == 2
    assert (short_lease_settings.versions_dir / "v3.tif").is_file()


# ---------------------------------------------------------------------------
# Durability and isolation (true multi-thread HTTP concurrency lives in
# test_subscriptions_concurrency.py, which boots a real uvicorn server)
# ---------------------------------------------------------------------------


def test_subscription_state_survives_restart(restartable_short_client):
    first = restartable_short_client()
    identity = _register(first)
    _publish(first, expected_version=0)
    note = _claim(first, identity)
    payload = note.json()
    assert payload["version"] == 1
    first.__exit__(None, None, None)

    # After restart the open lease is still valid (same credential, same expiry).
    restarted = restartable_short_client()
    same = _claim(restarted, identity)
    assert same.status_code == 200
    assert same.json()["claim"]["token"] == payload["claim"]["token"]
    assert (
        same.json()["claim"]["lease_expires_at"] == payload["claim"]["lease_expires_at"]
    )
    assert _confirm(
        restarted, identity, version=1, token=payload["claim"]["token"]
    ).status_code == 200
    restarted.__exit__(None, None, None)

    # Confirmed progress persists as well.
    again = restartable_short_client()
    status = again.get("/api/v1/subscriptions/me", headers=_headers(identity))
    assert status.json()["last_ack_version"] == 1
    assert _claim(again, identity).status_code == 404


def test_subscribers_are_independent(client):
    left = _register(client, name="model-a")
    right = _register(client, name="model-b")
    _publish(client, expected_version=0)

    left_note = _claim(client, left).json()
    # The second subscriber starts independently at version 1 even though the
    # first one already holds a lease for it.
    right_note = _claim(client, right).json()
    assert left_note["version"] == right_note["version"] == 1
    assert left_note["claim"]["token"] != right_note["claim"]["token"]

    assert _confirm(client, left, version=1, token=left_note["claim"]["token"]).status_code == 200
    # Confirming for one subscriber does not move the other's progress.
    assert (
        client.get("/api/v1/subscriptions/me", headers=_headers(right)).json()[
            "last_ack_version"
        ]
        == 0
    )
    assert _claim(client, right).json()["claim"]["token"] == right_note["claim"]["token"]
    assert _confirm(client, right, version=1, token=right_note["claim"]["token"]).status_code == 200
    assert (
        client.get("/api/v1/subscriptions/me", headers=_headers(left)).json()[
            "last_ack_version"
        ]
        == 1
    )


def test_many_versions_are_delivered_without_gaps_or_reordering(client):
    identity = _register(client)
    for i in range(6):
        _publish(client, expected_version=i, day=f"2026-01-{i + 1:02d}")

    seen: list[int] = []
    while True:
        note = _claim(client, identity)
        if note.status_code == 404:
            break
        payload = note.json()
        seen.append(payload["version"])
        # Fixed URL must always match the advertised hash.
        download = client.get(payload["download_url"])
        assert download.headers["etag"].strip('"') == payload["sha256"]
        confirmed = _confirm(
            client, identity, version=payload["version"], token=payload["claim"]["token"]
        )
        assert confirmed.status_code == 200

    assert seen == [1, 2, 3, 4, 5, 6]
    assert (
        client.get("/api/v1/subscriptions/me", headers=_headers(identity)).json()[
            "pending_versions"
        ]
        == 0
    )


def test_lease_backdated_in_database_is_treated_as_expired(client, settings):
    # Uses the default (60 s) client but rewinds the persisted expiry to prove
    # expiry is evaluated against durable state, not in-memory timers.
    identity = _register(client)
    _publish(client, expected_version=0)
    first = _claim(client, identity).json()

    _backdate_lease(settings, identity[0], age_seconds=120)
    renewed = _claim(client, identity).json()
    assert renewed["version"] == 1
    assert renewed["claim"]["token"] != first["claim"]["token"]

    stale = _confirm(client, identity, version=1, token=first["claim"]["token"])
    assert stale.status_code == 409
    assert stale.json()["detail"]["code"] == "claim_superseded"
