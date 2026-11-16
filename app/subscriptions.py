"""Subscriber registry and the claim/lease/confirm state machine.

A subscriber (hydrology model ingest job) starts at version 0 and receives one
published image notification at a time, strictly in publication order.

Sequence for one version::

    claim  -> (lease 60 s) -> download the fixed version URL -> confirm
                                  |                          |
                                  +-- same claim while open  +-- next claim
                                      (same credential)          returns next version

Durability rules, all enforced inside SQLite ``BEGIN IMMEDIATE`` transactions:

* progress (``last_ack_version``) only moves forward, once, on a confirm that
  presents the *active* claim token; a repeated confirm is idempotent and can
  never skip a version;
* a token from an expired lease that has been re-claimed is deactivated and
  cannot move progress;
* claims only ever target rows already present in the committed ``versions``
  table, so a publication in flight is never observable;
* if the image file of the next version is not on disk the claim stops at that
  version with ``ImageUnavailable`` and creates no lease, so the subscriber
  retries the same version once the file is back;
* every piece of state is persisted, so a service restart changes nothing;
* subscribers are independent rows and never block or observe each other.
"""

from __future__ import annotations

import hmac
import logging
import secrets
import sqlite3
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any

from .config import Settings
from .rasters import iso_utc

log = logging.getLogger(__name__)

SCHEMA = """
CREATE TABLE IF NOT EXISTS subscribers (
    subscriber_id     TEXT PRIMARY KEY,
    name              TEXT,
    secret            TEXT NOT NULL,
    created_at        TEXT NOT NULL,
    last_ack_version  INTEGER NOT NULL DEFAULT 0
);

CREATE TABLE IF NOT EXISTS subscription_claims (
    subscriber_id   TEXT PRIMARY KEY
                    REFERENCES subscribers(subscriber_id) ON DELETE CASCADE,
    version         INTEGER NOT NULL,
    status          TEXT NOT NULL CHECK (status IN ('leased', 'confirmed')),
    leased_at       TEXT NOT NULL,
    lease_expires_at REAL NOT NULL,
    token           TEXT NOT NULL,
    updated_at      TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS subscription_claim_tokens (
    subscriber_id  TEXT NOT NULL,
    version        INTEGER NOT NULL,
    token          TEXT NOT NULL,
    active         INTEGER NOT NULL,
    created_at     TEXT NOT NULL,
    PRIMARY KEY (subscriber_id, version, token)
);
"""


class SubscriptionError(Exception):
    """Base class for subscription failures mapped to HTTP errors."""

    status_code = 400
    code = "subscription_error"

    def __init__(self, message: str, **extra: Any):
        super().__init__(message)
        self.message = message
        self.extra = extra

    def as_detail(self) -> dict[str, Any]:
        detail: dict[str, Any] = {"code": self.code, "message": self.message}
        detail.update(self.extra)
        return detail


class UnknownSubscriber(SubscriptionError):
    status_code = 401
    code = "invalid_credentials"


class BadSecret(SubscriptionError):
    status_code = 401
    code = "invalid_credentials"


class NoPublishedVersion(SubscriptionError):
    status_code = 404
    code = "no_notification"


class ImageUnavailable(SubscriptionError):
    status_code = 503
    code = "image_unavailable"


class NoActiveClaim(SubscriptionError):
    status_code = 409
    code = "no_active_claim"


class InvalidClaimToken(SubscriptionError):
    status_code = 409
    code = "invalid_claim_token"


class ClaimSuperseded(SubscriptionError):
    status_code = 409
    code = "claim_superseded"


class LeaseExpired(SubscriptionError):
    status_code = 409
    code = "lease_expired"


@dataclass(frozen=True)
class Subscriber:
    subscriber_id: str
    name: str | None
    secret: str
    created_at: str
    last_ack_version: int


def init_subscriptions(conn: sqlite3.Connection) -> None:
    conn.executescript(SCHEMA)


def _now() -> tuple[float, str]:
    instant = datetime.now(timezone.utc)
    return instant.timestamp(), iso_utc(instant)


def _new_token() -> str:
    return secrets.token_urlsafe(32)


def register(conn: sqlite3.Connection, name: str | None) -> Subscriber:
    """Create a subscriber starting at version 0."""

    subscriber_id = f"sub_{secrets.token_hex(16)}"
    secret = secrets.token_urlsafe(32)
    _, created_at = _now()
    clean_name = name.strip() if isinstance(name, str) and name.strip() else None
    conn.execute(
        "INSERT INTO subscribers (subscriber_id, name, secret, created_at, last_ack_version) "
        "VALUES (?, ?, ?, ?, 0)",
        (subscriber_id, clean_name, secret, created_at),
    )
    log.info("registered subscriber %s (%s)", subscriber_id, clean_name or "unnamed")
    return Subscriber(
        subscriber_id=subscriber_id,
        name=clean_name,
        secret=secret,
        created_at=created_at,
        last_ack_version=0,
    )


def authenticate(
    conn: sqlite3.Connection, subscriber_id: str | None, secret: str | None
) -> Subscriber:
    """Resolve and authenticate a subscriber from request headers."""

    if not subscriber_id or not secret:
        raise UnknownSubscriber(
            "missing credentials; send X-Subscriber-Id and X-Subscriber-Secret"
        )
    row = conn.execute(
        "SELECT * FROM subscribers WHERE subscriber_id = ?",
        (subscriber_id,),
    ).fetchone()
    if row is None:
        raise UnknownSubscriber("unknown subscriber id")
    # Constant-time comparison so the secret is not guessable via timing.
    if not hmac.compare_digest(secret, row["secret"]):
        raise BadSecret("subscriber secret does not match")
    return Subscriber(
        subscriber_id=row["subscriber_id"],
        name=row["name"],
        secret=row["secret"],
        created_at=row["created_at"],
        last_ack_version=int(row["last_ack_version"]),
    )


def _version_row(conn: sqlite3.Connection, version: int) -> sqlite3.Row | None:
    return conn.execute(
        "SELECT * FROM versions WHERE version = ?", (version,)
    ).fetchone()


def claim(
    conn: sqlite3.Connection,
    subscriber: Subscriber,
    *,
    settings: Settings,
) -> dict[str, Any]:
    """Return the one notification this subscriber is allowed to process now.

    Opens a new lease for the next unacknowledged version, returns the open
    lease unchanged while it is still live, or re-leases the same version with
    a fresh credential once the previous lease has expired.

    All read checks run first against a WAL snapshot without taking the write
    lock, so a claim never queues behind an in-flight publication.  The write
    lock is acquired only to create or renew a lease, and every decision is
    re-validated inside that transaction.
    """

    # Phase 1: read-only snapshot.  WAL permits this while a publication holds
    # the write lock, so this can never block on publishing.
    for _attempt in range(2):
        sub_row = conn.execute(
            "SELECT last_ack_version FROM subscribers WHERE subscriber_id = ?",
            (subscriber.subscriber_id,),
        ).fetchone()
        assert sub_row is not None
        acked = int(sub_row["last_ack_version"])
        next_version = acked + 1
        version_row = _version_row(conn, next_version)
        if version_row is None:
            raise NoPublishedVersion(
                "no new published version is available yet",
                last_ack_version=acked,
            )

        image_path = settings.versions_dir / f"v{next_version}.tif"
        if not image_path.is_file():
            # Stop at this version: do not open a lease and do not skip ahead.
            log.error(
                "subscriber %s blocked: image for version %s missing at %s",
                subscriber.subscriber_id,
                next_version,
                image_path,
            )
            raise ImageUnavailable(
                f"image for version {next_version} is currently unavailable; "
                "retry the claim later",
                version=next_version,
            )

        now_ts, now_iso = _now()
        existing = conn.execute(
            "SELECT * FROM subscription_claims WHERE subscriber_id = ?",
            (subscriber.subscriber_id,),
        ).fetchone()

        if (
            existing is not None
            and int(existing["version"]) == next_version
            and existing["status"] == "leased"
            and float(existing["lease_expires_at"]) > now_ts
        ):
            # Open lease: hand back the exact same credential, never a new one.
            # Purely read-only, so no write lock is needed at all.
            return _notification_with_lease(version_row, existing)

        # Phase 2: a lease must be created or renewed.  Re-validate everything
        # inside the write transaction; another claim, confirm or publication
        # may have landed between the snapshot and the lock acquisition.
        try:
            conn.execute("BEGIN IMMEDIATE")
        except sqlite3.OperationalError as exc:
            if "locked" in str(exc).lower() or "busy" in str(exc).lower():
                raise _busy() from exc
            raise

        try:
            locked_row = conn.execute(
                "SELECT last_ack_version FROM subscribers WHERE subscriber_id = ?",
                (subscriber.subscriber_id,),
            ).fetchone()
            assert locked_row is not None
            locked_next = int(locked_row["last_ack_version"]) + 1
            if locked_next != next_version:
                # Progress moved concurrently: release the lock and take one
                # fresh snapshot instead of recursing.
                conn.execute("ROLLBACK")
                continue

            locked_version = _version_row(conn, next_version)
            if locked_version is None:
                conn.execute("ROLLBACK")
                raise NoPublishedVersion(
                    "no new published version is available yet",
                    last_ack_version=int(locked_row["last_ack_version"]),
                )
            if not image_path.is_file():
                conn.execute("ROLLBACK")
                raise ImageUnavailable(
                    f"image for version {next_version} is currently unavailable; "
                    "retry the claim later",
                    version=next_version,
                )

            locked_now_ts, locked_now_iso = _now()
            locked_existing = conn.execute(
                "SELECT * FROM subscription_claims WHERE subscriber_id = ?",
                (subscriber.subscriber_id,),
            ).fetchone()
            if (
                locked_existing is not None
                and int(locked_existing["version"]) == next_version
                and locked_existing["status"] == "leased"
                and float(locked_existing["lease_expires_at"]) > locked_now_ts
            ):
                # A concurrent request opened the lease first: return it.
                payload = _notification_with_lease(locked_version, locked_existing)
                conn.execute("COMMIT")
                return payload

            token = _new_token()
            lease_expires_at = locked_now_ts + settings.claim_lease_seconds
            if locked_existing is None:
                conn.execute(
                    "INSERT INTO subscription_claims "
                    "(subscriber_id, version, status, leased_at, lease_expires_at, token, updated_at) "
                    "VALUES (?, ?, 'leased', ?, ?, ?, ?)",
                    (
                        subscriber.subscriber_id,
                        next_version,
                        locked_now_iso,
                        lease_expires_at,
                        token,
                        locked_now_iso,
                    ),
                )
            else:
                # Expired lease (or a stale row left behind by an earlier flow):
                # deactivate every old token for this version and issue a fresh
                # credential.  Progress itself never moves here.
                conn.execute(
                    "UPDATE subscription_claim_tokens SET active = 0 "
                    "WHERE subscriber_id = ? AND version = ?",
                    (subscriber.subscriber_id, int(locked_existing["version"])),
                )
                conn.execute(
                    "UPDATE subscription_claims SET version = ?, status = 'leased', "
                    "leased_at = ?, lease_expires_at = ?, token = ?, updated_at = ? "
                    "WHERE subscriber_id = ?",
                    (
                        next_version,
                        locked_now_iso,
                        lease_expires_at,
                        token,
                        locked_now_iso,
                        subscriber.subscriber_id,
                    ),
                )
            conn.execute(
                "INSERT INTO subscription_claim_tokens "
                "(subscriber_id, version, token, active, created_at) VALUES (?, ?, ?, 1, ?)",
                (subscriber.subscriber_id, next_version, token, locked_now_iso),
            )
            row = conn.execute(
                "SELECT * FROM subscription_claims WHERE subscriber_id = ?",
                (subscriber.subscriber_id,),
            ).fetchone()
            conn.execute("COMMIT")
            log.info(
                "subscriber %s claimed version %s (lease until %s)",
                subscriber.subscriber_id,
                next_version,
                row["lease_expires_at"],
            )
            return _notification_with_lease(locked_version, row)
        except Exception:
            _rollback(conn)
            raise

    # Two snapshot/lock rounds raced against concurrent progress changes; the
    # caller simply retries the claim.
    raise _busy()


def _notification_with_lease(
    version_row: sqlite3.Row, claim_row: sqlite3.Row
) -> dict[str, Any]:
    version = int(version_row["version"])
    superseded = version_row["supersedes_version"]
    return {
        "version": version,
        "day": version_row["day"],
        "replaced_version": int(superseded) if superseded is not None else None,
        "sha256": version_row["sha256"],
        "download_url": f"/api/v1/mosaics/{version}/download",
        "claim": {
            "token": claim_row["token"],
            "leased_at": claim_row["leased_at"],
            "lease_expires_at": iso_utc(
                datetime.fromtimestamp(float(claim_row["lease_expires_at"]), timezone.utc)
            ),
        },
    }


def confirm(
    conn: sqlite3.Connection,
    subscriber: Subscriber,
    *,
    version: int,
    token: str,
) -> dict[str, Any]:
    """Acknowledge a downloaded and ingested version and advance progress.

    Only the currently active token for the subscriber's open claim at exactly
    the next expected version can advance progress.  Repeating a completed
    confirmation is idempotent; stale or foreign tokens are rejected without
    touching progress.
    """

    if version < 1:
        raise InvalidClaimToken("version must be a positive version number", version=version)
    if not token:
        raise InvalidClaimToken("claim token is required", version=version)

    # Phase 1: read-only snapshot checks (WAL readers never block a publisher).
    sub_row = conn.execute(
        "SELECT last_ack_version FROM subscribers WHERE subscriber_id = ?",
        (subscriber.subscriber_id,),
    ).fetchone()
    assert sub_row is not None
    acked = int(sub_row["last_ack_version"])

    # Idempotent replay of an already confirmed version: never skips ahead.
    if version <= acked:
        return _status(conn, subscriber.subscriber_id, acked, confirmed=True, repeated=True)

    next_version = acked + 1
    if version != next_version:
        # A confirm may only target the exact next version in order.
        raise InvalidClaimToken(
            f"out-of-order confirmation: expected version {next_version}, got {version}",
            expected_version=next_version,
            received_version=version,
        )

    claim_row = conn.execute(
        "SELECT * FROM subscription_claims WHERE subscriber_id = ?",
        (subscriber.subscriber_id,),
    ).fetchone()
    if claim_row is None or int(claim_row["version"]) != version:
        raise NoActiveClaim(
            f"no active claim for version {version}; claim it before confirming",
            version=version,
        )

    now_ts, _ = _now()
    if float(claim_row["lease_expires_at"]) <= now_ts:
        # The 60 s lease has elapsed: the credential can no longer move
        # progress.  The subscriber must re-claim the same version.
        raise LeaseExpired(
            "the claim lease has expired; re-claim this version and retry",
            version=version,
        )

    token_row = conn.execute(
        "SELECT * FROM subscription_claim_tokens "
        "WHERE subscriber_id = ? AND version = ? AND token = ?",
        (subscriber.subscriber_id, version, token),
    ).fetchone()
    if token_row is None:
        # The token was never issued for this subscriber/version.
        raise InvalidClaimToken("unknown claim token", version=version)
    if int(token_row["active"]) != 1:
        # The lease expired and the version was re-claimed: this old
        # credential must not advance progress.
        raise ClaimSuperseded(
            "this claim token was superseded by a newer lease after expiry",
            version=version,
        )

    # Phase 2: re-validate under the write lock and atomically advance
    # progress.  A concurrent confirmation of the same subscriber can only
    # finish one of the two attempts.
    try:
        conn.execute("BEGIN IMMEDIATE")
    except sqlite3.OperationalError as exc:
        if "locked" in str(exc).lower() or "busy" in str(exc).lower():
            raise _busy() from exc
        raise

    try:
        locked_row = conn.execute(
            "SELECT last_ack_version FROM subscribers WHERE subscriber_id = ?",
            (subscriber.subscriber_id,),
        ).fetchone()
        assert locked_row is not None
        locked_acked = int(locked_row["last_ack_version"])
        if version <= locked_acked:
            # A concurrent request confirmed it first: report idempotent replay.
            conn.execute("COMMIT")
            return _status(
                conn, subscriber.subscriber_id, locked_acked, confirmed=True, repeated=True
            )
        if version != locked_acked + 1:
            conn.execute("ROLLBACK")
            raise InvalidClaimToken(
                f"out-of-order confirmation: expected version {locked_acked + 1}, got {version}",
                expected_version=locked_acked + 1,
                received_version=version,
            )

        locked_claim = conn.execute(
            "SELECT * FROM subscription_claims WHERE subscriber_id = ?",
            (subscriber.subscriber_id,),
        ).fetchone()
        if locked_claim is None or int(locked_claim["version"]) != version:
            conn.execute("ROLLBACK")
            raise NoActiveClaim(
                f"no active claim for version {version}; claim it before confirming",
                version=version,
            )
        locked_now_ts, locked_now_iso = _now()
        if float(locked_claim["lease_expires_at"]) <= locked_now_ts:
            conn.execute("ROLLBACK")
            raise LeaseExpired(
                "the claim lease has expired; re-claim this version and retry",
                version=version,
            )
        locked_token = conn.execute(
            "SELECT * FROM subscription_claim_tokens "
            "WHERE subscriber_id = ? AND version = ? AND token = ?",
            (subscriber.subscriber_id, version, token),
        ).fetchone()
        if locked_token is None:
            conn.execute("ROLLBACK")
            raise InvalidClaimToken("unknown claim token", version=version)
        if int(locked_token["active"]) != 1:
            conn.execute("ROLLBACK")
            raise ClaimSuperseded(
                "this claim token was superseded by a newer lease after expiry",
                version=version,
            )

        conn.execute(
            "UPDATE subscription_claims SET status = 'confirmed', updated_at = ? "
            "WHERE subscriber_id = ?",
            (locked_now_iso, subscriber.subscriber_id),
        )
        conn.execute(
            "UPDATE subscription_claim_tokens SET active = 0 "
            "WHERE subscriber_id = ? AND version = ?",
            (subscriber.subscriber_id, version),
        )
        conn.execute(
            "UPDATE subscribers SET last_ack_version = ? WHERE subscriber_id = ?",
            (version, subscriber.subscriber_id),
        )
        conn.execute("COMMIT")
        log.info("subscriber %s confirmed version %s", subscriber.subscriber_id, version)
        return _status(
            conn, subscriber.subscriber_id, version, confirmed=True, repeated=False
        )
    except Exception:
        _rollback(conn)
        raise


def status(conn: sqlite3.Connection, subscriber: Subscriber) -> dict[str, Any]:
    row = conn.execute(
        "SELECT last_ack_version FROM subscribers WHERE subscriber_id = ?",
        (subscriber.subscriber_id,),
    ).fetchone()
    assert row is not None
    acked = int(row["last_ack_version"])
    return _status(conn, subscriber.subscriber_id, acked, confirmed=False, repeated=False)


def _status(
    conn: sqlite3.Connection,
    subscriber_id: str,
    acked: int,
    *,
    confirmed: bool,
    repeated: bool,
) -> dict[str, Any]:
    latest = conn.execute(
        "SELECT COALESCE(MAX(version), 0) AS v FROM versions"
    ).fetchone()
    latest_version = int(latest["v"])
    body: dict[str, Any] = {
        "subscriber_id": subscriber_id,
        "last_ack_version": acked,
        "latest_published_version": latest_version,
        "pending_versions": max(0, latest_version - acked),
    }
    if confirmed:
        body["confirmed"] = True
        body["repeated"] = repeated
    return body


class StoreBusy(Exception):
    """The SQLite write lock could not be acquired in time."""


def _busy() -> StoreBusy:
    return StoreBusy("database is busy; retry shortly")


def _rollback(conn: sqlite3.Connection) -> None:
    if conn.in_transaction:
        try:
            conn.execute("ROLLBACK")
        except sqlite3.Error:
            log.warning("rollback failed", exc_info=True)
