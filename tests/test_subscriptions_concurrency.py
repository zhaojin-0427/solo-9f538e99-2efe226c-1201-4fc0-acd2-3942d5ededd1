"""Concurrency tests against a real uvicorn server over HTTP.

The in-process ``TestClient`` funnels every request through a small anyio
worker threadpool, which cannot represent real concurrency while a publication
is holding a worker.  These tests therefore boot the actual ASGI app with
uvicorn on an ephemeral port and drive it with ``httpx``.
"""

from __future__ import annotations

import socket
import threading
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import httpx
import numpy as np
import pytest
import uvicorn

from app.config import Settings
from app.main import create_app
from app.rasters import NODATA
from tests.conftest import make_geotiff, submit_payload

A_TILE = np.array([[1, 2], [3, 4]], dtype=np.int16)
B_TILE = np.array([[NODATA, 2], [NODATA, 4]], dtype=np.int16)


def _free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind(("127.0.0.1", 0))
        return int(sock.getsockname()[1])


class LiveServer:
    def __init__(self, settings: Settings):
        self.base_url = f"http://127.0.0.1:{_free_port()}"
        self.config = uvicorn.Config(
            create_app(settings),
            host="127.0.0.1",
            port=int(self.base_url.rsplit(":", 1)[1]),
            log_level="warning",
        )
        self.server = uvicorn.Server(self.config)
        self.thread = threading.Thread(target=self.server.run, daemon=True)

    def __enter__(self) -> "LiveServer":
        self.thread.start()
        for _ in range(100):
            try:
                httpx.get(f"{self.base_url}/healthz", timeout=1)
                return self
            except httpx.TransportError:
                threading.Event().wait(0.05)
        raise RuntimeError("live server did not become ready")

    def __exit__(self, *exc) -> None:
        self.server.should_exit = True
        self.thread.join(timeout=10)


@pytest.fixture()
def live_settings(tmp_path: Path) -> Settings:
    return Settings(data_dir=tmp_path / "data", claim_lease_seconds=60.0)


@pytest.fixture()
def server(live_settings: Settings):
    with LiveServer(live_settings) as live:
        yield live


def _publish(client: httpx.Client, *, expected_version: int, day: str = "2026-01-01"):
    data, files = submit_payload(
        block_a=make_geotiff(A_TILE),
        block_b=make_geotiff(B_TILE, x0=0.01),
        expected_version=expected_version,
        day=day,
    )
    response = client.post("/api/v1/mosaics", data=data, files=files)
    assert response.status_code == 201, response.text
    return response.json()


def _register(client: httpx.Client, name: str = "hydro-model"):
    body = client.post("/api/v1/subscriptions", json={"name": name}).json()
    return body["subscriber_id"], body["secret"]


def _headers(identity):
    return {"X-Subscriber-Id": identity[0], "X-Subscriber-Secret": identity[1]}


def test_concurrent_claims_and_confirms_keep_single_order(server: LiveServer):
    with httpx.Client(base_url=server.base_url, timeout=30) as client:
        identity = _register(client)
        for i in range(4):
            _publish(client, expected_version=i)

        barrier = threading.Barrier(4)
        confirmed: list[int] = []
        lock = threading.Lock()

        def worker():
            barrier.wait()
            for _ in range(4):
                note = client.post("/api/v1/subscriptions/claims", headers=_headers(identity))
                if note.status_code != 200:
                    continue
                payload = note.json()
                done = client.post(
                    "/api/v1/subscriptions/confirmations",
                    headers=_headers(identity),
                    json={
                        "version": payload["version"],
                        "claim_token": payload["claim"]["token"],
                    },
                )
                if done.status_code == 200 and not done.json().get("repeated"):
                    with lock:
                        confirmed.append(payload["version"])

        with ThreadPoolExecutor(max_workers=4) as pool:
            list(pool.submit(worker) for _ in range(4))

        assert sorted(confirmed) == [1, 2, 3, 4]
        status = client.get("/api/v1/subscriptions/me", headers=_headers(identity)).json()
        assert status["last_ack_version"] == 4
        assert status["pending_versions"] == 0


@pytest.fixture()
def gated_writer(monkeypatch):
    """Gate ``rasters.write_mosaic``; only calls after ``arm()`` block."""

    from app import rasters

    state = {"armed": False}
    started = threading.Event()
    release = threading.Event()
    real_write = rasters.write_mosaic

    def slow_write(left, right, destination):
        if not state["armed"]:
            return real_write(left, right, destination)
        started.set()
        assert release.wait(timeout=30)
        return real_write(left, right, destination)

    monkeypatch.setattr(rasters, "write_mosaic", slow_write)

    def arm():
        state["armed"] = True

    yield arm, started, release
    release.set()
    monkeypatch.setattr(rasters, "write_mosaic", real_write)


def test_claim_during_inflight_publication_only_returns_committed_versions(
    server: LiveServer, gated_writer
):
    arm, started, release = gated_writer
    with httpx.Client(base_url=server.base_url, timeout=30) as client:
        identity = _register(client)
        _publish(client, expected_version=0)
        note = client.post(
            "/api/v1/subscriptions/claims", headers=_headers(identity)
        ).json()
        client.post(
            "/api/v1/subscriptions/confirmations",
            headers=_headers(identity),
            json={"version": note["version"], "claim_token": note["claim"]["token"]},
        )

        failures: list[Exception] = []

        def publish_second():
            try:
                result = _publish(client, expected_version=1)
                assert result["version"] == 2
            except Exception as exc:  # pragma: no cover - surfaced below
                failures.append(exc)

        arm()
        publisher = threading.Thread(target=publish_second, daemon=True)
        publisher.start()
        assert started.wait(timeout=10)
        try:
            # The publication is still uncommitted: no new notification.
            blocked = client.post(
                "/api/v1/subscriptions/claims", headers=_headers(identity)
            )
            assert blocked.status_code == 404
            assert blocked.json()["detail"]["code"] == "no_notification"
        finally:
            release.set()
        publisher.join(timeout=30)
        assert not publisher.is_alive()
        assert not failures

        delivered = client.post(
            "/api/v1/subscriptions/claims", headers=_headers(identity)
        )
        assert delivered.status_code == 200
        assert delivered.json()["version"] == 2


def test_claim_and_confirm_interleaved_with_new_publications(server: LiveServer):
    """New publications while a subscriber works never cause a gap or jump."""

    with httpx.Client(base_url=server.base_url, timeout=30) as client:
        identity = _register(client)
        _publish(client, expected_version=0)

        seen: list[int] = []
        note = client.post(
            "/api/v1/subscriptions/claims", headers=_headers(identity)
        ).json()
        # A second and third version land while the first lease is still open.
        _publish(client, expected_version=1, day="2026-01-02")
        _publish(client, expected_version=2, day="2026-01-03")
        # Open lease stays pinned to version 1.
        assert (
            client.post("/api/v1/subscriptions/claims", headers=_headers(identity)).json()[
                "version"
            ]
            == 1
        )
        client.post(
            "/api/v1/subscriptions/confirmations",
            headers=_headers(identity),
            json={"version": 1, "claim_token": note["claim"]["token"]},
        )
        seen.append(1)

        for expected in (2, 3):
            payload = client.post(
                "/api/v1/subscriptions/claims", headers=_headers(identity)
            ).json()
            assert payload["version"] == expected
            client.post(
                "/api/v1/subscriptions/confirmations",
                headers=_headers(identity),
                json={
                    "version": payload["version"],
                    "claim_token": payload["claim"]["token"],
                },
            )
            seen.append(expected)

        assert seen == [1, 2, 3]
        assert (
            client.post("/api/v1/subscriptions/claims", headers=_headers(identity)).status_code
            == 404
        )
