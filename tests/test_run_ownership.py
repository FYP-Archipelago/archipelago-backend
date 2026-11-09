import gzip
import json
from datetime import UTC, datetime
from typing import Any
from uuid import UUID, uuid4

import httpx
import pytest
import redis

from app.db import runs
from app.ids import mint_run_id
from app.main import app
from tests.conftest import FakeRunner
from tests.helpers import LOGIN, ORIGIN, RUN_BODY, cookie_headers, csrf_headers, register, seed_run

pytestmark = pytest.mark.anyio


@pytest.mark.parametrize(
    "method,path",
    [
        ("GET", "/presets"),
        ("GET", "/runs"),
        ("POST", "/runs"),
        ("GET", "/runs/{id}"),
        ("GET", "/runs/{id}/result"),
        ("POST", "/runs/{id}/cancel"),
        ("GET", "/runs/{id}/events"),
    ],
)
async def test_protected_routes_require_auth(browser: httpx.AsyncClient, runner: FakeRunner, sync_redis: redis.Redis, method: str, path: str) -> None:
    response = await browser.request(method, path.format(id=mint_run_id()), json=RUN_BODY if method == "POST" and path == "/runs" else None)
    assert response.status_code == 401 and response.headers["cache-control"] == "no-store"
    assert runner.calls == [] and sync_redis.xlen("pending_runs") == 0
    async with app.state.pool.connection() as conn:
        assert (await (await conn.execute("SELECT count(*) AS count FROM runs")).fetchone())["count"] == 0


async def test_cross_user_run_isolation(
    browser: httpx.AsyncClient, runner: FakeRunner, sync_redis: redis.Redis, monkeypatch: pytest.MonkeyPatch
) -> None:
    owner = await register(browser)
    user_id = UUID(owner.json()["user"]["id"])
    browser.cookies.clear()
    outsider = await register(browser, email="other@example.com")
    completed = await seed_run(user_id)
    active = await seed_run(user_id)
    before = await runs.get_run(app.state.pool, active)
    result = {"graph": {"nodes": [1]}}
    await runs.save_result(app.state.pool, completed, gzip.compress(json.dumps(result).encode()))

    async def forbidden_read(*args: Any, **kwargs: Any) -> None:
        raise AssertionError("Foreign requests must not read the stream")

    monkeypatch.setattr(app.state.redis, "xread", forbidden_read)
    assert (await browser.get("/runs")).json() == {"runs": [], "next_cursor": None}
    for method, path in (
        ("GET", f"/runs/{completed}"),
        ("GET", f"/runs/{completed}/result"),
        ("GET", f"/runs/{completed}/events"),
        ("POST", f"/runs/{active}/cancel"),
    ):
        response = await browser.request(method, path, headers=csrf_headers(outsider))
        assert response.status_code == 404 and response.json() == {"detail": "run not found"}
        assert response.headers["cache-control"] == "no-store"
    assert await runs.get_run(app.state.pool, active) == before
    assert runner.calls == [] and sync_redis.xlen("pending_runs") == 0
    assert (await browser.get(f"/runs/{completed}/result", headers=cookie_headers(owner))).json() == result
    cancelled = await browser.post(f"/runs/{active}/cancel", headers=csrf_headers(owner))
    assert cancelled.status_code == 202 and cancelled.json()["status"] == "cancelling"


async def test_run_owner_from_session(browser: httpx.AsyncClient, runner: FakeRunner, sync_redis: redis.Redis) -> None:
    owner = await register(browser)
    headers = csrf_headers(owner)
    forged = await browser.post("/runs", json={**RUN_BODY, "user_id": str(uuid4())}, headers=headers)
    assert forged.status_code == 422 and runner.calls == [] and sync_redis.xlen("pending_runs") == 0
    created = await browser.post("/runs", json=RUN_BODY, headers=headers)
    assert created.status_code == 202
    async with app.state.pool.connection() as conn:
        row = await (await conn.execute("SELECT user_id FROM runs WHERE run_id = %s", (created.json()["run_id"],))).fetchone()
    assert row["user_id"] == UUID(owner.json()["user"]["id"])


@pytest.mark.parametrize("problem", ["missing-csrf", "other-session-csrf", "missing-origin", "foreign-origin"])
async def test_run_mutation_csrf_checks(browser: httpx.AsyncClient, runner: FakeRunner, sync_redis: redis.Redis, problem: str) -> None:
    owner = await register(browser)
    run_id = await seed_run(UUID(owner.json()["user"]["id"]))
    before = await runs.get_run(app.state.pool, run_id)
    headers = csrf_headers(owner)
    if problem == "missing-csrf":
        del headers["X-CSRF-Token"]
    elif problem == "other-session-csrf":
        other = await browser.post("/auth/login", json=LOGIN, headers={"Origin": ORIGIN, "Cookie": ""})
        assert other.status_code == 200
        headers["X-CSRF-Token"] = other.json()["csrf_token"]
    elif problem == "missing-origin":
        del headers["Origin"]
    else:
        headers["Origin"] = ORIGIN + ".evil.example"
    for path in ("/runs", f"/runs/{run_id}/cancel"):
        response = await browser.post(path, json=RUN_BODY if path == "/runs" else None, headers=headers)
        assert response.status_code == 403
    assert await runs.get_run(app.state.pool, run_id) == before
    assert runner.calls == [] and sync_redis.xlen("pending_runs") == 0


async def test_history_owner_isolation(browser: httpx.AsyncClient) -> None:
    owner = await register(browser)
    browser.cookies.clear()
    outsider = await register(browser, email="other@example.com")
    ids = sorted(mint_run_id() for _ in range(42))
    for index, run_id in enumerate(ids):
        user = owner if index % 2 == 0 else outsider
        await seed_run(UUID(user.json()["user"]["id"]), run_id)
    async with app.state.pool.connection() as conn:
        await conn.execute("UPDATE runs SET created_at = %s", (datetime.now(UTC),))
    own_ids = ids[::2]
    filters = {"status": "queued", "preset": RUN_BODY["preset"]}
    first = (await browser.get("/runs", headers=cookie_headers(owner), params=filters)).json()
    second = (await browser.get("/runs", headers=cookie_headers(owner), params={**filters, "cursor": first["next_cursor"]})).json()
    assert [run["run_id"] for run in first["runs"] + second["runs"]] == own_ids[::-1]
    assert first["next_cursor"] and second["next_cursor"] is None
    foreign_cursor = (await browser.get("/runs", headers=cookie_headers(outsider))).json()["next_cursor"]
    page = (await browser.get("/runs", headers=cookie_headers(owner), params={**filters, "cursor": foreign_cursor})).json()
    assert [run["run_id"] for run in page["runs"]] == own_ids[:2][::-1]
