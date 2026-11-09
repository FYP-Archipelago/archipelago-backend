import asyncio
import gzip
import json
from typing import Any
from uuid import UUID

import httpx
import pytest
import redis

from app.db import runs
from app.ids import mint_run_id
from app.main import app
from app.models.runs import CreateRun
from app.routes import events as events_route
from tests.conftest import LOGIN, ORIGIN, FakeRunner, cookie_headers, csrf_headers, parse_sse, register

pytestmark = pytest.mark.anyio

BODY = {"preset": "de-rastrigin", "overrides": {}, "stream": True, "clustering": {"lsh": True}}


async def seed_run(owner: httpx.Response) -> str:
    run = await runs.insert_run(app.state.pool, mint_run_id(), UUID(owner.json()["user"]["id"]), CreateRun.model_validate(BODY))
    return run.run_id


@pytest.mark.parametrize(
    ("method", "path"),
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
async def test_run_routes_need_a_session(browser: httpx.AsyncClient, runner: FakeRunner, method: str, path: str) -> None:
    response = await browser.request(method, path.format(id=mint_run_id()), json=BODY if path == "/runs" else None, headers={"Origin": ORIGIN})

    assert response.status_code == 401 and response.json() == {"detail": "Authentication required"}
    assert runner.calls == []


@pytest.mark.parametrize("problem", ["no csrf token", "another session's csrf token", "no origin", "foreign origin"])
async def test_run_changes_need_our_origin_and_csrf_token(
    browser: httpx.AsyncClient, runner: FakeRunner, sync_redis: redis.Redis, problem: str
) -> None:
    owner = await register(browser)
    run_id = await seed_run(owner)
    headers = csrf_headers(owner)
    if problem == "no csrf token":
        del headers["X-CSRF-Token"]
    elif problem == "another session's csrf token":
        other = await browser.post("/auth/login", json=LOGIN, headers={"Origin": ORIGIN, "Cookie": ""})
        headers["X-CSRF-Token"] = other.json()["csrf_token"]
    elif problem == "no origin":
        del headers["Origin"]
    else:
        headers["Origin"] = "https://evil.example"

    created = await browser.post("/runs", json=BODY, headers=headers)
    cancelled = await browser.post(f"/runs/{run_id}/cancel", headers=headers)

    assert created.status_code == cancelled.status_code == 403
    assert runner.calls == [] and sync_redis.xlen("pending_runs") == 0
    run = await runs.get_run(app.state.pool, run_id)
    assert run is not None and run.status == "queued"


async def test_a_new_run_belongs_to_the_session_that_created_it(browser: httpx.AsyncClient) -> None:
    owner = await register(browser)

    created = await browser.post("/runs", json=BODY, headers=csrf_headers(owner))

    assert created.status_code == 202
    listed = await browser.get("/runs", headers=cookie_headers(owner))
    assert [run["run_id"] for run in listed.json()["runs"]] == [created.json()["run_id"]]


async def test_other_users_runs_are_not_found(browser: httpx.AsyncClient, runner: FakeRunner) -> None:
    owner = await register(browser)
    outsider = await register(browser, email="other@example.com")
    run_id = await seed_run(owner)
    await runs.save_result(app.state.pool, run_id, gzip.compress(b'{"graph": {}}'))

    assert (await browser.get("/runs", headers=cookie_headers(outsider))).json() == {"runs": [], "next_cursor": None}
    for method, path in (("GET", ""), ("GET", "/result"), ("GET", "/events"), ("POST", "/cancel")):
        response = await browser.request(method, f"/runs/{run_id}{path}", headers=csrf_headers(outsider))
        assert response.status_code == 404 and response.json() == {"detail": "Run not found"}
    run = await runs.get_run(app.state.pool, run_id)
    assert run is not None and run.status == "done" and runner.calls == []
    assert (await browser.get(f"/runs/{run_id}/result", headers=cookie_headers(owner))).json() == {"graph": {}}


# Opens a run's live stream and returns once it has sent `entry` and waits for more, checking the session on every loop.
async def stream_past(
    browser: httpx.AsyncClient, monkeypatch: pytest.MonkeyPatch, owner: httpx.Response, run_id: str, entry: str
) -> asyncio.Task[httpx.Response]:
    monkeypatch.setattr(events_route, "SESSION_CHECK_SECONDS", 0)
    waiting = asyncio.Event()
    xread = app.state.redis.xread

    async def observe_reads(streams: dict[str, str], *args: Any, **kwargs: Any) -> Any:
        if streams.get(f"run:{run_id}:clustered") == entry:
            waiting.set()
        return await xread(streams, *args, **kwargs)

    monkeypatch.setattr(app.state.redis, "xread", observe_reads)
    response = asyncio.create_task(browser.get(f"/runs/{run_id}/events", headers=cookie_headers(owner)))
    await asyncio.wait_for(waiting.wait(), 3)
    return response


async def test_a_live_stream_ends_when_its_session_logs_out(
    browser: httpx.AsyncClient, sync_redis: redis.Redis, monkeypatch: pytest.MonkeyPatch
) -> None:
    owner = await register(browser)
    run_id = await seed_run(owner)
    entry = sync_redis.xadd(f"run:{run_id}:clustered", {"update": json.dumps({"n": 1})}).decode()
    response = await stream_past(browser, monkeypatch, owner, run_id, entry)

    assert (await browser.post("/auth/logout", headers=cookie_headers(owner))).status_code == 204

    events = parse_sse((await asyncio.wait_for(response, 5)).text)
    assert events == [
        ("status", None, {"status": "queued"}),
        ("update", entry, {"n": 1}),
        ("auth_required", None, {"detail": "Authentication required"}),
    ]


async def test_a_live_stream_survives_a_refresh(browser: httpx.AsyncClient, sync_redis: redis.Redis, monkeypatch: pytest.MonkeyPatch) -> None:
    owner = await register(browser)
    run_id = await seed_run(owner)
    key = f"run:{run_id}:clustered"
    entry = sync_redis.xadd(key, {"update": json.dumps({"n": 1})}).decode()
    response = await stream_past(browser, monkeypatch, owner, run_id, entry)

    assert (await browser.post("/auth/refresh", headers=cookie_headers(owner))).status_code == 200
    final = sync_redis.xadd(key, {"final": json.dumps({"n": 2})}).decode()

    events = parse_sse((await asyncio.wait_for(response, 5)).text)
    assert events == [("status", None, {"status": "queued"}), ("update", entry, {"n": 1}), ("final", final, {"n": 2})]
