import asyncio
import json
from typing import Any
from uuid import UUID

import httpx
import psycopg
import pytest
import redis

from app.db import auth, runs
from app.main import app
from app.models.runs import RunStatus
from tests.conftest import FakeRunner
from tests.helpers import LOGIN, ORIGIN, RUN_BODY, RunStream, cookie_headers, csrf_headers, register, seed_run, session_for

pytestmark = pytest.mark.anyio
AUTH_REQUIRED = ("auth_required", None, {"detail": "Authentication required"})


@pytest.mark.parametrize("action,next_kind", [("logout", "update"), ("expire", "final")])
async def test_replay_auth_revocation(browser: httpx.AsyncClient, sync_redis: redis.Redis, action: str, next_kind: str) -> None:
    original = await register(browser)
    session = await session_for(original)
    run_id = await seed_run(session.user_id)
    key = f"run:{run_id}:clustered"
    sync_redis.xadd(key, {"update": json.dumps({"n": 1})})
    sync_redis.xadd(key, {next_kind: json.dumps({"n": 2})})
    async with RunStream(run_id, cookie_headers(original)) as stream:
        assert (await stream.next())[0] == "status"
        assert (await stream.next())[0] == "update"
        if action == "logout":
            assert (await browser.post("/auth/logout", headers={"Origin": ORIGIN})).status_code == 204
        else:
            async with app.state.pool.connection() as conn:
                await conn.execute(
                    "UPDATE auth_sessions SET created_at = now() - interval '1 day', access_expires_at = now() - interval '1 second' WHERE id = %s",
                    (session.id,),
                )
        assert await stream.next() == AUTH_REQUIRED
        assert await stream.next() is None


async def test_idle_stream_logout(browser: httpx.AsyncClient, monkeypatch: pytest.MonkeyPatch) -> None:
    original = await register(browser)
    run_id = await seed_run(UUID(original.json()["user"]["id"]))
    reading = asyncio.Event()
    xread = app.state.redis.xread

    async def observe_read(*args: Any, **kwargs: Any) -> Any:
        reading.set()
        return await xread(*args, **kwargs)

    monkeypatch.setattr(app.state.redis, "xread", observe_read)
    async with RunStream(run_id, cookie_headers(original)) as stream:
        assert (await stream.next())[0] == "status"
        pending = asyncio.create_task(stream.next())
        try:
            await asyncio.wait_for(reading.wait(), 5)
            assert not pending.done()
            assert (await browser.post("/auth/logout", headers={"Origin": ORIGIN})).status_code == 204
            assert await pending == AUTH_REQUIRED
            assert await stream.next() is None
        finally:
            pending.cancel()
            await asyncio.gather(pending, return_exceptions=True)


async def test_stream_database_failure(browser: httpx.AsyncClient, sync_redis: redis.Redis, monkeypatch: pytest.MonkeyPatch) -> None:
    original = await register(browser)
    run_id = await seed_run(UUID(original.json()["user"]["id"]))
    sync_redis.xadd(f"run:{run_id}:clustered", {"update": json.dumps({"private": True})})

    async def fail(*args: Any) -> None:
        raise psycopg.OperationalError("private database diagnostic")

    async with RunStream(run_id, cookie_headers(original)) as stream:
        assert (await stream.next())[0] == "status"
        monkeypatch.setattr(auth, "get_session_by_access_hash", fail)
        assert await stream.next() == AUTH_REQUIRED
        assert await stream.next() is None


async def test_stream_start_revocation(browser: httpx.AsyncClient, monkeypatch: pytest.MonkeyPatch) -> None:
    original = await register(browser)
    session = await session_for(original)
    run_id = await seed_run(session.user_id)
    lookup = runs.get_owned_run

    async def revoke_after_lookup(*args: Any) -> RunStatus | None:
        run = await lookup(*args)
        async with app.state.pool.connection() as conn:
            await auth.delete_session(conn, session.id)
        return run

    monkeypatch.setattr(runs, "get_owned_run", revoke_after_lookup)
    async with RunStream(run_id, cookie_headers(original)) as stream:
        assert await stream.next() == AUTH_REQUIRED
        assert await stream.next() is None


async def test_refresh_stream_reconnection(browser: httpx.AsyncClient, sync_redis: redis.Redis) -> None:
    original = await register(browser)
    other = await browser.post("/auth/login", json=LOGIN, headers={"Origin": ORIGIN, "Cookie": ""})
    assert other.status_code == 200
    run_id = await seed_run(UUID(original.json()["user"]["id"]))
    key = f"run:{run_id}:clustered"
    first = sync_redis.xadd(key, {"update": json.dumps({"n": 1})}).decode()
    second = sync_redis.xadd(key, {"update": json.dumps({"n": 2})}).decode()
    final = sync_redis.xadd(key, {"final": json.dumps({"n": 3})}).decode()
    async with RunStream(run_id, cookie_headers(original)) as stream, RunStream(run_id, cookie_headers(other)) as other_stream:
        assert (await stream.next())[0] == (await other_stream.next())[0] == "status"
        assert (await stream.next())[1] == (await other_stream.next())[1] == first
        renewed = await browser.post("/auth/refresh", headers=cookie_headers(original))
        assert renewed.status_code == 200
        assert await stream.next() == AUTH_REQUIRED
        assert await stream.next() is None
        assert await other_stream.next() == ("update", second, {"n": 2})
        assert await other_stream.next() == ("final", final, {"n": 3})
        assert await other_stream.next() is None
    async with RunStream(run_id, {**cookie_headers(renewed), "Last-Event-ID": first}) as resumed:
        assert (await resumed.next())[0] == "status"
        assert await resumed.next() == ("update", second, {"n": 2})
        assert await resumed.next() == ("final", final, {"n": 3})
        assert await resumed.next() is None


async def test_run_survives_logout(browser: httpx.AsyncClient, runner: FakeRunner, sync_redis: redis.Redis) -> None:
    original = await register(browser)
    created = await browser.post("/runs", json=RUN_BODY, headers=csrf_headers(original))
    assert created.status_code == 202
    run_id = created.json()["run_id"]
    async with RunStream(run_id, cookie_headers(original)) as stream:
        assert (await stream.next())[0] == "status"
        assert (await browser.post("/auth/logout", headers={"Origin": ORIGIN})).status_code == 204
        assert await stream.next() == AUTH_REQUIRED
        assert await stream.next() is None
    runner.state = "finished"
    result = {"graph": {"nodes": [7]}}
    sync_redis.xadd(f"run:{run_id}:clustered", {"final": json.dumps(result)})
    for job_id, fields in sync_redis.xrange("pending_runs"):
        if fields[b"run_id"].decode() == run_id:
            sync_redis.xdel("pending_runs", job_id)
    async with asyncio.timeout(10):
        while run_id in await runs.list_unexpired(app.state.pool):
            await asyncio.sleep(0.05)
    assert f"POST /runs/{run_id}/cancel" not in runner.calls
    assert (await browser.get(f"/runs/{run_id}/result")).status_code == 401
    assert (await browser.post("/auth/login", json=LOGIN, headers={"Origin": ORIGIN})).status_code == 200
    assert (await browser.get(f"/runs/{run_id}")).json()["status"] == "done"
    response = await browser.get(f"/runs/{run_id}/result")
    assert response.status_code == 200 and response.json() == result
