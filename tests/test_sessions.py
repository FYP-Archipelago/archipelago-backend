import asyncio
import hashlib
import os
from collections.abc import AsyncGenerator, Callable
from datetime import datetime, timedelta, tzinfo
from http.cookies import SimpleCookie
from typing import Any
from uuid import UUID

import httpx
import psycopg
import pytest
from psycopg.rows import dict_row

from app import main, security
from app.db import auth
from app.main import app
from app.models.auth import Session, User
from app.routes import auth as auth_routes
from app.settings import ACCESS_COOKIE_NAME, REFRESH_COOKIE_NAME
from tests.helpers import LOGIN, ORIGIN, cookie_headers, read_session, register, session_for, wait_blocked

pytestmark = pytest.mark.anyio


ACCESS_HASH = hashlib.sha256(b"test-access-token").digest()
REFRESH_HASH = hashlib.sha256(b"test-refresh-token").digest()


@pytest.fixture
async def conn() -> AsyncGenerator[psycopg.AsyncConnection[dict[str, Any]]]:
    async with await psycopg.AsyncConnection.connect(
        os.environ["ARCHIPELAGO_TEST_DATABASE_URL"], row_factory=dict_row, autocommit=True
    ) as connection:
        yield connection


@pytest.fixture
async def user(conn: psycopg.AsyncConnection[dict[str, Any]]) -> User:
    return await auth.insert_user(conn, "researcher@example.com", "test-password-hash", "Researcher", "Team 21")


@pytest.fixture
async def session(conn: psycopg.AsyncConnection[dict[str, Any]], user: User) -> Session:
    return await auth.insert_session(
        conn, user.id, ACCESS_HASH, REFRESH_HASH, "test-csrf-token", user.created_at + timedelta(hours=4), user.created_at + timedelta(days=30)
    )


@pytest.mark.parametrize("credential", ["access", "refresh"])
async def test_session_expiry_boundary(conn: psycopg.AsyncConnection[dict[str, Any]], session: Session, credential: str) -> None:
    lookup = auth.get_session_by_access_hash if credential == "access" else auth.get_session_by_refresh_hash
    token_hash = ACCESS_HASH if credential == "access" else REFRESH_HASH
    expires_at = session.access_expires_at if credential == "access" else session.refresh_expires_at

    assert await lookup(conn, token_hash, expires_at - timedelta(microseconds=1)) == session
    assert await lookup(conn, token_hash, expires_at) is None


async def test_cleanup_refresh_deadline(conn: psycopg.AsyncConnection[dict[str, Any]], user: User, session: Session) -> None:
    other_refresh = hashlib.sha256(b"other-refresh-token").digest()
    other = await auth.insert_session(
        conn,
        user.id,
        hashlib.sha256(b"other-access-token").digest(),
        other_refresh,
        "other-csrf-token",
        session.access_expires_at,
        session.refresh_expires_at + timedelta(days=1),
    )

    assert await auth.delete_expired_sessions(conn, session.access_expires_at) == 0
    assert await auth.get_session_by_refresh_hash(conn, REFRESH_HASH, session.access_expires_at) == session
    assert await auth.delete_expired_sessions(conn, session.refresh_expires_at - timedelta(microseconds=1)) == 0
    assert await auth.delete_expired_sessions(conn, session.refresh_expires_at) == 1
    assert await auth.get_session_by_access_hash(conn, ACCESS_HASH, session.created_at) is None
    assert await auth.get_session_by_refresh_hash(conn, REFRESH_HASH, session.created_at) is None
    assert await auth.get_session_by_refresh_hash(conn, other_refresh, session.refresh_expires_at) == other
    assert await auth.delete_expired_sessions(conn, other.refresh_expires_at) == 1
    assert await auth.delete_expired_sessions(conn, other.refresh_expires_at) == 0
    assert await auth.get_user(conn, user.id) == user


async def test_refresh_rotation_and_replay(browser: httpx.AsyncClient) -> None:
    original = await register(browser)
    session = await session_for(original)
    response = await browser.post("/auth/refresh", headers={"Origin": ORIGIN})
    assert response.status_code == 200 and response.headers["cache-control"] == "no-store"
    assert response.json()["user"] == original.json()["user"]
    for name in (ACCESS_COOKIE_NAME, REFRESH_COOKIE_NAME):
        assert response.cookies[name] != original.cookies[name] and response.cookies[name] not in response.text
    rotated = await session_for(response)
    assert rotated.id == session.id and rotated.created_at == session.created_at
    assert rotated.csrf_token == session.csrf_token and rotated.refresh_expires_at == session.refresh_expires_at
    assert (await browser.get("/auth/me", headers=cookie_headers(original))).status_code == 401
    replay = await browser.post("/auth/refresh", headers=cookie_headers(original))
    assert replay.status_code == 401 and "set-cookie" not in replay.headers
    assert (await browser.get("/auth/me")).json() == response.json()


async def test_refresh_absolute_deadline(browser: httpx.AsyncClient, monkeypatch: pytest.MonkeyPatch) -> None:
    session = await session_for(await register(browser))

    class Clock:
        @staticmethod
        def now(_tz: tzinfo) -> datetime:
            return now

    monkeypatch.setattr(auth_routes, "datetime", Clock)
    for now in (session.created_at + timedelta(hours=5), session.refresh_expires_at - timedelta(minutes=10)):
        response = await browser.post("/auth/refresh", headers={"Origin": ORIGIN})
        assert response.status_code == 200
        access_expiry = min(now + timedelta(hours=4), session.refresh_expires_at)
        assert datetime.fromisoformat(response.json()["access_expires_at"]) == access_expiry
        assert datetime.fromisoformat(response.json()["refresh_expires_at"]) == session.refresh_expires_at
        cookies = SimpleCookie()
        for header in response.headers.get_list("set-cookie"):
            cookies.load(header)
        assert int(cookies[ACCESS_COOKIE_NAME]["max-age"]) == int((access_expiry - now).total_seconds())
        assert int(cookies[REFRESH_COOKIE_NAME]["max-age"]) == int((session.refresh_expires_at - now).total_seconds())
    before = await read_session(session.id)
    now = session.refresh_expires_at
    expired = await browser.post("/auth/refresh", headers={"Origin": ORIGIN})
    assert expired.status_code == 401 and "set-cookie" not in expired.headers
    assert await read_session(session.id) == before


@pytest.mark.parametrize("credential", ["missing", "access-as-refresh", "unknown"])
async def test_refresh_rejects_invalid_tokens(browser: httpx.AsyncClient, credential: str) -> None:
    original = await register(browser)
    session = await session_for(original)
    token = original.cookies[ACCESS_COOKIE_NAME] if credential == "access-as-refresh" else security.mint_token()
    cookie = "" if credential == "missing" else f"{REFRESH_COOKIE_NAME}={token}"
    response = await browser.post("/auth/refresh", headers={"Origin": ORIGIN, "Cookie": cookie})
    assert response.status_code == 401 and "set-cookie" not in response.headers
    assert await read_session(session.id) == session
    assert (await browser.get("/auth/me")).status_code == 200


async def test_concurrent_refresh_rotation(browser: httpx.AsyncClient, monkeypatch: pytest.MonkeyPatch) -> None:
    original = await register(browser)
    session = await session_for(original)
    lock = auth.lock_session_by_refresh_hash
    held, waiting, release = asyncio.Event(), asyncio.Event(), asyncio.Event()
    calls = 0
    second_pid = None

    async def controlled_lock(conn: psycopg.AsyncConnection[dict[str, Any]], token_hash: bytes) -> Session | None:
        nonlocal calls, second_pid
        calls += 1
        index = calls
        if index == 2:
            second_pid = conn.info.backend_pid
            waiting.set()
        result = await lock(conn, token_hash)
        if index == 1:
            held.set()
            await release.wait()
        return result

    monkeypatch.setattr(auth, "lock_session_by_refresh_hash", controlled_lock)
    requests = [asyncio.create_task(browser.post("/auth/refresh", headers=cookie_headers(original)))]
    try:
        await asyncio.wait_for(held.wait(), 5)
        requests.append(asyncio.create_task(browser.post("/auth/refresh", headers=cookie_headers(original))))
        await asyncio.wait_for(waiting.wait(), 5)
        assert second_pid is not None
        await wait_blocked(second_pid)
    finally:
        release.set()
        responses = await asyncio.wait_for(asyncio.gather(*requests), 5)
    assert sorted(response.status_code for response in responses) == [200, 401]
    winner = next(response for response in responses if response.status_code == 200)
    loser = next(response for response in responses if response.status_code == 401)
    assert "set-cookie" not in loser.headers
    rotated = await session_for(winner)
    assert rotated.id == session.id
    assert (await browser.get("/auth/me")).json() == winner.json()


async def test_refresh_expiry_after_lock_wait(browser: httpx.AsyncClient, monkeypatch: pytest.MonkeyPatch) -> None:
    session = await session_for(await register(browser))
    now = session.refresh_expires_at - timedelta(seconds=1)
    lock = auth.lock_session_by_refresh_hash
    waiting = asyncio.Event()
    pid = None

    class Clock:
        @staticmethod
        def now(_tz: tzinfo) -> datetime:
            return now

    async def observed_lock(conn: psycopg.AsyncConnection[dict[str, Any]], token_hash: bytes) -> Session | None:
        nonlocal pid
        pid = conn.info.backend_pid
        waiting.set()
        return await lock(conn, token_hash)

    monkeypatch.setattr(auth_routes, "datetime", Clock)
    monkeypatch.setattr(auth, "lock_session_by_refresh_hash", observed_lock)
    request = None
    try:
        async with app.state.pool.connection() as conn, conn.transaction():
            await conn.execute("SELECT id FROM auth_sessions WHERE id = %s FOR UPDATE", (session.id,))
            request = asyncio.create_task(browser.post("/auth/refresh", headers={"Origin": ORIGIN}))
            await asyncio.wait_for(waiting.wait(), 5)
            assert pid is not None
            await wait_blocked(pid)
            now = session.refresh_expires_at
    finally:
        if request is not None:
            response = await asyncio.wait_for(request, 5)
    assert response.status_code == 401 and "set-cookie" not in response.headers
    assert await read_session(session.id) == session


def assert_cleared(response: httpx.Response) -> None:
    assert response.status_code == 204 and response.content == b""
    assert response.headers["cache-control"] == "no-store"
    cookies = SimpleCookie()
    for header in response.headers.get_list("set-cookie"):
        cookies.load(header)
    assert set(cookies) == {ACCESS_COOKIE_NAME, REFRESH_COOKIE_NAME}
    for cookie in cookies.values():
        assert cookie.value == "" and cookie["max-age"] == "0" and cookie["expires"]
        assert cookie["httponly"] and cookie["path"] == "/" and cookie["samesite"] == "lax" and not cookie["domain"]


async def test_logout_session_isolation(browser: httpx.AsyncClient) -> None:
    original = await register(browser)
    session = await session_for(original)
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://api") as other:
        assert (await other.post("/auth/login", json=LOGIN, headers={"Origin": ORIGIN})).status_code == 200
        assert_cleared(await browser.post("/auth/logout", headers={"Origin": ORIGIN}))
        assert not browser.cookies and await read_session(session.id) is None
        assert (await browser.get("/auth/me", headers=cookie_headers(original))).status_code == 401
        assert (await browser.post("/auth/refresh", headers=cookie_headers(original))).status_code == 401
        assert_cleared(await browser.post("/auth/logout", headers=cookie_headers(original)))
        assert_cleared(await browser.post("/auth/logout", headers={"Origin": ORIGIN}))
        assert (await other.get("/auth/me")).status_code == 200
        assert (await other.post("/auth/refresh", headers={"Origin": ORIGIN})).status_code == 200


@pytest.mark.parametrize("credential", ["expired-access", "refresh-only", "access-only", "invalid-refresh"])
async def test_logout_cookie_fallback(browser: httpx.AsyncClient, credential: str) -> None:
    original = await register(browser)
    session = await session_for(original)
    if credential == "expired-access":
        async with app.state.pool.connection() as conn:
            await conn.execute(
                "UPDATE auth_sessions SET created_at = now() - interval '1 day', access_expires_at = now() - interval '1 second' WHERE id = %s",
                (session.id,),
            )
    cookie = {
        "expired-access": cookie_headers(original)["Cookie"],
        "refresh-only": f"{REFRESH_COOKIE_NAME}={original.cookies[REFRESH_COOKIE_NAME]}",
        "access-only": f"{ACCESS_COOKIE_NAME}={original.cookies[ACCESS_COOKIE_NAME]}",
        "invalid-refresh": f"{REFRESH_COOKIE_NAME}={security.mint_token()}; {ACCESS_COOKIE_NAME}={original.cookies[ACCESS_COOKIE_NAME]}",
    }[credential]
    assert_cleared(await browser.post("/auth/logout", headers={"Origin": ORIGIN, "Cookie": cookie}))
    assert await read_session(session.id) is None


@pytest.mark.parametrize("first", ["refresh", "logout"])
async def test_refresh_logout_race(browser: httpx.AsyncClient, monkeypatch: pytest.MonkeyPatch, first: str) -> None:
    original = await register(browser)
    session = await session_for(original)
    headers = cookie_headers(original)
    lock, delete = auth.lock_session_by_refresh_hash, auth.delete_session
    held, waiting, release = asyncio.Event(), asyncio.Event(), asyncio.Event()
    second_pid = None

    async def controlled_lock(conn: psycopg.AsyncConnection[dict[str, Any]], token_hash: bytes) -> Session | None:
        nonlocal second_pid
        if first == "logout":
            second_pid = conn.info.backend_pid
            waiting.set()
        result = await lock(conn, token_hash)
        if first == "refresh":
            held.set()
            await release.wait()
        return result

    async def controlled_delete(conn: psycopg.AsyncConnection[dict[str, Any]], session_id: UUID) -> bool:
        nonlocal second_pid
        if first == "refresh":
            second_pid = conn.info.backend_pid
            waiting.set()
        result = await delete(conn, session_id)
        if first == "logout":
            held.set()
            await release.wait()
        return result

    monkeypatch.setattr(auth, "lock_session_by_refresh_hash", controlled_lock)
    monkeypatch.setattr(auth, "delete_session", controlled_delete)
    pending = {first: asyncio.create_task(browser.post(f"/auth/{first}", headers=headers))}
    second = "logout" if first == "refresh" else "refresh"
    try:
        await asyncio.wait_for(held.wait(), 5)
        pending[second] = asyncio.create_task(browser.post(f"/auth/{second}", headers=headers))
        await asyncio.wait_for(waiting.wait(), 5)
        assert second_pid is not None
        await wait_blocked(second_pid)
    finally:
        release.set()
        responses = dict(zip(pending, await asyncio.wait_for(asyncio.gather(*pending.values()), 5), strict=True))
    assert_cleared(responses["logout"])
    assert responses["refresh"].status_code == (200 if first == "refresh" else 401)
    assert await read_session(session.id) is None
    if first == "refresh":
        headers = cookie_headers(responses["refresh"])
    else:
        assert "set-cookie" not in responses["refresh"].headers
    assert (await browser.get("/auth/me", headers=headers)).status_code == 401
    assert (await browser.post("/auth/refresh", headers=headers)).status_code == 401


async def test_cleanup_retry_and_shutdown(running_app: Callable[[], Any], monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture) -> None:
    calls = 0
    recovered = asyncio.Event()
    delete = auth.delete_expired_sessions
    monkeypatch.setattr(main, "SESSION_CLEANUP_SECONDS", 0.01)

    async def flaky_delete(conn: psycopg.AsyncConnection[dict[str, Any]], now: datetime) -> int:
        nonlocal calls
        calls += 1
        if calls == 1:
            raise psycopg.OperationalError("temporary failure")
        deleted = await delete(conn, now)
        recovered.set()
        return deleted

    monkeypatch.setattr(auth, "delete_expired_sessions", flaky_delete)
    async with running_app():
        await asyncio.wait_for(recovered.wait(), 5)
        assert calls >= 2 and "Expired session cleanup failed" in caplog.text
        cleanup = next(task for task in asyncio.all_tasks() if task.get_name() == "session-cleanup")
        pool = main.app.state.pool
        assert not cleanup.done() and not pool.closed
    assert cleanup.cancelled() and pool.closed
