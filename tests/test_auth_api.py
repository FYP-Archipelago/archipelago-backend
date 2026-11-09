import asyncio
import hashlib
from dataclasses import replace
from datetime import UTC, datetime
from http.cookies import SimpleCookie
from typing import Any

import httpx
import psycopg
import pytest
from argon2 import PasswordHasher

from app import security
from app.db import auth
from app.main import app
from app.settings import ACCESS_COOKIE_NAME, REFRESH_COOKIE_NAME
from tests.helpers import DIGEST, LOGIN, ORIGIN, REGISTRATION, cookie_headers, read_session, register, session_for

pytestmark = pytest.mark.anyio


async def test_registration_session(browser: httpx.AsyncClient) -> None:
    response = await register(browser)
    value = response.json()
    assert value["user"]["email"] == "researcher@example.com"
    assert value["user"]["full_name"] == "Researcher" and value["user"]["institution"] == "Team 21"
    assert response.headers["cache-control"] == "no-store"
    access, refresh = response.cookies[ACCESS_COOKIE_NAME], response.cookies[REFRESH_COOKIE_NAME]
    session = await session_for(response)
    async with app.state.pool.connection() as conn:
        user = await auth.get_user(conn, session.user_id)
    assert user is not None and PasswordHasher().verify(user.password_hash, DIGEST)
    assert value["user"]["id"] == str(user.id)
    assert session.access_token_hash == security.hash_token(access)
    assert session.refresh_token_hash == security.hash_token(refresh)
    assert session.csrf_token == value["csrf_token"]
    assert len({access, refresh, session.csrf_token}) == 3
    assert all(secret not in response.text for secret in (DIGEST, user.password_hash, access, refresh))
    cookies = SimpleCookie()
    for header in response.headers.get_list("set-cookie"):
        cookies.load(header)
    assert set(cookies) == {ACCESS_COOKIE_NAME, REFRESH_COOKIE_NAME}
    for name, lifetime in ((ACCESS_COOKIE_NAME, 4 * 3600), (REFRESH_COOKIE_NAME, 30 * 24 * 3600)):
        cookie = cookies[name]
        assert cookie["httponly"] and cookie["samesite"] == "lax" and cookie["path"] == "/"
        assert not cookie["domain"] and not cookie["secure"]
        assert lifetime - 5 <= int(cookie["max-age"]) <= lifetime
    me = await browser.get("/auth/me")
    assert me.status_code == 200 and me.json() == value
    assert me.headers["cache-control"] == "no-store" and "set-cookie" not in me.headers


async def test_secure_cookie_lifecycle(browser: httpx.AsyncClient, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(app.state, "settings", replace(app.state.settings, cookie_secure=True))
    browser.base_url = "https://api"
    response = await register(browser)
    assert (await browser.get("/auth/me")).status_code == 200
    logged_out = await browser.post("/auth/logout", headers={"Origin": ORIGIN})
    assert logged_out.status_code == 204 and not browser.cookies
    for result in (response, logged_out):
        cookies = SimpleCookie()
        for header in result.headers.get_list("set-cookie"):
            cookies.load(header)
        assert set(cookies) == {ACCESS_COOKIE_NAME, REFRESH_COOKIE_NAME}
        for cookie in cookies.values():
            assert cookie["secure"] and cookie["httponly"] and cookie["samesite"] == "lax"
            assert cookie["path"] == "/" and not cookie["domain"]


async def test_login_session_replacement(browser: httpx.AsyncClient) -> None:
    original = await register(browser)
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://api") as other:
        assert (await other.post("/auth/login", json=LOGIN, headers={"Origin": ORIGIN})).status_code == 200
        wrong_digest = hashlib.sha256(b"archipelago:password:v1:wrong password").hexdigest()
        for credentials in ({**LOGIN, "password_digest": wrong_digest}, {**LOGIN, "email": "missing@example.com"}):
            rejected = await browser.post("/auth/login", json=credentials, headers={"Origin": ORIGIN})
            assert rejected.status_code == 401 and rejected.json() == {"detail": "Invalid email or password"}
            assert "set-cookie" not in rejected.headers and rejected.headers["cache-control"] == "no-store"
            assert (await browser.get("/auth/me")).json() == original.json()
        logged_in = await browser.post("/auth/login", json=LOGIN, headers={"Origin": ORIGIN})
        assert logged_in.status_code == 200 and logged_in.json()["user"] == original.json()["user"]
        assert all(logged_in.cookies[name] != original.cookies[name] for name in (ACCESS_COOKIE_NAME, REFRESH_COOKIE_NAME))
        assert (await browser.get("/auth/me")).json() == logged_in.json()
        assert (await browser.get("/auth/me", headers=cookie_headers(original))).status_code == 401
        assert (await browser.post("/auth/refresh", headers=cookie_headers(original))).status_code == 401
        assert (await other.get("/auth/me")).status_code == 200


async def test_duplicate_registration_race(browser: httpx.AsyncClient) -> None:
    responses = await asyncio.gather(
        browser.post("/auth/register", json=REGISTRATION, headers={"Origin": ORIGIN, "Cookie": ""}),
        browser.post("/auth/register", json={**REGISTRATION, "email": "researcher@example.com"}, headers={"Origin": ORIGIN, "Cookie": ""}),
    )
    assert sorted(response.status_code for response in responses) == [201, 409]
    rejected = next(response for response in responses if response.status_code == 409)
    assert rejected.json() == {"detail": "Email already registered"} and "set-cookie" not in rejected.headers
    async with app.state.pool.connection() as conn:
        counts = await (
            await conn.execute("SELECT (SELECT count(*) FROM users) AS users, (SELECT count(*) FROM auth_sessions) AS sessions")
        ).fetchone()
    assert counts == {"users": 1, "sessions": 1}


@pytest.mark.parametrize("endpoint", ["register", "login", "refresh", "logout"])
@pytest.mark.parametrize("origin", [None, ORIGIN + ".evil.example"])
async def test_auth_origin_rejection(browser: httpx.AsyncClient, endpoint: str, origin: str | None) -> None:
    original = await register(browser)
    session = await session_for(original)
    headers = {} if origin is None else {"Origin": origin}
    body = {**REGISTRATION, "email": "new@example.com"} if endpoint == "register" else LOGIN if endpoint == "login" else {}
    response = await browser.post(f"/auth/{endpoint}", json=body, headers=headers)
    assert response.status_code == 403 and "set-cookie" not in response.headers
    assert response.headers["cache-control"] == "no-store" and await read_session(session.id) == session
    async with app.state.pool.connection() as conn:
        assert await auth.get_user_by_email(conn, "new@example.com") is None


@pytest.mark.parametrize("endpoint,values", [("register", {"password_digest": "invalid-secret"}), ("login", {"password": "secret"})])
async def test_auth_validation_redaction(browser: httpx.AsyncClient, endpoint: str, values: dict[str, str]) -> None:
    body = REGISTRATION if endpoint == "register" else LOGIN
    response = await browser.post(f"/auth/{endpoint}", json={**body, **values}, headers={"Origin": ORIGIN})
    assert response.status_code == 422 and response.json() == {"detail": "Invalid authentication request"}
    assert "set-cookie" not in response.headers and response.headers["cache-control"] == "no-store"
    assert DIGEST not in response.text and "secret" not in response.text


async def test_access_expiry_and_refresh(browser: httpx.AsyncClient) -> None:
    assert (await browser.get("/auth/me")).status_code == 401
    original = await register(browser)
    session = await session_for(original)
    for cookie in (
        f"{REFRESH_COOKIE_NAME}={original.cookies[REFRESH_COOKIE_NAME]}",
        f"{ACCESS_COOKIE_NAME}={original.cookies[REFRESH_COOKIE_NAME]}",
        f"{ACCESS_COOKIE_NAME}={security.mint_token()}",
    ):
        assert (await browser.get("/auth/me", headers={"Cookie": cookie})).status_code == 401
    async with app.state.pool.connection() as conn:
        await conn.execute(
            "UPDATE auth_sessions SET created_at = now() - interval '1 day', access_expires_at = now() - interval '1 second' WHERE id = %s",
            (session.id,),
        )
    assert (await browser.get("/auth/me")).status_code == 401
    browser.cookies.delete(ACCESS_COOKIE_NAME)
    refreshed = await browser.post("/auth/refresh", headers={"Origin": ORIGIN})
    assert refreshed.status_code == 200 and (await browser.get("/auth/me")).status_code == 200
    assert datetime.fromisoformat(refreshed.json()["access_expires_at"]) > datetime.now(UTC)


async def test_credentialed_cors(browser: httpx.AsyncClient) -> None:
    headers = {"Origin": ORIGIN, "Access-Control-Request-Method": "POST", "Access-Control-Request-Headers": "content-type,x-csrf-token"}
    preflight = await browser.options("/auth/login", headers=headers)
    assert preflight.status_code == 200 and preflight.headers["access-control-allow-origin"] == ORIGIN
    assert preflight.headers["access-control-allow-credentials"] == "true"
    untrusted = await browser.options("/auth/login", headers={**headers, "Origin": "https://evil.example"})
    assert untrusted.status_code == 400 and "access-control-allow-origin" not in untrusted.headers
    error = await browser.post("/auth/login", json={}, headers={"Origin": ORIGIN})
    assert error.status_code == 422 and error.headers["access-control-allow-origin"] == ORIGIN


@pytest.mark.parametrize(
    "endpoint,operation", [("register", "insert_session"), ("login", "insert_session"), ("refresh", "rotate_session"), ("logout", "delete_session")]
)
async def test_auth_mutation_rollback(browser: httpx.AsyncClient, monkeypatch: pytest.MonkeyPatch, endpoint: str, operation: str) -> None:
    original = await register(browser)
    session = await session_for(original)
    mutate = getattr(auth, operation)

    async def fail_after_mutation(*args: Any, **kwargs: Any) -> None:
        await mutate(*args, **kwargs)
        raise psycopg.OperationalError("private database diagnostic")

    body = {**REGISTRATION, "email": "new@example.com"} if endpoint == "register" else LOGIN if endpoint == "login" else {}
    with monkeypatch.context() as patched:
        patched.setattr(auth, operation, fail_after_mutation)
        response = await browser.post(f"/auth/{endpoint}", json=body, headers={"Origin": ORIGIN})
    assert response.status_code == 503 and response.json() == {"detail": "Database unavailable"}
    assert "set-cookie" not in response.headers and response.headers["cache-control"] == "no-store"
    assert await read_session(session.id) == session
    assert (await browser.get("/auth/me")).json() == original.json()
    async with app.state.pool.connection() as conn:
        assert await auth.get_user_by_email(conn, "new@example.com") is None
        assert (await (await conn.execute("SELECT count(*) AS count FROM auth_sessions")).fetchone())["count"] == 1
