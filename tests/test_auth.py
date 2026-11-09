import hashlib
from datetime import datetime
from http.cookies import SimpleCookie

import httpx
import pytest

from app.auth import ACCESS_COOKIE, REFRESH_COOKIE, hash_token, passwords
from app.db import auth as auth_db
from app.main import app
from app.settings import load_settings
from tests.conftest import DIGEST, LOGIN, ORIGIN, REGISTRATION, cookie_headers, register

pytestmark = pytest.mark.anyio


async def expire(*columns: str) -> None:
    assignments = ", ".join(f"{column} = now() - interval '1 second'" for column in columns)
    async with app.state.pool.connection() as conn:
        await conn.execute(f"UPDATE auth_sessions SET {assignments} FROM users WHERE users.id = user_id AND users.email = 'researcher@example.com'")


async def test_register_starts_a_session(browser: httpx.AsyncClient) -> None:
    response = await register(browser)

    body = response.json()
    assert body["user"]["email"] == "researcher@example.com"
    assert (body["user"]["full_name"], body["user"]["institution"]) == ("Researcher", "Team 21")
    assert response.headers["cache-control"] == "no-store"
    cookies = SimpleCookie()
    for header in response.headers.get_list("set-cookie"):
        cookies.load(header)
    for name, lifetime, path in ((ACCESS_COOKIE, 4 * 3600, "/"), (REFRESH_COOKIE, 30 * 24 * 3600, "/auth")):
        assert cookies[name]["httponly"] and cookies[name]["samesite"] == "lax" and cookies[name]["path"] == path
        assert lifetime - 5 <= int(cookies[name]["max-age"]) <= lifetime
    me = await browser.get("/auth/me")
    assert me.status_code == 200 and me.json() == body


async def test_passwords_and_tokens_are_stored_only_as_hashes(browser: httpx.AsyncClient) -> None:
    response = await register(browser)

    async with app.state.pool.connection() as conn:
        user = await (await conn.execute("SELECT password_hash FROM users")).fetchone()
        session = await (await conn.execute("SELECT access_token_hash, refresh_token_hash FROM auth_sessions")).fetchone()
    assert user["password_hash"] != DIGEST and passwords.verify(DIGEST, user["password_hash"])
    assert bytes(session["access_token_hash"]) == hash_token(response.cookies[ACCESS_COOKIE])
    assert bytes(session["refresh_token_hash"]) == hash_token(response.cookies[REFRESH_COOKIE])


@pytest.mark.parametrize("values", [{"email": "not-an-email"}, {"password_digest": "short"}, {"full_name": "  "}])
async def test_invalid_registration_is_422(browser: httpx.AsyncClient, values: dict[str, str]) -> None:
    response = await browser.post("/auth/register", json={**REGISTRATION, **values}, headers={"Origin": ORIGIN})

    assert response.status_code == 422 and "set-cookie" not in response.headers


async def test_an_email_registers_once_whatever_its_case(browser: httpx.AsyncClient) -> None:
    await register(browser)

    response = await browser.post("/auth/register", json={**REGISTRATION, "email": "RESEARCHER@example.com"}, headers={"Origin": ORIGIN})

    assert response.status_code == 409 and response.json() == {"detail": "Email already registered"}


async def test_login_checks_the_password_and_replaces_the_browsers_session(browser: httpx.AsyncClient) -> None:
    first = await register(browser)
    wrong = hashlib.sha256(b"another password").hexdigest()

    for credentials in ({**LOGIN, "password_digest": wrong}, {**LOGIN, "email": "nobody@example.com"}):
        rejected = await browser.post("/auth/login", json=credentials, headers={"Origin": ORIGIN})
        assert rejected.status_code == 401 and rejected.json() == {"detail": "Invalid email or password"}
    second = await browser.post("/auth/login", json=LOGIN, headers={"Origin": ORIGIN})

    assert second.status_code == 200 and second.json()["user"] == first.json()["user"]
    assert (await browser.get("/auth/me", headers=cookie_headers(first))).status_code == 401
    assert (await browser.get("/auth/me", headers=cookie_headers(second))).status_code == 200


async def test_logout_ends_only_this_browsers_session(browser: httpx.AsyncClient) -> None:
    first = await register(browser)
    second = await browser.post("/auth/login", json=LOGIN, headers={"Origin": ORIGIN, "Cookie": ""})

    response = await browser.post("/auth/logout", headers=cookie_headers(first))

    assert response.status_code == 204
    cleared = SimpleCookie()
    for header in response.headers.get_list("set-cookie"):
        cleared.load(header)
    # A browser only deletes a cookie whose path matches the one it was set with
    assert {name: (cookie["path"], cookie["max-age"]) for name, cookie in cleared.items()} == {
        ACCESS_COOKIE: ("/", "0"),
        REFRESH_COOKIE: ("/auth", "0"),
    }
    assert (await browser.get("/auth/me", headers=cookie_headers(first))).status_code == 401
    assert (await browser.post("/auth/refresh", headers=cookie_headers(first))).status_code == 401
    assert (await browser.get("/auth/me", headers=cookie_headers(second))).status_code == 200
    assert (await browser.post("/auth/logout", headers={"Origin": ORIGIN, "Cookie": ""})).status_code == 204


@pytest.mark.parametrize("endpoint", ["register", "login", "refresh", "logout"])
@pytest.mark.parametrize("origin", [None, "https://evil.example"])
async def test_auth_posts_need_our_origin(browser: httpx.AsyncClient, endpoint: str, origin: str | None) -> None:
    session = await register(browser)
    body = {**REGISTRATION, "email": "new@example.com"} if endpoint == "register" else LOGIN if endpoint == "login" else None
    headers = {**cookie_headers(session), "Origin": origin} if origin else {"Cookie": cookie_headers(session)["Cookie"]}

    response = await browser.post(f"/auth/{endpoint}", json=body, headers=headers)

    assert response.status_code == 403 and response.json() == {"detail": "Untrusted origin"}
    assert (await browser.get("/auth/me", headers=cookie_headers(session))).status_code == 200


async def test_refresh_rotates_both_tokens_and_keeps_the_deadline(browser: httpx.AsyncClient) -> None:
    first = await register(browser)

    second = await browser.post("/auth/refresh", headers=cookie_headers(first))

    assert second.status_code == 200
    assert all(second.cookies[name] != first.cookies[name] for name in (ACCESS_COOKIE, REFRESH_COOKIE))
    assert second.json()["refresh_expires_at"] == first.json()["refresh_expires_at"]
    assert second.json()["csrf_token"] == first.json()["csrf_token"]
    assert (await browser.post("/auth/refresh", headers=cookie_headers(first))).status_code == 401
    assert (await browser.get("/auth/me", headers=cookie_headers(second))).status_code == 200


async def test_refresh_never_outlives_the_deadline(browser: httpx.AsyncClient) -> None:
    session = await register(browser)
    async with app.state.pool.connection() as conn:
        await conn.execute(
            "UPDATE auth_sessions SET access_expires_at = now() + interval '30 minutes', refresh_expires_at = now() + interval '1 hour'"
        )

    near = await browser.post("/auth/refresh", headers=cookie_headers(session))
    await expire("access_expires_at", "refresh_expires_at")
    past = await browser.post("/auth/refresh", headers=cookie_headers(near))

    assert near.status_code == 200 and near.json()["access_expires_at"] == near.json()["refresh_expires_at"]
    assert past.status_code == 401 and past.json() == {"detail": "Invalid or expired refresh token"}


async def test_an_expired_access_token_needs_a_refresh(browser: httpx.AsyncClient) -> None:
    session = await register(browser)
    await expire("access_expires_at")

    assert (await browser.get("/auth/me", headers=cookie_headers(session))).status_code == 401
    refreshed = await browser.post("/auth/refresh", headers=cookie_headers(session))
    assert refreshed.status_code == 200
    assert datetime.fromisoformat(refreshed.json()["access_expires_at"]) > datetime.fromisoformat(session.json()["access_expires_at"])
    assert (await browser.get("/auth/me", headers=cookie_headers(refreshed))).status_code == 200


async def test_cleanup_deletes_only_expired_sessions(browser: httpx.AsyncClient) -> None:
    await register(browser)
    other = await register(browser, email="other@example.com")
    await expire("access_expires_at", "refresh_expires_at")

    await auth_db.delete_expired_sessions(app.state.pool)

    async with app.state.pool.connection() as conn:
        rows = await (await conn.execute("SELECT email FROM auth_sessions JOIN users ON users.id = user_id")).fetchall()
    assert rows == [{"email": "other@example.com"}]
    assert (await browser.get("/auth/me", headers=cookie_headers(other))).status_code == 200


# The dashboard is on another origin, so without these headers browsers would block every request that sends cookies or the CSRF token.
async def test_cors_lets_only_our_origin_send_credentials(browser: httpx.AsyncClient) -> None:
    preflight = {"Access-Control-Request-Method": "POST", "Access-Control-Request-Headers": "content-type,x-csrf-token"}

    ours = await browser.options("/runs", headers={**preflight, "Origin": ORIGIN})
    theirs = await browser.options("/runs", headers={**preflight, "Origin": "https://evil.example"})

    assert ours.status_code == 200 and ours.headers["access-control-allow-origin"] == ORIGIN
    assert ours.headers["access-control-allow-credentials"] == "true"
    assert "access-control-allow-origin" not in theirs.headers


@pytest.mark.parametrize("origin", ["https://*.example.com", "http://a.example,http://b.example", "localhost:5173"])
def test_settings_accept_only_one_explicit_origin(monkeypatch: pytest.MonkeyPatch, origin: str) -> None:
    monkeypatch.setenv("ARCHIPELAGO_CORS_ORIGIN", origin)

    with pytest.raises(RuntimeError, match="ARCHIPELAGO_CORS_ORIGIN"):
        load_settings()


@pytest.mark.parametrize(("value", "secure"), [(None, True), ("false", False), ("False", True)])
def test_cookies_are_https_only_unless_explicitly_turned_off(monkeypatch: pytest.MonkeyPatch, value: str | None, secure: bool) -> None:
    if value is None:
        monkeypatch.delenv("ARCHIPELAGO_COOKIE_SECURE")
    else:
        monkeypatch.setenv("ARCHIPELAGO_COOKIE_SECURE", value)

    assert load_settings().cookie_secure is secure
