from datetime import UTC, datetime, timedelta
from typing import Any
from uuid import UUID

from fastapi import APIRouter, HTTPException, Request, Response
from psycopg import AsyncConnection
from psycopg.errors import UniqueViolation

from app import security
from app.db import auth
from app.dependencies import CurrentSession
from app.models.auth import LoginRequest, RegisterRequest, Session, SessionResponse, User, UserResponse
from app.settings import ACCESS_COOKIE_NAME, REFRESH_COOKIE_NAME, Settings

ACCESS_LIFETIME = timedelta(hours=4)
REFRESH_LIFETIME = timedelta(days=30)

router = APIRouter(prefix="/auth", tags=["auth"])


def _session_response(user: User, session: Session) -> SessionResponse:
    return SessionResponse(
        user=UserResponse.model_validate(user),
        csrf_token=session.csrf_token,
        access_expires_at=session.access_expires_at,
        refresh_expires_at=session.refresh_expires_at,
    )


async def _create_session(conn: AsyncConnection[dict[str, Any]], user_id: UUID, request: Request) -> tuple[Session, str, str]:
    now = datetime.now(UTC)
    previous = None
    refresh = request.cookies.get(REFRESH_COOKIE_NAME, "")
    access = request.cookies.get(ACCESS_COOKIE_NAME, "")
    if refresh:
        previous = await auth.get_session_by_refresh_hash(conn, security.hash_token(refresh), now)
    if previous is None and access:
        previous = await auth.get_session_by_access_hash(conn, security.hash_token(access), now)
    if previous is not None:
        await auth.delete_session(conn, previous.id)

    access, refresh = security.mint_token(), security.mint_token()
    session = await auth.insert_session(
        conn, user_id, security.hash_token(access), security.hash_token(refresh), security.mint_token(), now + ACCESS_LIFETIME, now + REFRESH_LIFETIME
    )
    return session, access, refresh


def _set_cookies(response: Response, settings: Settings, session: Session, access: str, refresh: str) -> None:
    now = datetime.now(UTC)
    for name, token, expires in (
        (ACCESS_COOKIE_NAME, access, session.access_expires_at),
        (REFRESH_COOKIE_NAME, refresh, session.refresh_expires_at),
    ):
        response.set_cookie(
            name,
            token,
            max_age=max(0, int((expires - now).total_seconds())),
            expires=expires,
            secure=settings.cookie_secure,
            httponly=True,
            samesite="lax",
            path="/",
        )


@router.post("/register", status_code=201)
async def register(body: RegisterRequest, request: Request, response: Response) -> SessionResponse:
    state = request.app.state
    password_hash = security.hash_password(body.password_digest)
    try:
        async with state.pool.connection() as conn, conn.transaction():
            user = await auth.insert_user(conn, body.email, password_hash, body.full_name, body.institution)
            session, access, refresh = await _create_session(conn, user.id, request)
    except UniqueViolation as error:
        if error.diag.constraint_name == "users_email_key":
            raise HTTPException(409, "Email already registered") from None
        raise
    _set_cookies(response, state.settings, session, access, refresh)
    return _session_response(user, session)


@router.post("/login")
async def login(body: LoginRequest, request: Request, response: Response) -> SessionResponse:
    state = request.app.state
    async with state.pool.connection() as conn:
        user = await auth.get_user_by_email(conn, body.email)
    if user is None or not security.verify_password(body.password_digest, user.password_hash):
        raise HTTPException(401, "Invalid email or password")
    async with state.pool.connection() as conn, conn.transaction():
        session, access, refresh = await _create_session(conn, user.id, request)
    _set_cookies(response, state.settings, session, access, refresh)
    return _session_response(user, session)


@router.get("/me")
async def current_user(current: CurrentSession) -> SessionResponse:
    return _session_response(*current)


@router.post("/refresh")
async def refresh_session(request: Request, response: Response) -> SessionResponse:
    token = request.cookies.get(REFRESH_COOKIE_NAME, "")
    if not token:
        raise HTTPException(401, "Invalid or expired refresh token")
    state = request.app.state
    async with state.pool.connection() as conn, conn.transaction():
        session = await auth.lock_session_by_refresh_hash(conn, security.hash_token(token))
        # Checks expiry after acquiring the lock, including time spent waiting for another request.
        now = datetime.now(UTC)
        if session is None or session.refresh_expires_at <= now:
            raise HTTPException(401, "Invalid or expired refresh token")
        user = await auth.get_user(conn, session.user_id)
        if user is None:
            raise HTTPException(401, "Invalid or expired refresh token")
        access, refresh = security.mint_token(), security.mint_token()
        session = await auth.rotate_session(
            conn, session.id, security.hash_token(access), security.hash_token(refresh), min(now + ACCESS_LIFETIME, session.refresh_expires_at)
        )
    _set_cookies(response, state.settings, session, access, refresh)
    return _session_response(user, session)
