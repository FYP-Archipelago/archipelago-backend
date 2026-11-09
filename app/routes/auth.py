import asyncio
from datetime import UTC, datetime

from fastapi import APIRouter, Depends, HTTPException, Request, Response

from app.auth import ACCESS_COOKIE, REFRESH_COOKIE, REFRESH_COOKIE_PATH, CurrentSession, hash_token, mint_token, passwords, require_origin
from app.db import auth as auth_db
from app.models.auth import LoginRequest, RegisterRequest, Session, SessionResponse, User

router = APIRouter(prefix="/auth")


def _body(user: User, session: Session) -> SessionResponse:
    return SessionResponse(
        user=user, csrf_token=session.csrf_token, access_expires_at=session.access_expires_at, refresh_expires_at=session.refresh_expires_at
    )


def _set_cookies(response: Response, secure: bool, session: Session, access: str, refresh: str) -> None:
    now = datetime.now(UTC)
    for name, token, expires, path in (
        (ACCESS_COOKIE, access, session.access_expires_at, "/"),
        (REFRESH_COOKIE, refresh, session.refresh_expires_at, REFRESH_COOKIE_PATH),
    ):
        response.set_cookie(name, token, max_age=int((expires - now).total_seconds()), path=path, secure=secure, httponly=True, samesite="lax")


async def _start_session(request: Request, response: Response, user: User) -> SessionResponse:
    state = request.app.state
    # A login replaces this browser's previous session rather than leaving it valid until it expires
    previous = request.cookies.get(REFRESH_COOKIE)
    if previous:
        await auth_db.delete_session(state.pool, hash_token(previous))
    access, refresh = mint_token(), mint_token()
    session = await auth_db.create_session(state.pool, user.id, hash_token(access), hash_token(refresh), mint_token())
    _set_cookies(response, state.settings.cookie_secure, session, access, refresh)
    return _body(user, session)


@router.post("/register", status_code=201, dependencies=[Depends(require_origin)])
async def register(body: RegisterRequest, request: Request, response: Response) -> SessionResponse:
    password_hash = await asyncio.to_thread(passwords.hash, body.password_digest)
    user = await auth_db.create_user(request.app.state.pool, body.email, password_hash, body.full_name, body.institution)
    if user is None:
        raise HTTPException(409, "Email already registered")
    return await _start_session(request, response, user)


@router.post("/login", dependencies=[Depends(require_origin)])
async def login(body: LoginRequest, request: Request, response: Response) -> SessionResponse:
    found = await auth_db.get_login(request.app.state.pool, body.email)
    if found is not None:
        user, password_hash = found
        if await asyncio.to_thread(passwords.verify, body.password_digest, password_hash):
            return await _start_session(request, response, user)
    raise HTTPException(401, "Invalid email or password")


@router.get("/me")
async def me(request: Request, session: CurrentSession) -> SessionResponse:
    return _body(await auth_db.get_user(request.app.state.pool, session.user_id), session)


@router.post("/refresh", dependencies=[Depends(require_origin)])
async def refresh_session(request: Request, response: Response) -> SessionResponse:
    state = request.app.state
    token = request.cookies.get(REFRESH_COOKIE)
    access, refresh = mint_token(), mint_token()
    session = await auth_db.refresh_session(state.pool, hash_token(token), hash_token(access), hash_token(refresh)) if token else None
    if session is None:
        raise HTTPException(401, "Invalid or expired refresh token")
    _set_cookies(response, state.settings.cookie_secure, session, access, refresh)
    return _body(await auth_db.get_user(state.pool, session.user_id), session)


@router.post("/logout", status_code=204, dependencies=[Depends(require_origin)])
async def logout(request: Request, response: Response) -> None:
    state = request.app.state
    token = request.cookies.get(REFRESH_COOKIE)
    if token:
        await auth_db.delete_session(state.pool, hash_token(token))
    # A cookie is only deleted by a matching path
    for name, path in ((ACCESS_COOKIE, "/"), (REFRESH_COOKIE, REFRESH_COOKIE_PATH)):
        response.delete_cookie(name, path=path, secure=state.settings.cookie_secure, httponly=True, samesite="lax")
