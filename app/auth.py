import hashlib
import secrets
from typing import Annotated

from fastapi import Depends, HTTPException, Request
from pwdlib import PasswordHash

from app.db import auth as auth_db
from app.models.auth import Session

ACCESS_COOKIE = "archipelago_access"
REFRESH_COOKIE = "archipelago_refresh"
REFRESH_COOKIE_PATH = "/auth"  # Only the auth routes need it

passwords = PasswordHash.recommended()


def mint_token() -> str:
    return secrets.token_urlsafe(32)


def hash_token(token: str) -> bytes:
    return hashlib.sha256(token.encode()).digest()


def require_origin(request: Request) -> None:
    if request.headers.get("origin") != request.app.state.settings.cors_origin:
        raise HTTPException(403, "Untrusted origin")


async def require_session(request: Request) -> Session:
    token = request.cookies.get(ACCESS_COOKIE)
    session = await auth_db.get_session(request.app.state.pool, hash_token(token)) if token else None
    if session is None:
        raise HTTPException(401, "Authentication required")
    return session


CurrentSession = Annotated[Session, Depends(require_session)]


async def require_csrf(request: Request, session: CurrentSession) -> Session:
    require_origin(request)
    if not secrets.compare_digest(request.headers.get("x-csrf-token", "").encode(), session.csrf_token.encode()):
        raise HTTPException(403, "Invalid CSRF token")
    return session


CsrfSession = Annotated[Session, Depends(require_csrf)]
