import secrets
from datetime import UTC, datetime
from typing import Annotated

from fastapi import Depends, HTTPException, Request

from app.db import auth
from app.models.auth import Session, User
from app.security import hash_token
from app.settings import ACCESS_COOKIE_NAME, Settings


def require_origin(request: Request) -> None:
    origins = request.headers.getlist("origin")
    settings: Settings = request.app.state.settings
    if len(origins) != 1 or origins[0] != settings.cors_origin:
        raise HTTPException(403, "Untrusted Origin")


async def require_session(request: Request) -> tuple[User, Session]:
    token = request.cookies.get(ACCESS_COOKIE_NAME, "")
    if not token:
        raise HTTPException(401, "Authentication required")
    async with request.app.state.pool.connection() as conn:
        session = await auth.get_session_by_access_hash(conn, hash_token(token), datetime.now(UTC))
        if session is None:
            raise HTTPException(401, "Authentication required")
        user = await auth.get_user(conn, session.user_id)
    if user is None:
        raise HTTPException(401, "Authentication required")
    return user, session


CurrentSession = Annotated[tuple[User, Session], Depends(require_session)]


async def require_csrf(request: Request, current: CurrentSession) -> tuple[User, Session]:
    require_origin(request)
    tokens = request.headers.getlist("x-csrf-token")
    if len(tokens) != 1 or not secrets.compare_digest(tokens[0].encode(), current[1].csrf_token.encode()):
        raise HTTPException(403, "Invalid CSRF token")
    return current
