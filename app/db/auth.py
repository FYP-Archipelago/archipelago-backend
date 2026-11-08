from datetime import datetime
from typing import Any
from uuid import UUID, uuid4

from psycopg import AsyncConnection

from app.models.auth import Session, User


async def insert_user(conn: AsyncConnection[dict[str, Any]], email: str, password_hash: str, full_name: str, institution: str | None = None) -> User:
    cur = await conn.execute(
        "INSERT INTO users (id, email, password_hash, full_name, institution) VALUES (%s, %s, %s, %s, %s) RETURNING *",
        (uuid4(), email.strip().lower(), password_hash, full_name, institution),
    )
    row = await cur.fetchone()
    assert row is not None
    return User(**row)


async def get_user(conn: AsyncConnection[dict[str, Any]], user_id: UUID) -> User | None:
    cur = await conn.execute("SELECT * FROM users WHERE id = %s", (user_id,))
    row = await cur.fetchone()
    return User(**row) if row else None


async def get_user_by_email(conn: AsyncConnection[dict[str, Any]], email: str) -> User | None:
    cur = await conn.execute("SELECT * FROM users WHERE email = %s", (email.strip().lower(),))
    row = await cur.fetchone()
    return User(**row) if row else None


async def insert_session(
    conn: AsyncConnection[dict[str, Any]],
    user_id: UUID,
    access_token_hash: bytes,
    refresh_token_hash: bytes,
    csrf_token: str,
    access_expires_at: datetime,
    refresh_expires_at: datetime,
) -> Session:
    cur = await conn.execute(
        "INSERT INTO auth_sessions (id, user_id, access_token_hash, refresh_token_hash, csrf_token, access_expires_at, refresh_expires_at) "
        "VALUES (%s, %s, %s, %s, %s, %s, %s) RETURNING *",
        (uuid4(), user_id, access_token_hash, refresh_token_hash, csrf_token, access_expires_at, refresh_expires_at),
    )
    row = await cur.fetchone()
    assert row is not None
    return Session(**row)


async def get_session_by_access_hash(conn: AsyncConnection[dict[str, Any]], token_hash: bytes, now: datetime) -> Session | None:
    cur = await conn.execute(
        "SELECT * FROM auth_sessions WHERE access_token_hash = %s AND access_expires_at > %s AND refresh_expires_at > %s",
        (token_hash, now, now),
    )
    row = await cur.fetchone()
    return Session(**row) if row else None


async def get_session_by_refresh_hash(conn: AsyncConnection[dict[str, Any]], token_hash: bytes, now: datetime) -> Session | None:
    cur = await conn.execute("SELECT * FROM auth_sessions WHERE refresh_token_hash = %s AND refresh_expires_at > %s", (token_hash, now))
    row = await cur.fetchone()
    return Session(**row) if row else None


async def lock_session_by_refresh_hash(conn: AsyncConnection[dict[str, Any]], token_hash: bytes) -> Session | None:
    cur = await conn.execute("SELECT * FROM auth_sessions WHERE refresh_token_hash = %s FOR UPDATE", (token_hash,))
    row = await cur.fetchone()
    return Session(**row) if row else None


async def rotate_session(
    conn: AsyncConnection[dict[str, Any]], session_id: UUID, access_token_hash: bytes, refresh_token_hash: bytes, access_expires_at: datetime
) -> Session:
    cur = await conn.execute(
        "UPDATE auth_sessions SET access_token_hash = %s, refresh_token_hash = %s, access_expires_at = %s WHERE id = %s RETURNING *",
        (access_token_hash, refresh_token_hash, access_expires_at, session_id),
    )
    row = await cur.fetchone()
    assert row is not None
    return Session(**row)


async def delete_session(conn: AsyncConnection[dict[str, Any]], session_id: UUID) -> bool:
    cur = await conn.execute("DELETE FROM auth_sessions WHERE id = %s", (session_id,))
    return cur.rowcount > 0


async def delete_expired_sessions(conn: AsyncConnection[dict[str, Any]], now: datetime) -> int:
    cur = await conn.execute("DELETE FROM auth_sessions WHERE refresh_expires_at <= %s", (now,))
    return cur.rowcount
