from datetime import timedelta
from uuid import UUID, uuid4

from psycopg_pool import AsyncConnectionPool

from app.models.auth import Session, User

ACCESS_LIFETIME = timedelta(hours=4)
REFRESH_LIFETIME = timedelta(days=30)  # Time from login

USER_COLUMNS = "id, email, full_name, institution, created_at"
SESSION_COLUMNS = "id, user_id, csrf_token, access_expires_at, refresh_expires_at"


async def create_user(pool: AsyncConnectionPool, email: str, password_hash: str, full_name: str, institution: str | None) -> User | None:
    async with pool.connection() as conn:
        cur = await conn.execute(
            f"INSERT INTO users (id, email, password_hash, full_name, institution) VALUES (%s, %s, %s, %s, %s) "
            f"ON CONFLICT (email) DO NOTHING RETURNING {USER_COLUMNS}",
            (uuid4(), email, password_hash, full_name, institution),
        )
        row = await cur.fetchone()
    return User(**row) if row else None


async def get_user(pool: AsyncConnectionPool, user_id: UUID) -> User:
    async with pool.connection() as conn:
        cur = await conn.execute(f"SELECT {USER_COLUMNS} FROM users WHERE id = %s", (user_id,))
        row = await cur.fetchone()
    assert row is not None
    return User(**row)


async def get_login(pool: AsyncConnectionPool, email: str) -> tuple[User, str] | None:
    async with pool.connection() as conn:
        cur = await conn.execute(f"SELECT {USER_COLUMNS}, password_hash FROM users WHERE email = %s", (email,))
        row = await cur.fetchone()
    if row is None:
        return None
    password_hash = row.pop("password_hash")
    return User(**row), password_hash


async def create_session(pool: AsyncConnectionPool, user_id: UUID, access_token_hash: bytes, refresh_token_hash: bytes, csrf_token: str) -> Session:
    async with pool.connection() as conn:
        cur = await conn.execute(
            "INSERT INTO auth_sessions (id, user_id, access_token_hash, refresh_token_hash, csrf_token, access_expires_at, refresh_expires_at) "
            f"VALUES (%s, %s, %s, %s, %s, now() + %s, now() + %s) RETURNING {SESSION_COLUMNS}",
            (uuid4(), user_id, access_token_hash, refresh_token_hash, csrf_token, ACCESS_LIFETIME, REFRESH_LIFETIME),
        )
        row = await cur.fetchone()
    assert row is not None
    return Session(**row)


async def get_session(pool: AsyncConnectionPool, access_token_hash: bytes) -> Session | None:
    async with pool.connection() as conn:
        cur = await conn.execute(
            f"SELECT {SESSION_COLUMNS} FROM auth_sessions WHERE access_token_hash = %s AND access_expires_at > now()", (access_token_hash,)
        )
        row = await cur.fetchone()
    return Session(**row) if row else None


async def is_session_active(pool: AsyncConnectionPool, session_id: UUID) -> bool:
    async with pool.connection() as conn:
        cur = await conn.execute("SELECT 1 FROM auth_sessions WHERE id = %s AND refresh_expires_at > now()", (session_id,))
        return await cur.fetchone() is not None


# One UPDATE, so two refreshes with the same token can't both succeed: the second waits for the first's row lock, then no longer matches the old hash.
# Uses clock_timestamp() rather than now(), so a refresh that waited for that lock checks the deadline as of after the wait.
async def refresh_session(pool: AsyncConnectionPool, refresh_token_hash: bytes, new_access_hash: bytes, new_refresh_hash: bytes) -> Session | None:
    async with pool.connection() as conn:
        cur = await conn.execute(
            "UPDATE auth_sessions SET access_token_hash = %s, refresh_token_hash = %s, "
            "access_expires_at = LEAST(clock_timestamp() + %s, refresh_expires_at) "
            f"WHERE refresh_token_hash = %s AND refresh_expires_at > clock_timestamp() RETURNING {SESSION_COLUMNS}",
            (new_access_hash, new_refresh_hash, ACCESS_LIFETIME, refresh_token_hash),
        )
        row = await cur.fetchone()
    return Session(**row) if row else None


# Deletes by the session's id, found before any wait, so a logout racing a refresh still ends the session the refresh rotated.
async def delete_session(pool: AsyncConnectionPool, refresh_token_hash: bytes) -> None:
    async with pool.connection() as conn:
        await conn.execute("DELETE FROM auth_sessions WHERE id = (SELECT id FROM auth_sessions WHERE refresh_token_hash = %s)", (refresh_token_hash,))


async def delete_expired_sessions(pool: AsyncConnectionPool) -> None:
    async with pool.connection() as conn:
        await conn.execute("DELETE FROM auth_sessions WHERE refresh_expires_at <= now()")
