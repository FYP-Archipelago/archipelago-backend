from pathlib import Path
from typing import Any

from psycopg.rows import dict_row
from psycopg.types.json import Jsonb
from psycopg_pool import AsyncConnectionPool

from app.models import CreateRun, Phase, RunError, RunStatus, Status

SCHEMA_PATH = Path(__file__).with_name("schema.sql")
ORDER: list[Status] = ["queued", "running", "clustering", "done"]
NON_FINAL = ["queued", "running", "clustering"]


async def open_pool(url: str) -> AsyncConnectionPool:
    pool = AsyncConnectionPool(url, open=False, kwargs={"row_factory": dict_row})
    await pool.open(wait=True)
    return pool


async def apply_schema(pool: AsyncConnectionPool) -> None:
    async with pool.connection() as conn:
        await conn.execute(SCHEMA_PATH.read_text())


def _to_run_status(row: dict[str, Any]) -> RunStatus:
    error = RunError(phase=row["error_phase"], message=row["error_message"]) if row["error_phase"] else None
    return RunStatus(
        run_id=row["run_id"],
        status=row["status"],
        stream=row["stream"],
        preset=row["preset"],
        overrides=row["overrides"],
        clustering=row["clustering"],
        error=error,
        created_at=row["created_at"],
        updated_at=row["updated_at"],
    )


async def insert_run(pool: AsyncConnectionPool, run_id: str, body: CreateRun) -> RunStatus:
    async with pool.connection() as conn:
        cur = await conn.execute(
            "INSERT INTO runs (run_id, stream, preset, overrides, clustering) VALUES (%s, %s, %s, %s, %s) RETURNING *",
            (run_id, body.stream, body.preset, Jsonb(body.overrides), Jsonb(body.clustering.model_dump(mode="json"))),
        )
        row = await cur.fetchone()
    assert row is not None
    return _to_run_status(row)


async def get_run(pool: AsyncConnectionPool, run_id: str) -> RunStatus | None:
    async with pool.connection() as conn:
        cur = await conn.execute("SELECT * FROM runs WHERE run_id = %s", (run_id,))
        row = await cur.fetchone()
    return _to_run_status(row) if row else None


async def advance_status(pool: AsyncConnectionPool, run_id: str, to: Status) -> None:
    earlier = ORDER[: ORDER.index(to)]
    async with pool.connection() as conn:
        await conn.execute(
            "UPDATE runs SET status = %s, updated_at = now() WHERE run_id = %s AND status = ANY(%s)",
            (to, run_id, earlier),
        )


async def set_failed(pool: AsyncConnectionPool, run_id: str, phase: Phase, message: str) -> None:
    async with pool.connection() as conn:
        await conn.execute(
            "UPDATE runs SET status = 'failed', error_phase = %s, error_message = %s, updated_at = now() WHERE run_id = %s AND status = ANY(%s)",
            (phase, message, run_id, NON_FINAL),
        )


async def save_result(pool: AsyncConnectionPool, run_id: str, body: bytes) -> None:
    async with pool.connection() as conn, conn.transaction():
        cur = await conn.execute(
            "UPDATE runs SET status = 'done', updated_at = now() WHERE run_id = %s AND status = ANY(%s)",
            (run_id, NON_FINAL),
        )
        if cur.rowcount == 0:
            return
        await conn.execute("INSERT INTO results (run_id, body) VALUES (%s, %s)", (run_id, body))


async def mark_streams_expired(pool: AsyncConnectionPool, run_id: str) -> None:
    async with pool.connection() as conn:
        await conn.execute("UPDATE runs SET streams_expired = true, updated_at = now() WHERE run_id = %s", (run_id,))


async def list_unexpired(pool: AsyncConnectionPool) -> list[str]:
    async with pool.connection() as conn:
        cur = await conn.execute("SELECT run_id FROM runs WHERE NOT streams_expired")
        return [row["run_id"] for row in await cur.fetchall()]


async def get_result_body(pool: AsyncConnectionPool, run_id: str) -> bytes | None:
    async with pool.connection() as conn:
        cur = await conn.execute("SELECT body FROM results WHERE run_id = %s", (run_id,))
        row = await cur.fetchone()
    return bytes(row["body"]) if row else None
