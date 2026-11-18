from pathlib import Path

from psycopg.rows import dict_row
from psycopg_pool import AsyncConnectionPool

SCHEMA_PATH = Path(__file__).with_name("schema.sql")


async def open_pool(url: str) -> AsyncConnectionPool:
    pool = AsyncConnectionPool(url, open=False, kwargs={"row_factory": dict_row})
    await pool.open(wait=True)
    return pool


async def apply_schema(pool: AsyncConnectionPool) -> None:
    async with pool.connection() as conn:
        await conn.execute(SCHEMA_PATH.read_text())
