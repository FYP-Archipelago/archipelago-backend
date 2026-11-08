import asyncio
from typing import Any

import psycopg
from fastapi import APIRouter, Request
from fastapi.responses import JSONResponse
from redis.exceptions import RedisError

from app import __version__, runner
from app.runner import RunnerUnavailable

router = APIRouter()


@router.get("/health")
async def health(request: Request) -> JSONResponse:
    state: Any = request.app.state

    async def check_db() -> bool:
        try:
            async with asyncio.timeout(2), state.pool.connection() as conn:
                await conn.execute("SELECT 1")
        except (OSError, psycopg.Error):
            return False
        return True

    async def check_redis() -> bool:
        try:
            async with asyncio.timeout(2):
                await state.redis.ping()
        except (OSError, RedisError):
            return False
        return True

    async def check_runner() -> bool:
        try:
            async with asyncio.timeout(2):
                return await runner.health(state.runner)
        except (TimeoutError, RunnerUnavailable):
            return False

    db_ok, redis_ok, runner_ok = await asyncio.gather(check_db(), check_redis(), check_runner())
    ok = db_ok and redis_ok
    ready = ok and runner_ok

    if ready:
        message = "ᕕ( ᐛ )ᕗ  200 OK. We are go for launch."
    elif ok:
        message = "(-_-) zZ  Runner is touching grass somewhere. Past runs are still here though, so Yayy?"
    else:
        missing = ", ".join(name for name, available in (("Postgres", db_ok), ("Redis", redis_ok), ("runner", runner_ok)) if not available)
        message = f"(╯°□°)╯︵ ┻━┻  The dependency graph has become a crime scene. They lowkey dead: {missing}."

    return JSONResponse(
        {
            "status": "ok" if ok else "unavailable",
            "version": __version__,
            "db": db_ok,
            "redis": redis_ok,
            "runner": runner_ok,
            "ready": ready,
            "message": message,
        },
        status_code=200 if ok else 503,
    )
