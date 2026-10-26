import asyncio
from typing import Any

import psycopg
from fastapi import APIRouter, HTTPException, Request, Response
from fastapi.responses import JSONResponse
from redis.asyncio import Redis
from redis.exceptions import RedisError

from app import __version__, runner
from app.db import queries
from app.ids import mint_run_id
from app.models import CreateRun, RunStatus
from app.redis_names import JOB_CONFIG, JOB_RUN_ID, JOB_STREAM, PENDING_RUNS
from app.runner import RunnerRejected, RunnerUnavailable
from app.supervisor import start_supervisor, write_abort

router = APIRouter()


@router.get("/presets")
async def get_presets(request: Request) -> Response:
    try:
        body = await runner.presets(request.app.state.runner)
    except RunnerUnavailable as e:
        raise HTTPException(502, str(e)) from e
    return Response(body, media_type="application/json")


@router.post("/runs", status_code=202)
async def create_run(body: CreateRun, request: Request) -> RunStatus:
    state: Any = request.app.state
    redis: Redis = state.redis
    run = await queries.insert_run(state.pool, mint_run_id(), body)
    # enqueue before starting the runner: if the API dies in between, what's left is a waiting job
    # that recovery cleans up, not an EA running with logs that never expire
    await redis.xadd(
        PENDING_RUNS,
        {JOB_RUN_ID: run.run_id, JOB_STREAM: "true" if run.stream else "false", JOB_CONFIG: run.clustering.model_dump_json()},
    )
    try:
        started = await runner.start(state.runner, run.run_id, run.preset, run.overrides)
    except RunnerRejected as e:
        await queries.set_failed(state.pool, run.run_id, "runner", str(e))
        # a worker may already hold the job, and the abort makes it let go
        await write_abort(redis, run.run_id, str(e))
        start_supervisor(run.run_id, state.pool, redis, state.runner, runner_done=True, abort_sent=True)
        raise HTTPException(422, str(e)) from e
    except RunnerUnavailable as e:
        await queries.set_failed(state.pool, run.run_id, "runner", str(e))
        await write_abort(redis, run.run_id, str(e))
        # the start may have reached the runner before the error, so the supervisor still polls and cancels it
        start_supervisor(run.run_id, state.pool, redis, state.runner, abort_sent=True)
        raise HTTPException(502, str(e)) from e
    if started.state == "running":
        await queries.advance_status(state.pool, run.run_id, "running")
        run = run.model_copy(update={"status": "running"})
    start_supervisor(run.run_id, state.pool, redis, state.runner)
    return run


@router.get("/health")
async def health(request: Request) -> JSONResponse:
    state: Any = request.app.state

    async def check_db() -> bool:
        try:
            async with asyncio.timeout(2), state.pool.connection() as conn:
                await conn.execute("SELECT 1")
        except (OSError, TimeoutError, psycopg.Error):
            return False
        return True

    async def check_redis() -> bool:
        try:
            async with asyncio.timeout(2):
                await state.redis.ping()
        except (OSError, TimeoutError, RedisError):
            return False
        return True

    db_ok, redis_ok = await asyncio.gather(check_db(), check_redis())
    ok = db_ok and redis_ok
    return JSONResponse(
        {"status": "ok" if ok else "unavailable", "version": __version__, "db": db_ok, "redis": redis_ok},
        status_code=200 if ok else 503,
    )
