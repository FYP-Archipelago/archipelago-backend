import asyncio
import gzip
import json
import re
import time
from collections.abc import AsyncGenerator
from typing import Any

import psycopg
from archipelago import Result
from fastapi import APIRouter, HTTPException, Request, Response
from fastapi.responses import JSONResponse, StreamingResponse
from redis.asyncio import Redis
from redis.exceptions import RedisError

from app import __version__, runner
from app.db import queries
from app.ids import is_run_id, mint_run_id
from app.models import CreateRun, RunStatus
from app.redis_names import ERROR, FINAL, JOB_CONFIG, JOB_RUN_ID, JOB_STREAM, PENDING_RUNS, PHASE, UPDATE, clustered_key
from app.runner import RunnerRejected, RunnerUnavailable
from app.supervisor import start_supervisor, write_abort

SSE_BLOCK_MS = 2000
KEEPALIVE_SECONDS = 15

STREAM_ID = re.compile(r"^\d+-\d+$")

router = APIRouter()


def _sse(event: str, data: bytes, event_id: bytes | None = None) -> bytes:
    head = f"event: {event}\n".encode()
    if event_id is not None:
        head += b"id: " + event_id + b"\n"
    return head + b"data: " + data + b"\n\n"


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


@router.get("/runs/{run_id}/events")
async def run_events(run_id: str, request: Request) -> StreamingResponse:
    state: Any = request.app.state
    redis: Redis = state.redis
    run = await queries.get_run(state.pool, run_id) if is_run_id(run_id) else None
    if run is None:
        raise HTTPException(404, "run not found")
    last_event_id = request.headers.get("last-event-id", "")
    key = clustered_key(run_id)

    async def stream() -> AsyncGenerator[bytes]:
        status = run.status
        after = last_event_id if STREAM_ID.fullmatch(last_event_id) else "0-0"
        last_output = time.monotonic()
        yield _sse("status", json.dumps({"status": status}).encode())
        while not await request.is_disconnected():
            response = await redis.xread({key: after}, block=SSE_BLOCK_MS)
            for _, entries in response:
                for entry_id, fields in entries:
                    after = entry_id.decode()
                    if UPDATE in fields:
                        yield _sse("update", fields[UPDATE], entry_id)
                    elif FINAL in fields or (PHASE in fields and ERROR in fields):
                        current = await queries.get_run(state.pool, run_id)
                        # a cancelled run ends with its status: the error entry is the worker's "run cancelled" and a final is stale
                        if current is not None and current.status == "cancelled":
                            if status != "cancelled":
                                yield _sse("status", json.dumps({"status": "cancelled"}).encode())
                            return
                        if FINAL not in fields:
                            yield _sse("error", json.dumps({"phase": fields[PHASE].decode(), "message": fields[ERROR].decode()}).encode())
                            return
                        # a run the backend already failed has no result, so /result would answer 404
                        if current is not None and current.error is not None:
                            yield _sse("error", current.error.model_dump_json().encode())
                            return
                        yield _sse("final", fields[FINAL], entry_id)
                        return

            current = await queries.get_run(state.pool, run_id)
            if current is None:
                return
            if response or current.status != status:
                last_output = time.monotonic()
            if current.status != status:
                status = current.status
                yield _sse("status", json.dumps({"status": status}).encode())
            # only once the stream is idle, so entries already written go out before the run is reported failed, cancelled or done
            if not response:
                if current.error is not None:
                    yield _sse("error", current.error.model_dump_json().encode())
                    return
                if status in ("done", "cancelled"):
                    return
                if time.monotonic() - last_output >= KEEPALIVE_SECONDS:
                    yield b": keep-alive\n\n"
                    last_output = time.monotonic()

    return StreamingResponse(stream(), media_type="text/event-stream", headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"})


@router.get("/runs/{run_id}/result", responses={200: {"model": Result}})
async def run_result(run_id: str, request: Request) -> Response:
    state: Any = request.app.state
    run = await queries.get_run(state.pool, run_id) if is_run_id(run_id) else None
    if run is None:
        raise HTTPException(404, "run not found")
    body = await queries.get_result_body(state.pool, run_id)
    if body is None:
        raise HTTPException(404, f"no result: run {run.status}" if run.status in ("failed", "cancelled") else f"no result: run is {run.status}")
    headers = {"Vary": "Accept-Encoding"}
    if "gzip" in request.headers.get("accept-encoding", ""):
        return Response(body, media_type="application/json", headers={**headers, "Content-Encoding": "gzip"})
    return Response(await asyncio.to_thread(gzip.decompress, body), media_type="application/json", headers=headers)


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
