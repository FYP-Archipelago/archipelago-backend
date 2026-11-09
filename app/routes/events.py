import json
import re
import time
from collections.abc import AsyncGenerator
from datetime import UTC, datetime
from typing import Any

import psycopg
from fastapi import APIRouter, HTTPException, Request
from fastapi.responses import StreamingResponse
from redis.asyncio import Redis

from app.db import auth, runs
from app.dependencies import CurrentSession
from app.ids import is_run_id
from app.redis_names import ERROR, FINAL, PHASE, UPDATE, clustered_key

SSE_BLOCK_MS = 2000
KEEPALIVE_SECONDS = 15
STREAM_ID = re.compile(r"^\d+-\d+$")

router = APIRouter()


def _sse(event: str, data: bytes, event_id: bytes | None = None) -> bytes:
    head = f"event: {event}\n".encode()
    if event_id is not None:
        head += b"id: " + event_id + b"\n"
    return head + b"data: " + data + b"\n\n"


@router.get("/runs/{run_id}/events")
async def run_events(run_id: str, request: Request, current: CurrentSession) -> StreamingResponse:
    state: Any = request.app.state
    redis: Redis = state.redis
    user, session = current
    run = await runs.get_owned_run(state.pool, run_id, user.id) if is_run_id(run_id) else None
    if run is None:
        raise HTTPException(404, "run not found")
    last_event_id = request.headers.get("last-event-id", "")
    key = clustered_key(run_id)

    async def check_session() -> None:
        async with state.pool.connection() as conn:
            active = await auth.get_session_by_access_hash(conn, session.access_token_hash, datetime.now(UTC))
        if active is None:
            raise HTTPException(401, "Authentication required")

    async def stream() -> AsyncGenerator[bytes]:
        status = run.status
        after = last_event_id if STREAM_ID.fullmatch(last_event_id) else "0-0"
        last_output = time.monotonic()
        yield _sse("status", json.dumps({"status": status}).encode())
        if status == "cancelled":
            return
        while not await request.is_disconnected():
            response = await redis.xread({key: after}, block=SSE_BLOCK_MS)
            # Checking idle streams after each Redis wait, even when there is no event to emit.
            await check_session()
            current = await runs.get_owned_run(state.pool, run_id, user.id)
            if current is None:
                return
            if current.status in ("cancelling", "cancelled"):
                if current.status != status:
                    status = current.status
                    last_output = time.monotonic()
                    yield _sse("status", json.dumps({"status": status}).encode())
                for _, entries in response:
                    if entries:
                        after = entries[-1][0].decode()
                if status == "cancelled":
                    return
                if time.monotonic() - last_output >= KEEPALIVE_SECONDS:
                    yield b": keep-alive\n\n"
                    last_output = time.monotonic()
                continue
            for _, entries in response:
                for entry_id, fields in entries:
                    after = entry_id.decode()
                    if status == "cancelling":
                        continue
                    if UPDATE in fields:
                        yield _sse("update", fields[UPDATE], entry_id)
                    elif FINAL in fields or (PHASE in fields and ERROR in fields):
                        current = await runs.get_owned_run(state.pool, run_id, user.id)
                        if current is None:
                            return
                        # Cancellation acknowledgements and late results do not end an unfinished cancellation.
                        if current.status in ("cancelling", "cancelled"):
                            if current.status != status:
                                status = current.status
                                yield _sse("status", json.dumps({"status": status}).encode())
                            if status == "cancelled":
                                return
                            continue
                        if FINAL not in fields:
                            yield _sse("error", json.dumps({"phase": fields[PHASE].decode(), "message": fields[ERROR].decode()}).encode())
                            return
                        # a run the backend already failed has no result, so /result would answer 404
                        if current.error is not None:
                            yield _sse("error", current.error.model_dump_json().encode())
                            return
                        yield _sse("final", fields[FINAL], entry_id)
                        return

            current = await runs.get_owned_run(state.pool, run_id, user.id)
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

    async def authenticated_stream() -> AsyncGenerator[bytes]:
        try:
            async for event in stream():
                await check_session()
                yield event
        except (HTTPException, psycopg.Error):
            yield _sse("auth_required", b'{"detail":"Authentication required"}')

    return StreamingResponse(authenticated_stream(), media_type="text/event-stream", headers={"Cache-Control": "no-store", "X-Accel-Buffering": "no"})
