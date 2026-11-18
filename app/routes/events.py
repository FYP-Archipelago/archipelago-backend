import json
import re
import time
from collections.abc import AsyncGenerator
from typing import Any

from fastapi import APIRouter, HTTPException, Request
from fastapi.responses import StreamingResponse
from redis.asyncio import Redis

from app.auth import CurrentSession
from app.db import auth as auth_db
from app.db import runs
from app.ids import is_run_id
from app.redis_names import ERROR, FINAL, PHASE, UPDATE, clustered_key

SSE_BLOCK_MS = 2000
KEEPALIVE_SECONDS = 15  # Idle stream sends Keep Alive so proxies and browsers don't drop the connection
SESSION_CHECK_SECONDS = 10  # A logout ends open streams within the limit
STREAM_ID = re.compile(r"^\d+-\d+$")

router = APIRouter()


def _sse(event: str, data: bytes, event_id: bytes | None = None) -> bytes:
    head = f"event: {event}\n".encode()
    if event_id is not None:
        head += b"id: " + event_id + b"\n"
    return head + b"data: " + data + b"\n\n"


@router.get("/runs/{run_id}/events")
async def run_events(run_id: str, request: Request, session: CurrentSession) -> StreamingResponse:
    state: Any = request.app.state
    redis: Redis = state.redis
    run = await runs.get_owned_run(state.pool, run_id, session.user_id) if is_run_id(run_id) else None
    if run is None:
        raise HTTPException(404, "Run not found")
    last_event_id = request.headers.get("last-event-id", "")
    key = clustered_key(run_id)

    async def stream() -> AsyncGenerator[bytes]:
        status = run.status
        after = last_event_id if STREAM_ID.fullmatch(last_event_id) else "0-0"
        last_output = last_session_check = time.monotonic()
        yield _sse("status", json.dumps({"status": status}).encode())
        if status == "cancelled":
            return
        while not await request.is_disconnected():
            response = await redis.xread({key: after}, block=SSE_BLOCK_MS)
            if time.monotonic() - last_session_check >= SESSION_CHECK_SECONDS:
                if not await auth_db.is_session_active(state.pool, session.id):
                    yield _sse("auth_required", b'{"detail":"Authentication required"}')
                    return
                last_session_check = time.monotonic()
            current = await runs.get_owned_run(state.pool, run_id, session.user_id)
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
                        current = await runs.get_owned_run(state.pool, run_id, session.user_id)
                        if current is None:
                            return
                        # A worker's cancellation acknowledgement or a late result doesn't end a cancellation still in progress
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
                        # A run the backend already failed has no result, so /result would answer 404
                        if current.error is not None:
                            yield _sse("error", current.error.model_dump_json().encode())
                            return
                        yield _sse("final", fields[FINAL], entry_id)
                        return

            current = await runs.get_owned_run(state.pool, run_id, session.user_id)
            if current is None:
                return
            if response or current.status != status:
                last_output = time.monotonic()
            if current.status != status:
                status = current.status
                yield _sse("status", json.dumps({"status": status}).encode())
            # Only once the stream is idle, so entries already written go out before the run is reported failed, cancelled or done
            if not response:
                if current.error is not None:
                    yield _sse("error", current.error.model_dump_json().encode())
                    return
                if status in ("done", "cancelled"):
                    return
                if time.monotonic() - last_output >= KEEPALIVE_SECONDS:
                    yield b": keep-alive\n\n"
                    last_output = time.monotonic()

    return StreamingResponse(stream(), media_type="text/event-stream", headers={"X-Accel-Buffering": "no"})
