import asyncio
import base64
import gzip
import json
from datetime import UTC, datetime
from typing import Any

from archipelago import Result
from fastapi import APIRouter, Depends, HTTPException, Request, Response
from redis.asyncio import Redis

from app import runner
from app.auth import CsrfSession, CurrentSession, require_session
from app.db import runs
from app.ids import is_run_id, mint_run_id
from app.models.runs import CreateRun, RunPage, RunStatus, Status
from app.redis_names import JOB_CONFIG, JOB_RUN_ID, JOB_STREAM, PENDING_RUNS
from app.runner import RunnerRejected, RunnerUnavailable
from app.supervisor import start_supervisor, write_abort

RUNS_PAGE_SIZE = 20

router = APIRouter()


def _decode_cursor(cursor: str) -> tuple[datetime, str]:
    try:
        raw = base64.b64decode(cursor + "=" * (-len(cursor) % 4), altchars=b"-_", validate=True)
        value = json.loads(raw)
        if not isinstance(value, dict) or set(value) != {"created_at", "run_id"}:
            raise ValueError
        created_at = datetime.fromisoformat(value["created_at"])
        if created_at.tzinfo is None or created_at.utcoffset() != UTC.utcoffset(created_at) or not is_run_id(value["run_id"]):
            raise ValueError
        return created_at, value["run_id"]
    except (ValueError, TypeError, KeyError) as e:
        raise HTTPException(422, "Invalid cursor") from e


def _encode_cursor(run: RunStatus) -> str:
    value = json.dumps({"created_at": run.created_at.astimezone(UTC).isoformat(), "run_id": run.run_id}, separators=(",", ":"))
    return base64.urlsafe_b64encode(value.encode()).decode().rstrip("=")


@router.get("/presets", dependencies=[Depends(require_session)])
async def get_presets(request: Request) -> Response:
    try:
        body = await runner.presets(request.app.state.runner)
    except RunnerUnavailable as e:
        raise HTTPException(502, str(e)) from e
    return Response(body, media_type="application/json")


@router.post("/runs", status_code=202)
async def create_run(body: CreateRun, request: Request, session: CsrfSession) -> RunStatus:
    state: Any = request.app.state
    redis: Redis = state.redis
    run = await runs.insert_run(state.pool, mint_run_id(), session.user_id, body)
    # Enqueue first: if the API dies before the start, recovery cleans up a waiting job, not an EA whose logs never expire
    await redis.xadd(
        PENDING_RUNS,
        {JOB_RUN_ID: run.run_id, JOB_STREAM: "true" if run.stream else "false", JOB_CONFIG: run.clustering.model_dump_json()},
    )
    try:
        started = await runner.start(state.runner, run.run_id, run.preset, run.overrides)
    except RunnerRejected as e:
        await runs.set_failed(state.pool, run.run_id, "runner", str(e))
        # A worker may already hold the job, and the abort makes it let go
        await write_abort(redis, run.run_id, str(e))
        start_supervisor(run.run_id, state.pool, redis, state.runner, runner_done=True, abort_sent=True)
        raise HTTPException(422, str(e)) from e
    except RunnerUnavailable as e:
        await runs.set_failed(state.pool, run.run_id, "runner", str(e))
        await write_abort(redis, run.run_id, str(e))
        # The start may have reached the runner before the error, so the supervisor still polls and cancels it
        start_supervisor(run.run_id, state.pool, redis, state.runner, abort_sent=True)
        raise HTTPException(502, str(e)) from e
    if started.state == "running":
        await runs.advance_status(state.pool, run.run_id, "running")
        run = run.model_copy(update={"status": "running"})
    start_supervisor(run.run_id, state.pool, redis, state.runner)
    return run


@router.get("/runs", response_model=RunPage)
async def list_runs(
    request: Request,
    session: CurrentSession,
    status: Status | None = None,
    preset: str | None = None,
    cursor: str | None = None,
) -> RunPage:
    before = _decode_cursor(cursor) if cursor is not None else None
    rows = await runs.list_runs(request.app.state.pool, session.user_id, RUNS_PAGE_SIZE + 1, status, preset, before)
    more = len(rows) > RUNS_PAGE_SIZE
    page = rows[:RUNS_PAGE_SIZE]
    return RunPage(runs=page, next_cursor=_encode_cursor(page[-1]) if more else None)


@router.get("/runs/{run_id}", response_model=RunStatus)
async def get_run(run_id: str, request: Request, session: CurrentSession) -> RunStatus:
    run = await runs.get_owned_run(request.app.state.pool, run_id, session.user_id) if is_run_id(run_id) else None
    if run is None:
        raise HTTPException(404, "Run not found")
    return run


@router.post("/runs/{run_id}/cancel", responses={202: {"model": RunStatus}})
async def cancel_run(run_id: str, request: Request, response: Response, session: CsrfSession) -> RunStatus:
    run = await runs.request_run_cancellation(request.app.state.pool, run_id, session.user_id) if is_run_id(run_id) else None
    if run is None:
        raise HTTPException(404, "Run not found")
    if run.status == "cancelling":
        response.status_code = 202
    return run


@router.get("/runs/{run_id}/result", responses={200: {"model": Result}})
async def run_result(run_id: str, request: Request, session: CurrentSession) -> Response:
    state: Any = request.app.state
    run = await runs.get_owned_run(state.pool, run_id, session.user_id) if is_run_id(run_id) else None
    if run is None:
        raise HTTPException(404, "Run not found")
    body = await runs.get_result_body(state.pool, run_id)
    if body is None:
        raise HTTPException(404, f"No result: run {run.status}" if run.status in ("failed", "cancelled") else f"No result: run is {run.status}")
    headers = {"Vary": "Accept-Encoding"}
    if "gzip" in request.headers.get("accept-encoding", ""):
        return Response(body, media_type="application/json", headers={**headers, "Content-Encoding": "gzip"})
    return Response(await asyncio.to_thread(gzip.decompress, body), media_type="application/json", headers=headers)
