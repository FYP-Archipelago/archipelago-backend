import asyncio
import gzip
import json
import logging
import time
from dataclasses import dataclass
from typing import cast

import httpx
from psycopg_pool import AsyncConnectionPool
from redis.asyncio import Redis

from app import runner
from app.db import queries
from app.models import Phase
from app.redis_names import ABORT, DATA, ERROR, FINAL, KIND, PHASE, clustered_key, runner_logs_key
from app.runner import RunnerUnavailable

POLL_SECONDS = 2
RUNNER_MIA_SECONDS = 60
STREAM_TTL_SECONDS = 60
MAX_ERRORS = 3
ERROR_RETRY_SECONDS = 5

logger = logging.getLogger(__name__)

_tasks: set[asyncio.Task[None]] = set()


@dataclass
class _RunState:
    run_id: str
    pool: AsyncConnectionPool
    redis: Redis
    client: httpx.AsyncClient
    runner_done: bool
    abort_sent: bool
    worker_done: bool = False
    cancel_sent: bool = False
    last_id: str = "0-0"
    last_poll: float = 0.0
    first_failed_poll: float | None = None


def start_supervisor(
    run_id: str,
    pool: AsyncConnectionPool,
    redis: Redis,
    client: httpx.AsyncClient,
    *,
    runner_done: bool = False,
    abort_sent: bool = False,
) -> None:
    state = _RunState(run_id, pool, redis, client, runner_done=runner_done, abort_sent=abort_sent)
    task = asyncio.create_task(_supervise(state), name=f"supervisor-{run_id}")
    _tasks.add(task)
    task.add_done_callback(_tasks.discard)


async def write_abort(redis: Redis, run_id: str, message: str) -> None:
    await redis.xadd(runner_logs_key(run_id), {KIND: ABORT, DATA: json.dumps({"error": message})})


async def cancel_all() -> None:
    tasks = list(_tasks)
    for task in tasks:
        task.cancel()
    await asyncio.gather(*tasks, return_exceptions=True)


# Looks after one run until both of its streams have a TTL: saves the result, follows the
# runner's state, and cleans up after a failure on either side.
async def _supervise(state: _RunState) -> None:
    errors = 0
    while True:
        try:
            await _read_clustered(state)
            if not state.runner_done and time.monotonic() - state.last_poll >= POLL_SECONDS:
                await _poll_runner(state)

            run = await queries.get_run(state.pool, state.run_id)
            if run is None:
                return
            # a done run's result is saved, even if its streams expired before the API last stopped
            if run.status == "done":
                state.worker_done = True
            if run.status in ("failed", "cancelled"):
                # a cancelled run's runner was already cancelled, by the route or by the runner itself
                state.cancel_sent = state.cancel_sent or run.status == "cancelled"
                await _clean_up_stopped(state, run.error.message if run.error else "run cancelled")

            # EXPIRE deletes the key, and a later XADD recreates it with no TTL, so wait until
            # neither the runner nor the worker can write to these streams again
            if state.runner_done and state.worker_done:
                await state.redis.expire(runner_logs_key(state.run_id), STREAM_TTL_SECONDS)
                await state.redis.expire(clustered_key(state.run_id), STREAM_TTL_SECONDS)
                await queries.mark_streams_expired(state.pool, state.run_id)
                return
            errors = 0
        except asyncio.CancelledError:
            raise
        except Exception as e:
            # a short Redis or Postgres outage only fails the run if it outlasts a few retries
            errors += 1
            logger.exception("supervisor of %s hit an error (%d in a row)", state.run_id, errors)
            if errors >= MAX_ERRORS:
                try:
                    await queries.set_failed(state.pool, state.run_id, "internal", f"{type(e).__name__}: {e}")
                except Exception:
                    logger.exception("marking %s failed raised too", state.run_id)
            await asyncio.sleep(ERROR_RETRY_SECONDS)


async def _read_clustered(state: _RunState) -> None:
    # blocks up to 2 s, which also paces the supervisor loop
    response = await state.redis.xread({clustered_key(state.run_id): state.last_id}, block=2000)
    for _, entries in response:
        for entry_id, fields in entries:
            if FINAL in fields:
                body = await asyncio.to_thread(gzip.compress, fields[FINAL])
                await queries.save_result(state.pool, state.run_id, body)
                state.worker_done = True
            elif PHASE in fields and ERROR in fields:
                await queries.set_failed(state.pool, state.run_id, cast(Phase, fields[PHASE].decode()), fields[ERROR].decode())
                state.worker_done = True
            # only past an entry once it's handled, so a failed save is read and retried next loop
            state.last_id = entry_id.decode()


async def _poll_runner(state: _RunState) -> None:
    now = time.monotonic()
    state.last_poll = now
    try:
        polled = await runner.status(state.client, state.run_id)
    except RunnerUnavailable as e:
        state.first_failed_poll = state.first_failed_poll or now
        logger.warning("polling the runner for %s failed: %s", state.run_id, e)
        if now - state.first_failed_poll >= RUNNER_MIA_SECONDS:
            await queries.set_failed(state.pool, state.run_id, "runner", "runner unreachable")
            state.runner_done = True
        return

    state.first_failed_poll = None
    if polled.state == "running":
        await queries.advance_status(state.pool, state.run_id, "running")
    elif polled.state == "finished":
        state.runner_done = True
        run = await queries.get_run(state.pool, state.run_id)
        if run is not None and not run.stream:
            await queries.advance_status(state.pool, state.run_id, "clustering")
    elif polled.state == "failed":
        state.runner_done = True
        await queries.set_failed(state.pool, state.run_id, "runner", polled.error or "runner failed")
    elif polled.state == "cancelled":
        state.runner_done = True
        await queries.set_cancelled(state.pool, state.run_id)
    elif polled.state == "unknown":
        state.runner_done = True
        await queries.set_failed(state.pool, state.run_id, "runner", "runner has no record of this run")


# A failed or cancelled run still needs both sides to stop before its streams can expire.
# The worker is told through an abort entry (it answers with an error entry), the runner through its cancel endpoint.
async def _clean_up_stopped(state: _RunState, message: str) -> None:
    if not state.worker_done and not state.abort_sent:
        await write_abort(state.redis, state.run_id, message)
        state.abort_sent = True
    if not state.runner_done and not state.cancel_sent:
        try:
            await runner.cancel(state.client, state.run_id)
        except RunnerUnavailable as e:
            logger.warning("cancel of %s failed: %s", state.run_id, e)
        state.cancel_sent = True
