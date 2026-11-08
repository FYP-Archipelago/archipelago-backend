import gzip
import json
import time
from collections.abc import Callable
from typing import Any

import anyio
import httpx
import pytest
import redis

from app import supervisor
from app.db import runs
from app.main import app
from app.models import CreateRun
from tests.conftest import FakeRunner

pytestmark = pytest.mark.anyio

BODY: dict[str, Any] = {"preset": "de-rastrigin", "overrides": {}, "stream": False, "clustering": {"lsh": True}}


async def create_run(api: httpx.AsyncClient, **body: Any) -> str:
    r = await api.post("/runs", json={**BODY, **body})
    assert r.status_code == 202
    return r.json()["run_id"]


async def wait_for(check: Callable[[], Any], timeout: float = 20) -> None:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if await check():
            return
        await anyio.sleep(0.1)
    raise AssertionError("timed out waiting")


async def status_is(run_id: str, status: str) -> bool:
    run = await runs.get_run(app.state.pool, run_id)
    return run is not None and run.status == status


async def streams_expired(run_id: str) -> bool:
    return run_id not in await runs.list_unexpired(app.state.pool)


def finish_job(client: redis.Redis, run_id: str, **fields: str) -> None:
    client.xadd(f"run:{run_id}:clustered", fields)
    for job_id, job in client.xrange("pending_runs"):
        if job[b"run_id"] == run_id.encode():
            client.xdel("pending_runs", job_id)


async def test_batch_run_goes_through_clustering_to_done(api: httpx.AsyncClient, runner: FakeRunner, sync_redis: redis.Redis) -> None:
    run_id = await create_run(api)
    runner.state = "finished"
    await wait_for(lambda: status_is(run_id, "clustering"))
    final = b'{"graph": {"nodes": [7]}}'

    finish_job(sync_redis, run_id, final=final.decode())

    await wait_for(lambda: streams_expired(run_id))
    assert await status_is(run_id, "done")
    body = await runs.get_result_body(app.state.pool, run_id)
    assert body is not None and gzip.decompress(body) == final
    assert 0 < sync_redis.ttl(f"run:{run_id}:clustered") <= supervisor.STREAM_TTL_SECONDS


async def test_a_failed_result_save_is_retried_not_lost(
    api: httpx.AsyncClient, runner: FakeRunner, sync_redis: redis.Redis, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(supervisor, "ERROR_RETRY_SECONDS", 0.05)
    save_result = runs.save_result
    saves = 0

    async def flaky_save(*args: Any) -> None:
        nonlocal saves
        saves += 1
        if saves == 1:
            raise RuntimeError("postgres went away")
        await save_result(*args)

    monkeypatch.setattr(runs, "save_result", flaky_save)
    run_id = await create_run(api)
    runner.state = "finished"

    finish_job(sync_redis, run_id, final='{"graph": {}}')

    await wait_for(lambda: streams_expired(run_id))
    assert saves == 2
    assert await status_is(run_id, "done")
    body = await runs.get_result_body(app.state.pool, run_id)
    assert body is not None and gzip.decompress(body) == b'{"graph": {}}'


# The TTL must wait for the runner too: EXPIRE deletes the key, and a later XADD from a runner
# that is still going would recreate runner_logs with no TTL at all.
async def test_streams_expire_only_after_the_runner_stopped(api: httpx.AsyncClient, runner: FakeRunner, sync_redis: redis.Redis) -> None:
    run_id = await create_run(api, stream=True)

    finish_job(sync_redis, run_id, phase="clustering", error="bad genome")
    await wait_for(lambda: status_is(run_id, "failed"))
    await anyio.sleep(3)

    assert not await streams_expired(run_id)
    assert runner.calls.count(f"POST /runs/{run_id}/cancel") == 1
    runner.state, runner.error = "cancelled", None
    await wait_for(lambda: streams_expired(run_id))
    run = await runs.get_run(app.state.pool, run_id)
    assert run is not None and run.status == "failed"
    assert run.error is not None and (run.error.phase, run.error.message) == ("clustering", "bad genome")


# Batch cancellation waits for the worker to release its job, even after it writes a final.
async def test_cancelling_batch_waits_for_worker_release_and_discards_its_final(
    api: httpx.AsyncClient, runner: FakeRunner, sync_redis: redis.Redis, monkeypatch: pytest.MonkeyPatch
) -> None:
    run_id = await create_run(api)
    job_id = sync_redis.xreadgroup("workers", "worker-1", {"pending_runs": ">"}, count=1)[0][1][0][0]
    runner.state = "finished"
    await wait_for(lambda: status_is(run_id, "clustering"))

    response = await api.post(f"/runs/{run_id}/cancel")
    assert response.status_code == 202 and response.json()["status"] == "cancelling"
    await wait_for(lambda: app.state.redis.xlen(f"run:{run_id}:runner_logs"))
    [(_, abort)] = sync_redis.xrange(f"run:{run_id}:runner_logs")
    assert abort[b"kind"] == b"abort"

    waiting = anyio.Event()
    read_clustered = supervisor._read_clustered

    async def observe_worker(state: supervisor._RunState) -> None:
        if state.run_id == run_id and state.worker_done:
            waiting.set()
        await read_clustered(state)

    monkeypatch.setattr(supervisor, "_read_clustered", observe_worker)
    sync_redis.xadd(f"run:{run_id}:clustered", {"final": '{"graph": {}}'})
    with anyio.fail_after(10):
        await waiting.wait()
    assert await status_is(run_id, "cancelling") and not await streams_expired(run_id)
    assert await runs.get_result_body(app.state.pool, run_id) is None

    sync_redis.xack("pending_runs", "workers", job_id)
    sync_redis.xdel("pending_runs", job_id)
    await wait_for(lambda: streams_expired(run_id))
    assert await status_is(run_id, "cancelled")
    assert await runs.get_result_body(app.state.pool, run_id) is None
    assert f"POST /runs/{run_id}/cancel" not in runner.calls


async def test_cancellation_survives_restart_and_retries_an_unreachable_runner(
    running_app: Callable[[], Any], runner: FakeRunner, sync_redis: redis.Redis, monkeypatch: pytest.MonkeyPatch
) -> None:
    run_id = "run-20261027T000000Z-c4c4c4c4"
    monkeypatch.setattr(supervisor, "RUNNER_MIA_SECONDS", 0)
    async with running_app():
        await runs.insert_run(app.state.pool, run_id, CreateRun.model_validate(BODY))
        sync_redis.xadd("pending_runs", {"run_id": run_id, "stream": "false", "config": "{}"})
        await runs.set_cancelling(app.state.pool, run_id)

    runner.status = httpx.ConnectError("runner offline")
    runner.cancel = httpx.ConnectError("runner offline")
    async with running_app():
        with anyio.fail_after(15):
            while runner.calls.count(f"POST /runs/{run_id}/cancel") < 2:
                await anyio.sleep(0.1)
        finish_job(sync_redis, run_id, phase="runner", error="run cancelled")
        with anyio.fail_after(10):
            while runner.calls.count(f"POST /runs/{run_id}/cancel") < 3:
                await anyio.sleep(0.1)
        assert await status_is(run_id, "cancelling") and not await streams_expired(run_id)
        attempts = runner.calls.count(f"POST /runs/{run_id}/cancel")
        runner.status = None
        runner.cancel = httpx.Response(202, json={"state": "running"})
        with anyio.fail_after(10):
            while runner.calls.count(f"POST /runs/{run_id}/cancel") < attempts + 2:
                await anyio.sleep(0.1)
        assert await status_is(run_id, "cancelling")
        runner.state = "cancelled"
        await wait_for(lambda: streams_expired(run_id))
        assert await status_is(run_id, "cancelled")


async def test_orphaned_saved_run_without_a_queued_job_is_cleaned_up(running_app: Callable[[], Any], runner: FakeRunner) -> None:
    run_id = "run-20261027T000000Z-a1a1a1a1"
    runner.status = httpx.Response(404)

    async with running_app():
        await runs.insert_run(app.state.pool, run_id, CreateRun.model_validate(BODY))

    async with running_app():
        await wait_for(lambda: streams_expired(run_id))
        run = await runs.get_run(app.state.pool, run_id)
        assert run is not None and run.status == "failed"
        assert await app.state.redis.xlen(f"run:{run_id}:runner_logs") == 0


@pytest.mark.parametrize("status", ["failed", "cancelling"])
async def test_stopped_run_with_lost_terminal_entry_is_cleaned_after_restart(
    running_app: Callable[[], Any], runner: FakeRunner, sync_redis: redis.Redis, status: str
) -> None:
    run_id = "run-20261027T000000Z-a1a1a1a1"
    runner.state = "finished"

    async with running_app():
        await runs.insert_run(app.state.pool, run_id, CreateRun.model_validate(BODY))
        if status == "failed":
            await runs.set_failed(app.state.pool, run_id, "runner", "runner failed")
        else:
            await runs.set_cancelling(app.state.pool, run_id)
        sync_redis.xadd("pending_runs", {"run_id": run_id, "stream": "false", "config": "{}"})
        sync_redis.xadd(f"run:{run_id}:clustered", {"phase": "clustering", "error": "terminal response"})
        for job_id, job in sync_redis.xrange("pending_runs"):
            if job[b"run_id"] == run_id.encode():
                sync_redis.xdel("pending_runs", job_id)
        sync_redis.delete(f"run:{run_id}:clustered")

    async with running_app():
        await wait_for(lambda: streams_expired(run_id))
        assert await status_is(run_id, "failed" if status == "failed" else "cancelled")
        assert await app.state.redis.xlen(f"run:{run_id}:runner_logs") == 0


@pytest.mark.parametrize("claimed", [False, True])
async def test_pending_job_prevents_early_cleanup_until_worker_finishes(
    running_app: Callable[[], Any], runner: FakeRunner, sync_redis: redis.Redis, claimed: bool
) -> None:
    run_id = "run-20261027T000000Z-b2b2b2b2"
    runner.state = "finished"

    async with running_app():
        await runs.insert_run(app.state.pool, run_id, CreateRun.model_validate(BODY))
        await runs.set_failed(app.state.pool, run_id, "runner", "runner failed")
        sync_redis.xadd("pending_runs", {"run_id": run_id, "stream": "false", "config": "{}"})
        if claimed:
            sync_redis.xreadgroup("workers", "worker-1", {"pending_runs": ">"}, count=1)

    async with running_app():
        await wait_for(lambda: anyio.to_thread.run_sync(lambda: sync_redis.xlen(f"run:{run_id}:runner_logs") > 0))
        assert not await streams_expired(run_id)
        [(_, abort)] = sync_redis.xrange(f"run:{run_id}:runner_logs")
        assert abort[b"kind"] == b"abort"

        finish_job(sync_redis, run_id, phase="runner", error="runner failed")
        await wait_for(lambda: streams_expired(run_id))


async def test_pending_job_scan_finds_a_match_on_a_later_page(api: httpx.AsyncClient, sync_redis: redis.Redis) -> None:
    for index in range(supervisor.PENDING_SCAN_PAGE_SIZE):
        sync_redis.xadd("pending_runs", {"run_id": f"other-{index}", "stream": "false", "config": "{}"})
    run_id = "run-on-later-page"
    sync_redis.xadd("pending_runs", {"run_id": run_id, "stream": "false", "config": "{}"})

    assert await supervisor._has_worker_job(app.state.redis, run_id)


async def test_pending_job_scan_propagates_redis_errors(api: httpx.AsyncClient, monkeypatch: pytest.MonkeyPatch) -> None:
    async def broken_xrange(*args: Any, **kwargs: Any) -> list[Any]:
        raise redis.RedisError("redis unavailable")

    monkeypatch.setattr(app.state.redis, "xrange", broken_xrange)
    with pytest.raises(redis.RedisError, match="redis unavailable"):
        await supervisor._has_worker_job(app.state.redis, "run-with-unknown-job-state")


async def test_runner_failure_aborts_the_job_with_its_reason(api: httpx.AsyncClient, runner: FakeRunner, sync_redis: redis.Redis) -> None:
    run_id = await create_run(api)

    runner.state, runner.error = "failed", "island 3 crashed"

    await wait_for(lambda: status_is(run_id, "failed"))
    await wait_for(lambda: anyio.to_thread.run_sync(lambda: sync_redis.xlen(f"run:{run_id}:runner_logs") > 0))
    [(_, abort)] = sync_redis.xrange(f"run:{run_id}:runner_logs")
    assert abort[b"kind"] == b"abort" and json.loads(abort[b"data"]) == {"error": "island 3 crashed"}


@pytest.mark.parametrize(
    ("status", "message"),
    [(httpx.Response(404), "runner has no record of this run"), (httpx.ConnectError("refused"), "runner unreachable")],
)
async def test_vanished_or_unreachable_runner_fails_the_run(
    api: httpx.AsyncClient, runner: FakeRunner, monkeypatch: pytest.MonkeyPatch, status: httpx.Response | Exception, message: str
) -> None:
    monkeypatch.setattr(supervisor, "RUNNER_MIA_SECONDS", 0)
    run_id = await create_run(api)

    runner.status = status

    await wait_for(lambda: status_is(run_id, "failed"))
    run = await runs.get_run(app.state.pool, run_id)
    assert run is not None and run.error is not None and (run.error.phase, run.error.message) == ("runner", message)


@pytest.mark.parametrize(("errors", "status"), [(supervisor.MAX_ERRORS - 1, "running"), (100, "failed")])
async def test_supervisor_errors_fail_the_run_only_after_several_in_a_row(
    api: httpx.AsyncClient, runner: FakeRunner, monkeypatch: pytest.MonkeyPatch, errors: int, status: str
) -> None:
    monkeypatch.setattr(supervisor, "ERROR_RETRY_SECONDS", 0.05)
    monkeypatch.setattr(supervisor, "POLL_SECONDS", 0)
    run_id = await create_run(api)
    polls = 0

    def flaky() -> httpx.Response:
        nonlocal polls
        polls += 1
        if polls <= errors:
            raise RuntimeError("redis went away")
        return httpx.Response(200, json={"state": "running", "error": None})

    async def polled_past_the_limit() -> bool:
        return polls > supervisor.MAX_ERRORS

    runner.status = flaky
    await wait_for(polled_past_the_limit)
    await wait_for(lambda: status_is(run_id, status))
    if status == "failed":
        run = await runs.get_run(app.state.pool, run_id)
        assert run is not None and run.error is not None and run.error.phase == "internal"


async def test_done_run_whose_streams_expired_is_finished_after_a_restart(running_app: Callable[[], Any], runner: FakeRunner) -> None:
    run_id = "run-20261027T000000Z-d0d0d0d0"
    async with running_app():
        await runs.insert_run(app.state.pool, run_id, CreateRun.model_validate(BODY))
        await runs.save_result(app.state.pool, run_id, gzip.compress(b"{}"))

    runner.state = "finished"

    async with running_app():
        await wait_for(lambda: streams_expired(run_id))
        assert await status_is(run_id, "done")


async def test_api_restart_reattaches_unfinished_runs(running_app: Callable[[], Any], runner: FakeRunner, sync_redis: redis.Redis) -> None:
    async with running_app(), httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://api") as api:
        run_id = await create_run(api)

    runner.state = "finished"
    finish_job(sync_redis, run_id, final="{}")

    async with running_app():
        await wait_for(lambda: streams_expired(run_id))
        assert await status_is(run_id, "done")
