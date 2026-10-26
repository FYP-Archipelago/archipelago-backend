import gzip
import json
from typing import Any

import httpx
import pytest
import redis
from archipelago import Config

from app.db import queries
from app.main import app
from app.models import CreateRun
from tests.conftest import FakeRunner

pytestmark = pytest.mark.anyio

BODY: dict[str, Any] = {"preset": "de-rastrigin", "overrides": {"num_islands": 4}, "stream": True, "clustering": {"lsh": True}}
RUN_ID = "run-20261027T000000Z-0123abcd"


def parse_sse(text: str) -> list[tuple[str, str | None, Any]]:
    events = []
    for block in text.strip().split("\n\n"):
        lines = dict(line.split(": ", 1) for line in block.splitlines() if not line.startswith(":"))
        if lines:
            events.append((lines["event"], lines.get("id"), json.loads(lines["data"])))
    return events


async def seed_run(status: str = "queued", stream: bool = True) -> None:
    await queries.insert_run(app.state.pool, RUN_ID, CreateRun.model_validate({**BODY, "stream": stream}))
    if status == "failed":
        await queries.set_failed(app.state.pool, RUN_ID, "clustering", "boom")
    elif status != "queued":
        await queries.advance_status(app.state.pool, RUN_ID, status)


async def test_create_run_enqueues_the_job_before_starting_the_runner(api: httpx.AsyncClient, runner: FakeRunner, sync_redis: redis.Redis) -> None:
    jobs_at_start: list[int] = []
    runner.on_start = lambda: jobs_at_start.append(sync_redis.xlen("pending_runs"))

    r = await api.post("/runs", json=BODY)

    assert r.status_code == 202
    run = r.json()
    assert run["status"] == "running"
    assert run["clustering"] == Config(lsh=True).model_dump(mode="json")
    assert jobs_at_start == [1]
    [(_, job)] = sync_redis.xrange("pending_runs")
    assert job[b"run_id"].decode() == run["run_id"]
    assert job[b"stream"] == b"true"
    assert Config.model_validate_json(job[b"config"]) == Config(lsh=True)


async def test_runner_rejection_fails_the_run_and_aborts_the_job(api: httpx.AsyncClient, runner: FakeRunner, sync_redis: redis.Redis) -> None:
    runner.start = httpx.Response(422, json={"error": "unknown preset 'de-rastrigin'"})

    r = await api.post("/runs", json=BODY)

    assert r.status_code == 422
    assert r.json() == {"detail": "unknown preset 'de-rastrigin'"}
    [(_, job)] = sync_redis.xrange("pending_runs")
    run_id = job[b"run_id"].decode()
    run = await queries.get_run(app.state.pool, run_id)
    assert run is not None and run.status == "failed"
    assert run.error is not None and (run.error.phase, run.error.message) == ("runner", "unknown preset 'de-rastrigin'")
    [(_, abort)] = sync_redis.xrange(f"run:{run_id}:runner_logs")
    assert abort[b"kind"] == b"abort"
    assert json.loads(abort[b"data"]) == {"error": "unknown preset 'de-rastrigin'"}


async def test_unreachable_runner_gives_502_and_fails_the_run(api: httpx.AsyncClient, runner: FakeRunner, sync_redis: redis.Redis) -> None:
    runner.start = httpx.ConnectError("connection refused")

    r = await api.post("/runs", json=BODY)

    assert r.status_code == 502
    [(_, job)] = sync_redis.xrange("pending_runs")
    run = await queries.get_run(app.state.pool, job[b"run_id"].decode())
    assert run is not None and run.status == "failed" and run.error is not None and run.error.phase == "runner"


@pytest.mark.parametrize(
    "body",
    [
        {**BODY, "clustering": {}},
        {**BODY, "clustering": {"lsh": True, "detail": 3.0}},
        {**BODY, "unknown": 1},
        {k: v for k, v in BODY.items() if k != "clustering"},
    ],
)
async def test_invalid_body_is_rejected_before_anything_happens(
    api: httpx.AsyncClient, runner: FakeRunner, sync_redis: redis.Redis, body: dict[str, Any]
) -> None:
    r = await api.post("/runs", json=body)

    assert r.status_code == 422
    assert sync_redis.xlen("pending_runs") == 0
    assert runner.calls == []


async def test_presets_are_forwarded_byte_for_byte(api: httpx.AsyncClient, runner: FakeRunner) -> None:
    body = b'{"overrides": {"seed": {"type": "int"}}, "presets": [{"preset": "x"}]}'
    runner.presets = httpx.Response(200, content=body)

    r = await api.get("/presets")

    assert r.status_code == 200
    assert r.content == body


async def test_presets_give_502_when_the_runner_fails(api: httpx.AsyncClient, runner: FakeRunner) -> None:
    runner.presets = httpx.Response(500)

    assert (await api.get("/presets")).status_code == 502


@pytest.mark.parametrize("path", ["/runs/not-a-run/result", f"/runs/{RUN_ID}/result", "/runs/not-a-run/events", f"/runs/{RUN_ID}/events"])
async def test_malformed_and_unknown_runs_are_404(api: httpx.AsyncClient, path: str) -> None:
    r = await api.get(path)

    assert r.status_code == 404
    assert r.json() == {"detail": "run not found"}


@pytest.mark.parametrize(("status", "detail"), [("running", "no result: run is running"), ("failed", "no result: run failed")])
async def test_result_is_404_until_the_run_is_done(api: httpx.AsyncClient, status: str, detail: str) -> None:
    await seed_run(status)

    r = await api.get(f"/runs/{RUN_ID}/result")

    assert r.status_code == 404
    assert r.json() == {"detail": detail}


@pytest.mark.parametrize(("accept", "encoding"), [("gzip", "gzip"), ("identity", None)])
async def test_result_is_served_as_stored(api: httpx.AsyncClient, accept: str, encoding: str | None) -> None:
    final = b'{"graph": {"nodes": [1, 2, 3]}}'
    await seed_run()
    await queries.save_result(app.state.pool, RUN_ID, gzip.compress(final))

    r = await api.get(f"/runs/{RUN_ID}/result", headers={"Accept-Encoding": accept})

    assert r.status_code == 200
    assert r.headers.get("content-encoding") == encoding
    assert r.content == final


async def test_events_replay_the_clustered_stream_and_close_after_final(api: httpx.AsyncClient, sync_redis: redis.Redis) -> None:
    await seed_run()
    key = f"run:{RUN_ID}:clustered"
    first = sync_redis.xadd(key, {"covers": "1-0", "update": '{"n": 1}'}).decode()
    second = sync_redis.xadd(key, {"covers": "2-0", "update": '{"n": 2}'}).decode()
    final = sync_redis.xadd(key, {"final": '{"n": 3}'}).decode()

    events = parse_sse((await api.get(f"/runs/{RUN_ID}/events")).text)
    resumed = parse_sse((await api.get(f"/runs/{RUN_ID}/events", headers={"Last-Event-ID": first})).text)

    assert events == [("status", None, {"status": "queued"}), ("update", first, {"n": 1}), ("update", second, {"n": 2}), ("final", final, {"n": 3})]
    assert resumed == [("status", None, {"status": "queued"}), ("update", second, {"n": 2}), ("final", final, {"n": 3})]


async def test_events_end_with_the_error_of_a_failed_run(api: httpx.AsyncClient, sync_redis: redis.Redis) -> None:
    await seed_run()
    sync_redis.xadd(f"run:{RUN_ID}:clustered", {"phase": "clustering", "error": "bad genome"})

    events = parse_sse((await api.get(f"/runs/{RUN_ID}/events")).text)

    assert events[-1] == ("error", None, {"phase": "clustering", "message": "bad genome"})


async def test_events_of_a_done_run_with_expired_streams_say_done(api: httpx.AsyncClient) -> None:
    await seed_run()
    await queries.save_result(app.state.pool, RUN_ID, gzip.compress(b"{}"))

    events = parse_sse((await api.get(f"/runs/{RUN_ID}/events")).text)

    assert events == [("status", None, {"status": "done"})]


async def test_statuses_only_move_forward(api: httpx.AsyncClient) -> None:
    await seed_run()
    await queries.set_failed(app.state.pool, RUN_ID, "runner", "island crashed")
    await queries.advance_status(app.state.pool, RUN_ID, "done")
    await queries.save_result(app.state.pool, RUN_ID, gzip.compress(b"{}"))

    run = await queries.get_run(app.state.pool, RUN_ID)
    assert run is not None and run.status == "failed"
    assert await queries.get_result_body(app.state.pool, RUN_ID) is None


async def test_health(api: httpx.AsyncClient) -> None:
    r = await api.get("/health")

    assert r.status_code == 200
    assert r.json() == {"status": "ok", "version": "0.2.0", "db": True, "redis": True}
