import asyncio
import gzip
import json
from typing import Any

import httpx
import psycopg
import pytest
import redis
from archipelago import Config

from app import runner as runner_client
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
    elif status == "cancelled":
        await queries.set_cancelled(app.state.pool, RUN_ID)
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


async def test_run_history_cursor_handles_tied_timestamps_and_new_runs(api: httpx.AsyncClient) -> None:
    ids = ["run-20261027T000000Z-00000001", "run-20261027T000000Z-00000002", "run-20261027T000000Z-00000003"]
    for run_id in ids:
        await queries.insert_run(app.state.pool, run_id, CreateRun.model_validate(BODY))
    async with app.state.pool.connection() as conn:
        await conn.execute("UPDATE runs SET created_at = '2026-10-27T00:00:00Z'")

    first = await api.get("/runs?limit=1")
    assert first.status_code == 200
    assert [run["run_id"] for run in first.json()["runs"]] == [ids[2]]
    cursor = first.json()["next_cursor"]
    assert (await api.get("/runs", params={"cursor": cursor + "!!!!"})).status_code == 422
    await queries.insert_run(app.state.pool, "run-20261027T000000Z-00000004", CreateRun.model_validate(BODY))

    second = await api.get("/runs", params={"limit": 1, "cursor": cursor})
    assert [run["run_id"] for run in second.json()["runs"]] == [ids[1]]
    third = await api.get("/runs", params={"limit": 1, "cursor": second.json()["next_cursor"]})
    assert [run["run_id"] for run in third.json()["runs"]] == [ids[0]]
    assert third.json()["next_cursor"] is None


async def test_run_history_filters_metadata_and_empty_page(api: httpx.AsyncClient, runner: FakeRunner) -> None:
    await seed_run("failed")
    await queries.insert_run(app.state.pool, "run-20261027T000000Z-00000001", CreateRun.model_validate({**BODY, "preset": "other"}))
    await queries.insert_run(app.state.pool, "run-20261027T000000Z-00000002", CreateRun.model_validate(BODY))
    await queries.advance_status(app.state.pool, "run-20261027T000000Z-00000002", "running")
    response = await api.get("/runs", params={"status": "failed", "preset": "de-rastrigin", "limit": 1})
    assert response.status_code == 200
    [run] = response.json()["runs"]
    assert run["run_id"] == RUN_ID and run["status"] == "failed"
    assert run["preset"] == BODY["preset"] and run["overrides"] == BODY["overrides"]
    assert run["stream"] is True and run["clustering"] == Config(lsh=True).model_dump(mode="json")
    assert run["error"] == {"phase": "clustering", "message": "boom"}
    assert runner.calls == []
    empty = await api.get("/runs", params={"preset": "removed-preset"})
    assert empty.json() == {"runs": [], "next_cursor": None}


@pytest.mark.parametrize("limit", [0, 101])
async def test_run_history_rejects_invalid_page_sizes(api: httpx.AsyncClient, limit: int) -> None:
    assert (await api.get("/runs", params={"limit": limit})).status_code == 422


# What a Redis restart does to the API's idle connections.
async def test_create_run_survives_a_dead_pooled_redis_connection(api: httpx.AsyncClient, sync_redis: redis.Redis) -> None:
    assert (await api.get("/health")).status_code == 200
    assert sync_redis.client_kill_filter(_type="normal", skipme=True) >= 1

    r = await api.post("/runs", json=BODY)

    assert r.status_code == 202
    assert sync_redis.xlen("pending_runs") == 1


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


async def test_a_failed_start_shows_the_runners_error_text(api: httpx.AsyncClient, runner: FakeRunner) -> None:
    runner.start = httpx.Response(503, json={"error": "runner is shutting down"})

    r = await api.post("/runs", json=BODY)

    assert r.status_code == 502
    assert r.json() == {"detail": "runner answered 503 to the start: runner is shutting down"}


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


async def test_events_give_the_error_not_a_final_for_a_run_that_already_failed(api: httpx.AsyncClient, sync_redis: redis.Redis) -> None:
    await seed_run("failed")
    sync_redis.xadd(f"run:{RUN_ID}:clustered", {"final": '{"n": 3}'})

    events = parse_sse((await api.get(f"/runs/{RUN_ID}/events")).text)

    assert events == [("status", None, {"status": "failed"}), ("error", None, {"phase": "clustering", "message": "boom"})]


async def test_events_of_a_done_run_with_expired_streams_say_done(api: httpx.AsyncClient) -> None:
    await seed_run()
    await queries.save_result(app.state.pool, RUN_ID, gzip.compress(b"{}"))

    events = parse_sse((await api.get(f"/runs/{RUN_ID}/events")).text)

    assert events == [("status", None, {"status": "done"})]


@pytest.mark.parametrize("path", ["/runs/nope/cancel", "/runs/run-20261027T000000Z-00000000/cancel"])
async def test_cancel_of_a_malformed_or_unknown_run_is_404(api: httpx.AsyncClient, runner: FakeRunner, path: str) -> None:
    r = await api.post(path)

    assert r.status_code == 404
    assert r.json() == {"detail": "run not found"}
    assert runner.calls == []


@pytest.mark.parametrize("status", ["done", "failed", "cancelled"])
async def test_cancel_of_a_final_run_is_a_no_op_without_the_runner(api: httpx.AsyncClient, runner: FakeRunner, status: str) -> None:
    await seed_run(status)

    r = await api.post(f"/runs/{RUN_ID}/cancel")

    assert r.status_code == 200
    assert r.json()["status"] == status
    assert runner.calls == []


@pytest.mark.parametrize(
    ("status", "answer"),
    [
        ("queued", httpx.Response(200, json={"state": "cancelled"})),
        ("running", httpx.Response(202, json={"state": "running"})),
        ("running", httpx.Response(200, json={"state": "cancelled"})),
        ("running", httpx.Response(200, json={"state": "finished"})),
    ],
)
async def test_cancel_accepted_by_the_runner_sets_cancelled(api: httpx.AsyncClient, runner: FakeRunner, status: str, answer: httpx.Response) -> None:
    await seed_run(status)
    runner.cancel = answer

    r = await api.post(f"/runs/{RUN_ID}/cancel")

    assert r.status_code == 202
    assert r.json()["status"] == "cancelled" and r.json()["error"] is None
    assert runner.calls == [f"POST /runs/{RUN_ID}/cancel"]
    run = await queries.get_run(app.state.pool, RUN_ID)
    assert run is not None and run.status == "cancelled" and run.error is None


# Only the worker is left in clustering, so a runner that can't be reached doesn't matter.
async def test_cancel_while_clustering_needs_no_runner(api: httpx.AsyncClient, runner: FakeRunner) -> None:
    await seed_run("clustering", stream=False)
    runner.cancel = httpx.ConnectError("refused")

    r = await api.post(f"/runs/{RUN_ID}/cancel")

    assert r.status_code == 202
    assert r.json()["status"] == "cancelled"
    assert runner.calls == []


@pytest.mark.parametrize("answer", [httpx.Response(200, json={"state": "failed"}), httpx.Response(404)])
async def test_cancel_of_a_run_the_runner_lost_leaves_the_status_to_the_supervisor(
    api: httpx.AsyncClient, runner: FakeRunner, answer: httpx.Response
) -> None:
    await seed_run("running")
    runner.cancel = answer

    r = await api.post(f"/runs/{RUN_ID}/cancel")

    assert r.status_code == 200
    assert r.json()["status"] == "running"


# The result can be saved between the runner's answer and set_cancelled, and then the run stays done.
async def test_cancel_that_loses_to_the_result_returns_the_done_run(
    api: httpx.AsyncClient, runner: FakeRunner, monkeypatch: pytest.MonkeyPatch
) -> None:
    await seed_run("running")
    runner.cancel = httpx.Response(200, json={"state": "finished"})

    async def result_saved_first(pool: object, run_id: str) -> None:
        await queries.save_result(app.state.pool, run_id, b"result")

    monkeypatch.setattr(queries, "set_cancelled", result_saved_first)

    r = await api.post(f"/runs/{RUN_ID}/cancel")

    assert r.status_code == 200
    assert r.json()["status"] == "done"


@pytest.mark.parametrize("answer", [httpx.ConnectError("refused"), httpx.Response(500, text="boom"), httpx.Response(202, text="not json")])
async def test_cancel_the_runner_did_not_accept_gives_502_and_keeps_the_status(
    api: httpx.AsyncClient, runner: FakeRunner, answer: httpx.Response | Exception
) -> None:
    await seed_run("running")
    runner.cancel = answer

    r = await api.post(f"/runs/{RUN_ID}/cancel")

    assert r.status_code == 502
    run = await queries.get_run(app.state.pool, RUN_ID)
    assert run is not None and run.status == "running"


@pytest.mark.parametrize("entry", [{"phase": "runner", "error": "run cancelled"}, {"final": '{"n": 3}'}, {}])
async def test_events_of_a_cancelled_run_end_with_the_status_only(api: httpx.AsyncClient, sync_redis: redis.Redis, entry: dict[str, str]) -> None:
    await seed_run("cancelled")
    if entry:
        sync_redis.xadd(f"run:{RUN_ID}:clustered", entry)

    events = parse_sse((await api.get(f"/runs/{RUN_ID}/events")).text)

    assert events == [("status", None, {"status": "cancelled"})]


async def test_result_of_a_cancelled_run_is_404(api: httpx.AsyncClient) -> None:
    await seed_run("cancelled")

    r = await api.get(f"/runs/{RUN_ID}/result")

    assert r.status_code == 404
    assert r.json() == {"detail": "no result: run cancelled"}


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
    body = r.json()
    for field in ("mood", "message"):
        value = body.pop(field)
        assert isinstance(value, str) and value
    assert body == {"status": "ok", "version": "0.2.0", "db": True, "redis": True, "runner": True, "ready": True}


@pytest.mark.parametrize(
    "response",
    [httpx.Response(503, json={"status": "unavailable"}), httpx.ConnectError("runner offline"), httpx.Response(200, text="not JSON")],
)
async def test_unhealthy_runner_does_not_make_the_backend_unavailable(
    api: httpx.AsyncClient, runner: FakeRunner, response: httpx.Response | Exception
) -> None:
    runner.health = response

    r = await api.get("/health")

    assert r.status_code == 200
    body = r.json()
    for field in ("mood", "message"):
        value = body.pop(field)
        assert isinstance(value, str) and value
    assert body == {"status": "ok", "version": "0.2.0", "db": True, "redis": True, "runner": False, "ready": False}


@pytest.mark.parametrize("dependency", ["db", "redis"])
async def test_health_still_reports_unavailable_for_backend_dependency_failures(
    api: httpx.AsyncClient, monkeypatch: pytest.MonkeyPatch, dependency: str
) -> None:
    def broken_connection() -> None:
        raise psycopg.OperationalError("postgres offline")

    async def broken_ping() -> None:
        raise redis.RedisError("redis offline")

    if dependency == "db":
        monkeypatch.setattr(app.state.pool, "connection", broken_connection)
    else:
        monkeypatch.setattr(app.state.redis, "ping", broken_ping)

    r = await api.get("/health")

    assert r.status_code == 503
    body = r.json()
    assert body["status"] == "unavailable" and body["ready"] is False
    assert body["db"] is (dependency != "db") and body["redis"] is (dependency != "redis")
    assert body["runner"] is True
    assert ("Postgres" if dependency == "db" else "Redis") in body["message"]
    assert isinstance(body["mood"], str) and body["mood"]


async def test_health_does_not_wait_indefinitely_for_the_runner(api: httpx.AsyncClient, monkeypatch: pytest.MonkeyPatch) -> None:
    async def stalled_health(client: httpx.AsyncClient) -> bool:
        await asyncio.Event().wait()
        return True

    monkeypatch.setattr(runner_client, "health", stalled_health)

    r = await asyncio.wait_for(api.get("/health"), timeout=3)

    assert r.status_code == 200
    assert r.json()["runner"] is False and r.json()["ready"] is False
