import json
import threading
import time
from typing import Any

import pytest
import redis
from archipelago import ClusterStream, Config, cluster_run

from app.worker import __main__ as worker_main
from app.worker import jobs
from tests.conftest import SYNTHETIC_RUN_ID, push_logs, without_elapsed

CONFIG = Config(lsh=True, birch=True, denstream=True)
CLUSTERED = f"run:{SYNTHETIC_RUN_ID}:clustered"


# the synthetic run is short, so let streamed runs go live early and produce real updates
@pytest.fixture(autouse=True)
def short_warm_up(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr("archipelago.stream.WARMUP_ROWS", 50)


def split_logs(entries: list[dict[str, Any]]) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    return [e["data"] for e in entries if e["kind"] == "row"], [e["data"] for e in entries if e["kind"] == "event"]


def stream_id(entry_id: bytes) -> tuple[int, int]:
    ms, seq = entry_id.split(b"-")
    return int(ms), int(seq)


def clustered(client: redis.Redis) -> list[dict[bytes, bytes]]:
    return [fields for _, fields in client.xrange(CLUSTERED)]


def entry(kind: str, data: dict[str, Any], entry_id: str) -> tuple[bytes, dict[bytes, bytes]]:
    return entry_id.encode(), {b"kind": kind.encode(), b"data": json.dumps(data).encode()}


def test_batch_job_result_equals_the_module(sync_redis: redis.Redis, log_entries: list[dict[str, Any]]) -> None:
    push_logs(sync_redis, SYNTHETIC_RUN_ID, log_entries)

    jobs.run_job(sync_redis, SYNTHETIC_RUN_ID, False, CONFIG.model_dump_json().encode())

    [only] = clustered(sync_redis)
    assert without_elapsed(only[b"final"]) == without_elapsed(cluster_run(*split_logs(log_entries), CONFIG).model_dump_json())


def test_streamed_job_result_equals_the_module(sync_redis: redis.Redis, log_entries: list[dict[str, Any]]) -> None:
    push_logs(sync_redis, SYNTHETIC_RUN_ID, log_entries)
    reference = ClusterStream(CONFIG)
    reference.cluster(*split_logs(log_entries))

    jobs.run_job(sync_redis, SYNTHETIC_RUN_ID, True, CONFIG.model_dump_json().encode())

    entries = clustered(sync_redis)
    assert all(b"update" in e and b"covers" in e for e in entries[:-1])
    assert without_elapsed(entries[-1][b"final"]) == without_elapsed(reference.finish().model_dump_json())


# A worker that crashed after its update covering entry N must, on re-claim, rebuild its state
# from the logs up to N without writing anything, then carry on and reach the same final result.
# Paged recovery must give the same result as one read, also when covers lands on a page boundary.
@pytest.mark.parametrize(("read_count", "covered_at"), [(10_000, 225), (100, 225), (100, 199)])
def test_streamed_job_recovers_from_its_last_update(
    sync_redis: redis.Redis, log_entries: list[dict[str, Any]], monkeypatch: pytest.MonkeyPatch, read_count: int, covered_at: int
) -> None:
    monkeypatch.setattr(jobs, "READ_COUNT", read_count)
    ids = push_logs(sync_redis, SYNTHETIC_RUN_ID, log_entries)
    covered = ids[covered_at]
    sync_redis.xadd(CLUSTERED, {"covers": covered, "update": "{}"})
    reference = ClusterStream(CONFIG)
    reference.cluster(*split_logs(log_entries))

    jobs.run_job(sync_redis, SYNTHETIC_RUN_ID, True, CONFIG.model_dump_json().encode())

    entries = clustered(sync_redis)
    assert entries[0] == {b"covers": covered, b"update": b"{}"}
    new_covers = [stream_id(e[b"covers"]) for e in entries[1:-1]]
    assert new_covers and new_covers[0] > stream_id(covered) and new_covers == sorted(set(new_covers))
    assert new_covers[-1] == stream_id(ids[-1])
    assert without_elapsed(entries[-1][b"final"]) == without_elapsed(reference.finish().model_dump_json())


def test_backlog_longer_than_one_read_is_processed_fully_and_in_order(
    sync_redis: redis.Redis, log_entries: list[dict[str, Any]], monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(jobs, "READ_COUNT", 100)
    ids = push_logs(sync_redis, SYNTHETIC_RUN_ID, log_entries)

    read = list(jobs.chunks(sync_redis, SYNTHETIC_RUN_ID, b"0-0", None))

    rows, events = split_logs(log_entries)
    assert [c.full for c in read] == [True] * (len(ids) // 100) + [False] * (len(read) - len(ids) // 100)
    assert [row for c in read for row in c.rows] == rows
    assert [event for c in read for event in c.events] == events
    assert read[-1].ended and read[-1].last_id == ids[-1]


def test_a_full_read_does_not_sleep(sync_redis: redis.Redis, log_entries: list[dict[str, Any]], monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(jobs, "READ_COUNT", 100)
    sleeps: list[float] = []
    monkeypatch.setattr(jobs.time, "sleep", sleeps.append)
    push_logs(sync_redis, SYNTHETIC_RUN_ID, log_entries)
    reference = ClusterStream(CONFIG)
    reference.cluster(*split_logs(log_entries))

    jobs.run_job(sync_redis, SYNTHETIC_RUN_ID, True, CONFIG.model_dump_json().encode())

    entries = clustered(sync_redis)
    assert len(entries) > 2 and sleeps == []
    assert without_elapsed(entries[-1][b"final"]) == without_elapsed(reference.finish().model_dump_json())


def test_finished_job_is_not_redone(sync_redis: redis.Redis, log_entries: list[dict[str, Any]]) -> None:
    push_logs(sync_redis, SYNTHETIC_RUN_ID, log_entries)
    sync_redis.xadd(CLUSTERED, {"phase": "runner", "error": "island 3 crashed"})

    jobs.run_job(sync_redis, SYNTHETIC_RUN_ID, True, CONFIG.model_dump_json().encode())

    assert len(clustered(sync_redis)) == 1


@pytest.mark.parametrize(
    ("terminator", "failure", "ended"),
    [
        (entry("abort", {"error": "runner unreachable"}, "3-0"), "runner unreachable", False),
        (entry("event", {"type": "run_end", "termination_reason": "error", "error": "island 3 crashed"}, "3-0"), "island 3 crashed", False),
        (entry("event", {"type": "run_end", "termination_reason": "error"}, "3-0"), "runner failed", False),
        (
            entry("event", {"type": "run_end", "termination_reason": "time_limit", "error": "time limit of 3600 s"}, "3-0"),
            "time limit of 3600 s",
            False,
        ),
        (entry("event", {"type": "run_end", "termination_reason": "aborted"}, "3-0"), "run cancelled", False),
        (entry("event", {"type": "run_end", "termination_reason": "completed"}, "3-0"), None, True),
    ],
)
def test_first_terminator_wins(terminator: tuple[bytes, dict[bytes, bytes]], failure: str | None, ended: bool) -> None:
    before = [entry("row", {"seq": 1}, "1-0"), entry("event", {"type": "generation_end"}, "2-0")]
    after = [entry("row", {"seq": 2}, "4-0"), entry("abort", {"error": "late"}, "5-0")]

    chunk = jobs.split([*before, terminator, *after])

    assert (chunk.failure, chunk.ended, chunk.last_id) == (failure, ended, b"3-0")
    assert len(chunk.rows) == 1


@pytest.mark.parametrize("stream", [True, False])
def test_job_reports_the_runners_failure(sync_redis: redis.Redis, log_entries: list[dict[str, Any]], stream: bool) -> None:
    push_logs(sync_redis, SYNTHETIC_RUN_ID, log_entries[:200])
    sync_redis.xadd(f"run:{SYNTHETIC_RUN_ID}:runner_logs", {"kind": "abort", "data": json.dumps({"error": "island 3 crashed"})})

    jobs.run_job(sync_redis, SYNTHETIC_RUN_ID, stream, CONFIG.model_dump_json().encode())

    assert clustered(sync_redis)[-1] == {b"phase": b"runner", b"error": b"island 3 crashed"}


def test_job_fails_when_logs_stop(sync_redis: redis.Redis, log_entries: list[dict[str, Any]], monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(jobs, "IDLE_TIMEOUT_SECONDS", 1)
    push_logs(sync_redis, SYNTHETIC_RUN_ID, log_entries[:200])

    jobs.run_job(sync_redis, SYNTHETIC_RUN_ID, False, CONFIG.model_dump_json().encode())

    assert clustered(sync_redis) == [{b"phase": b"runner", b"error": b"no logs for 1 s"}]


def test_clustering_error_is_reported_not_raised(sync_redis: redis.Redis, log_entries: list[dict[str, Any]]) -> None:
    without_run_start = [e for e in log_entries if e["data"].get("type") != "run_start"]
    push_logs(sync_redis, SYNTHETIC_RUN_ID, without_run_start)

    jobs.run_job(sync_redis, SYNTHETIC_RUN_ID, False, CONFIG.model_dump_json().encode())

    [error] = clustered(sync_redis)
    assert error[b"phase"] == b"clustering"


def enqueue(client: redis.Redis) -> bytes:
    client.xgroup_create("pending_runs", "workers", id="0", mkstream=True)
    return client.xadd("pending_runs", {"run_id": SYNTHETIC_RUN_ID, "stream": "false", "config": CONFIG.model_dump_json()})


def test_finished_job_is_acked_and_deleted(sync_redis: redis.Redis, log_entries: list[dict[str, Any]]) -> None:
    push_logs(sync_redis, SYNTHETIC_RUN_ID, log_entries)
    enqueue(sync_redis)

    worker_main.process_next(sync_redis, "w1")

    assert b"final" in clustered(sync_redis)[-1]
    assert sync_redis.xlen("pending_runs") == 0
    assert sync_redis.xpending("pending_runs", "workers")["pending"] == 0


def test_job_that_raised_stays_pending_and_is_reclaimed(
    sync_redis: redis.Redis, log_entries: list[dict[str, Any]], monkeypatch: pytest.MonkeyPatch
) -> None:
    push_logs(sync_redis, SYNTHETIC_RUN_ID, log_entries)
    enqueue(sync_redis)
    monkeypatch.setattr(worker_main, "CLAIM_IDLE_MS", 0)
    run_job = worker_main.run_job

    def crash(*args: Any) -> None:
        raise RuntimeError("worker crashed")

    monkeypatch.setattr(worker_main, "run_job", crash)
    with pytest.raises(RuntimeError):
        worker_main.process_next(sync_redis, "w1")

    assert sync_redis.xlen("pending_runs") == 1
    assert sync_redis.xpending("pending_runs", "workers")["pending"] == 1

    monkeypatch.setattr(worker_main, "run_job", run_job)
    worker_main.process_next(sync_redis, "w2")

    assert b"final" in clustered(sync_redis)[-1]
    assert sync_redis.xlen("pending_runs") == 0


# A job that kills its worker every time, e.g. out of memory, must not be re-claimed forever.
def test_job_over_the_delivery_limit_gives_up(sync_redis: redis.Redis, monkeypatch: pytest.MonkeyPatch) -> None:
    job_id = enqueue(sync_redis)
    sync_redis.xreadgroup("workers", "w1", {"pending_runs": ">"}, count=1)
    for _ in range(worker_main.MAX_DELIVERIES - 1):
        sync_redis.xclaim("pending_runs", "workers", "w1", 0, [job_id])
    monkeypatch.setattr(worker_main, "CLAIM_IDLE_MS", 0)
    monkeypatch.setattr(worker_main, "run_job", lambda *args: pytest.fail("ran a job over the limit"))

    worker_main.process_next(sync_redis, "w2")

    assert clustered(sync_redis) == [{b"phase": b"internal", b"error": b"failed after 3 attempts"}]
    assert sync_redis.xlen("pending_runs") == 0
    assert sync_redis.xpending("pending_runs", "workers")["pending"] == 0


# A plain XCLAIM adds a delivery on every beat, which would make a crashed job look like it had
# already failed 3 times and skip recovery.
def test_heartbeat_keeps_the_delivery_count_at_one(sync_redis: redis.Redis, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(worker_main, "HEARTBEAT_SECONDS", 0.05)
    sync_redis.xgroup_create("pending_runs", "workers", id="0", mkstream=True)
    job_id = sync_redis.xadd("pending_runs", {"run_id": SYNTHETIC_RUN_ID, "stream": "true", "config": "{}"})
    sync_redis.xreadgroup("workers", "w1", {"pending_runs": ">"}, count=1)
    stop = threading.Event()
    beat = threading.Thread(target=worker_main.heartbeat, args=(sync_redis, "w1", job_id, stop))

    beat.start()
    time.sleep(0.5)
    stop.set()
    beat.join()

    [pending] = sync_redis.xpending_range("pending_runs", "workers", "-", "+", 10)
    assert pending["times_delivered"] == 1
    assert pending["time_since_delivered"] < 200
