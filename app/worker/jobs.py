import json
import logging
import time
from collections.abc import Iterator
from dataclasses import dataclass, field
from typing import Any

import redis
from archipelago import ClusteringError, ClusterStream, Config, cluster_run

from app.redis_names import ABORT, COVERS, DATA, ERROR, EVENT, FINAL, KIND, PHASE, ROW, UPDATE, clustered_key, runner_logs_key

UPDATE_INTERVAL_SECONDS = 1
IDLE_TIMEOUT_SECONDS = 900

log = logging.getLogger(__name__)

type Entry = tuple[bytes, dict[bytes, bytes]]


@dataclass
class Chunk:
    rows: list[dict[str, Any]] = field(default_factory=list)
    events: list[dict[str, Any]] = field(default_factory=list)
    last_id: bytes = b"0-0"
    ended: bool = False
    failure: str | None = None


# Splits entries into rows and events, stopping at the first terminator (abort, or run_end).
def split(entries: list[Entry]) -> Chunk:
    chunk = Chunk()
    for entry_id, fields in entries:
        chunk.last_id = entry_id
        kind = fields[KIND]
        data = json.loads(fields[DATA])
        if kind == ABORT:
            chunk.failure = data["error"]
            break
        if kind == ROW:
            chunk.rows.append(data)
        elif kind == EVENT:
            chunk.events.append(data)
            if data.get("type") == "run_end":
                if data.get("termination_reason") in ("error", "time_limit"):
                    chunk.failure = data.get("error") or "runner failed"
                else:
                    chunk.ended = True
                break
    return chunk


class RunnerFailed(Exception):
    pass


# Yields runner_logs after `after` as chunks, up to and including the one with the normal run_end.
# Raises RunnerFailed when the runner failed or, once logs have started, they stop for too long.
def chunks(r: redis.Redis, run_id: str, after: bytes, last_entry: float | None) -> Iterator[Chunk]:
    while True:
        response = r.xread({runner_logs_key(run_id): after}, block=1000)  # ms
        if not response:
            if last_entry is not None and time.monotonic() - last_entry > IDLE_TIMEOUT_SECONDS:
                raise RunnerFailed(f"no logs for {IDLE_TIMEOUT_SECONDS} s")
            continue
        last_entry = time.monotonic()
        chunk = split(response[0][1])
        if chunk.failure is not None:
            raise RunnerFailed(chunk.failure)
        yield chunk
        if chunk.ended:
            return
        after = chunk.last_id


def write_error(r: redis.Redis, run_id: str, phase: str, message: str) -> None:
    log.info("run %s failed in %s: %s", run_id, phase, message)
    r.xadd(clustered_key(run_id), {PHASE: phase, ERROR: message})


def run_job(r: redis.Redis, run_id: str, stream: bool, config_json: bytes) -> None:
    config = Config.model_validate_json(config_json)
    last = r.xrevrange(clustered_key(run_id), count=1)
    # a final or error entry means an earlier delivery already finished this job
    if last and (FINAL in last[0][1] or ERROR in last[0][1]):
        return
    try:
        if stream:
            run_streamed(r, run_id, config, last[0][1][COVERS] if last else None)
        else:
            run_batch(r, run_id, config)
    except RunnerFailed as e:
        write_error(r, run_id, "runner", str(e))
    except ClusteringError as e:
        write_error(r, run_id, "clustering", str(e))


def run_streamed(r: redis.Redis, run_id: str, config: Config, covers: bytes | None) -> None:
    cs = ClusterStream(config)
    after = b"0-0"
    ended = False
    last_entry: float | None = None
    if covers is not None:
        # crash recovery: rebuild the state from the logs already clustered, and write nothing
        chunk = split(r.xrange(runner_logs_key(run_id), "-", covers))
        cs.cluster(chunk.rows, chunk.events)
        after, ended, last_entry = covers, chunk.ended, time.monotonic()
        log.info("run %s: replayed up to %s", run_id, covers.decode())

    if not ended:
        for chunk in chunks(r, run_id, after, last_entry):
            tick = time.monotonic()
            update = cs.cluster(chunk.rows, chunk.events)
            r.xadd(clustered_key(run_id), {COVERS: chunk.last_id, UPDATE: update.model_dump_json()})
            # at most one cluster() per tick: each update re-sends drifting percentiles and weights, so
            # many tiny updates would bloat clustered and every SSE replay of it
            if not chunk.ended:
                time.sleep(max(0.0, tick + UPDATE_INTERVAL_SECONDS - time.monotonic()))

    r.xadd(clustered_key(run_id), {FINAL: cs.finish().model_dump_json()})


def run_batch(r: redis.Redis, run_id: str, config: Config) -> None:
    rows: list[dict[str, Any]] = []
    events: list[dict[str, Any]] = []
    for chunk in chunks(r, run_id, b"0-0", None):
        rows += chunk.rows
        events += chunk.events
    r.xadd(clustered_key(run_id), {FINAL: cluster_run(rows, events, config).model_dump_json()})
