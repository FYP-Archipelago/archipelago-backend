import hashlib
import json
import os
import random
from collections.abc import AsyncGenerator, Callable, Iterator
from contextlib import asynccontextmanager
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import httpx
import psycopg
import pytest
import redis
from fastapi import FastAPI

# the tests wipe both stores, so they only run against the ones named for them, never a dev .env
os.environ["ARCHIPELAGO_DATABASE_URL"] = os.environ["ARCHIPELAGO_TEST_DATABASE_URL"]
os.environ["ARCHIPELAGO_REDIS_URL"] = os.environ["ARCHIPELAGO_TEST_REDIS_URL"]
os.environ["ARCHIPELAGO_RUNNER_URL"] = "http://runner"

from app import runner as runner_client
from app.main import app, lifespan

SYNTHETIC_RUN_ID = "run-20261027T000000Z-5e1f0c0d"
SCHEMA = Path(__file__).parents[1] / "app" / "db" / "schema.sql"


@dataclass
class FakeRunner:
    state: str = "running"
    error: str | None = None
    start: httpx.Response | Exception = field(default_factory=lambda: httpx.Response(202, json={"run_id": "", "state": "running"}))
    status: httpx.Response | Exception | Callable[[], httpx.Response] | None = None
    presets: httpx.Response = field(default_factory=lambda: httpx.Response(200, content=b'{"overrides": {}, "presets": []}'))
    on_start: Callable[[], None] | None = None
    calls: list[str] = field(default_factory=list)

    def handle(self, request: httpx.Request) -> httpx.Response:
        self.calls.append(f"{request.method} {request.url.path}")
        if request.url.path == "/presets":
            return self.presets
        if request.method == "POST" and request.url.path == "/runs":
            if self.on_start:
                self.on_start()
            return _answer(self.start)
        if request.url.path.endswith("/cancel"):
            return httpx.Response(202)
        if callable(self.status):
            return self.status()
        if self.status is not None:
            return _answer(self.status)
        return httpx.Response(200, json={"state": self.state, "error": self.error})


def _answer(response: httpx.Response | Exception) -> httpx.Response:
    if isinstance(response, Exception):
        raise response
    return response


@pytest.fixture
def anyio_backend() -> str:
    return "asyncio"


# A small island-model run as runner_logs entries, in the order the runner writes them: run and
# island starts, then each island's rows and migration arrivals in seq order (islands taking turns),
# then the ends. Each island random-walks from its own start, every seventh child repeats its
# parent's genome, and two migrants per island arrive from the next island.
@pytest.fixture(scope="session")
def log_entries(per_island: int = 150, islands: int = 3, dimension: int = 4) -> list[dict[str, Any]]:
    rng = random.Random(1)
    ids = [[f"i{island}-{k}" for k in range(per_island)] for island in range(islands)]
    records: list[list[dict[str, Any]]] = []
    for island in range(islands):
        records.append([])
        previous = [(island - 1) * 2.0] * dimension
        for k in range(per_island):
            genome = previous if k % 7 == 6 else [round(min(5.0, max(-5.0, x + rng.gauss(0, 0.3))), 6) for x in previous]
            text = json.dumps(genome)
            row = {
                "run_id": SYNTHETIC_RUN_ID,
                "island_id": island,
                "seq": len(records[island]),
                "eval_index": k,
                "generation": k // 10,
                "t_wall": 0.5 * k + 0.1 * island,
                "individual_id": ids[island][k],
                "parent_ids": [ids[island][k - 1]] if k else [],
                "fitness": sum(x * x for x in genome),
                "genome_encoding": "real_vector",
                "genome_hash": hashlib.sha1(text.encode()).hexdigest()[:12],
                "genome_repr": genome,
                "genome_repr_mode": "full",
            }
            records[island].append({"kind": "row", "data": row})
            previous = genome
            if k in (50, 80):
                arrival = {
                    "type": "migration_arrive",
                    "island_id": island,
                    "seq": len(records[island]),
                    "t_wall": 0.5 * k + 0.1 * island + 0.01,
                    "origin_individual_ids": [ids[(island + 1) % islands][k - 20]],
                    "arrived_individual_ids": [ids[island][k]],
                }
                records[island].append({"kind": "event", "data": arrival})

    start = {
        "type": "run_start",
        "schema_version": "2.0",
        "run_id": SYNTHETIC_RUN_ID,
        "algorithm": "ga",
        "benchmark": "sphere",
        "num_islands": islands,
        "evaluation_budget": islands * per_island,
        "evaluation_budget_per_island": per_island,
        "datasets": {"maximising": False},
        "migration": {"topology": "ring"},
    }
    island_starts = [
        {
            "type": "island_start",
            "island_id": island,
            "genome_encoding": "real_vector",
            "benchmark_params": {"dimension": dimension, "lower": -5.0, "upper": 5.0},
            "clock_offset_ns": 0,
        }
        for island in range(islands)
    ]
    ends = [
        *({"type": "generation_end", "island_id": island, "maximising": False} for island in range(islands)),
        *({"type": "island_end", "island_id": island} for island in range(islands)),
        {"type": "run_end", "termination_reason": "completed"},
    ]
    turns = [record for turn in zip(*records, strict=True) for record in turn]
    return [*({"kind": "event", "data": e} for e in [start, *island_starts]), *turns, *({"kind": "event", "data": e} for e in ends)]


@pytest.fixture
def sync_redis() -> Iterator[redis.Redis]:
    client = redis.Redis.from_url(os.environ["ARCHIPELAGO_REDIS_URL"])
    yield client
    client.close()


@pytest.fixture(autouse=True)
def clean_stores(sync_redis: redis.Redis) -> None:
    sync_redis.flushdb()
    with psycopg.connect(os.environ["ARCHIPELAGO_DATABASE_URL"], autocommit=True) as conn:
        conn.execute(SCHEMA.read_text())
        conn.execute("TRUNCATE runs CASCADE")


@pytest.fixture
def runner() -> FakeRunner:
    return FakeRunner()


@pytest.fixture
def running_app(runner: FakeRunner, monkeypatch: pytest.MonkeyPatch) -> Callable[[], Any]:
    monkeypatch.setattr(
        runner_client, "make_client", lambda base_url: httpx.AsyncClient(transport=httpx.MockTransport(runner.handle), base_url=base_url)
    )

    # each `async with running_app()` is one API process lifetime, so a test can restart it
    @asynccontextmanager
    async def start() -> AsyncGenerator[FastAPI]:
        async with lifespan(app):
            yield app

    return start


@pytest.fixture
async def api(running_app: Callable[[], Any]) -> AsyncGenerator[httpx.AsyncClient]:
    async with running_app(), httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://api") as client:
        yield client


def push_logs(client: redis.Redis, run_id: str, entries: list[dict[str, Any]]) -> list[bytes]:
    pipe = client.pipeline(transaction=False)
    for entry in entries:
        pipe.xadd(f"run:{run_id}:runner_logs", {"kind": entry["kind"], "data": json.dumps(entry["data"])})
    return pipe.execute()


def without_elapsed(result_json: str | bytes) -> dict[str, Any]:
    result = json.loads(result_json)
    result["clustering"].pop("elapsed_seconds")
    return result
