import asyncio
import hashlib
import json
from datetime import UTC, datetime
from types import TracebackType
from typing import Any, Self
from uuid import UUID

import httpx
from starlette.types import Message

from app.db import auth, runs
from app.ids import mint_run_id
from app.main import app
from app.models.auth import Session
from app.models.runs import CreateRun
from app.security import hash_token
from app.settings import ACCESS_COOKIE_NAME, REFRESH_COOKIE_NAME

ORIGIN = "http://localhost:5173"
DIGEST = hashlib.sha256(b"archipelago:password:v1:test password").hexdigest()
REGISTRATION = {"email": " Researcher@Example.COM ", "password_digest": DIGEST, "full_name": " Researcher ", "institution": " Team 21 "}
LOGIN = {"email": "RESEARCHER@example.com", "password_digest": DIGEST}
RUN_BODY = {"preset": "de-rastrigin", "overrides": {}, "stream": True, "clustering": {"lsh": True}}


async def register(browser: httpx.AsyncClient, **values: str) -> httpx.Response:
    response = await browser.post("/auth/register", json={**REGISTRATION, **values}, headers={"Origin": ORIGIN})
    assert response.status_code == 201, response.text
    return response


async def session_for(response: httpx.Response) -> Session:
    async with app.state.pool.connection() as conn:
        session = await auth.get_session_by_refresh_hash(conn, hash_token(response.cookies[REFRESH_COOKIE_NAME]), datetime.now(UTC))
    assert session is not None
    return session


async def read_session(session_id: UUID) -> Session | None:
    async with app.state.pool.connection() as conn:
        row = await (await conn.execute("SELECT * FROM auth_sessions WHERE id = %s", (session_id,))).fetchone()
    return Session(**row) if row else None


def cookie_headers(response: httpx.Response) -> dict[str, str]:
    return {"Origin": ORIGIN, "Cookie": "; ".join(f"{name}={response.cookies[name]}" for name in (ACCESS_COOKIE_NAME, REFRESH_COOKIE_NAME))}


def csrf_headers(response: httpx.Response) -> dict[str, str]:
    return {**cookie_headers(response), "X-CSRF-Token": response.json()["csrf_token"]}


async def wait_blocked(pid: int) -> None:
    # Confirm actual PostgreSQL contention before releasing the competing request.
    async with asyncio.timeout(5), app.state.pool.connection() as conn:
        while True:
            row = await (await conn.execute("SELECT cardinality(pg_blocking_pids(%s)) > 0 AS blocked", (pid,))).fetchone()
            if row["blocked"]:
                return
            await asyncio.sleep(0.01)


async def seed_run(user_id: UUID, run_id: str | None = None) -> str:
    run = await runs.insert_run(app.state.pool, run_id or mint_run_id(), user_id, CreateRun.model_validate(RUN_BODY))
    return run.run_id


class RunStream:
    def __init__(self, run_id: str, headers: dict[str, str]) -> None:
        self.run_id, self.headers = run_id, headers
        self.messages: asyncio.Queue[Message] = asyncio.Queue(maxsize=1)
        self.ack: asyncio.Queue[None] = asyncio.Queue()
        self.disconnected = asyncio.Event()
        self.pending_ack = False
        self.first_receive = True

    async def receive(self) -> Message:
        if self.first_receive:
            self.first_receive = False
            return {"type": "http.request", "body": b"", "more_body": False}
        await self.disconnected.wait()
        return {"type": "http.disconnect"}

    async def send(self, message: Message) -> None:
        await self.messages.put(message)
        if message.get("more_body"):
            await self.ack.get()

    async def __aenter__(self) -> Self:
        path = f"/runs/{self.run_id}/events"
        scope = {
            "type": "http",
            "asgi": {"version": "3.0", "spec_version": "2.3"},
            "http_version": "1.1",
            "method": "GET",
            "scheme": "http",
            "path": path,
            "raw_path": path.encode(),
            "root_path": "",
            "query_string": b"",
            "headers": [(key.lower().encode(), value.encode()) for key, value in self.headers.items()],
            "client": ("127.0.0.1", 1234),
            "server": ("api", 80),
        }
        self.task = asyncio.create_task(app(scope, self.receive, self.send))
        try:
            started = await asyncio.wait_for(self.messages.get(), 5)
            assert started["type"] == "http.response.start" and started["status"] == 200
            assert dict(started["headers"])[b"cache-control"] == b"no-store"
        except BaseException:
            self.task.cancel()
            await asyncio.gather(self.task, return_exceptions=True)
            raise
        return self

    async def next(self) -> tuple[str, str | None, Any] | None:
        if self.pending_ack:
            self.ack.put_nowait(None)
        message = await asyncio.wait_for(self.messages.get(), 5)
        self.pending_ack = message.get("more_body", False)
        body = message.get("body", b"")
        if not body:
            await asyncio.wait_for(self.task, 5)
            return None
        if body.startswith(b":"):
            return "keepalive", None, None
        fields = dict(line.split(": ", 1) for line in body.decode().strip().splitlines())
        return fields["event"], fields.get("id"), json.loads(fields["data"])

    async def __aexit__(self, exc_type: type[BaseException] | None, exc: BaseException | None, traceback: TracebackType | None) -> None:
        self.disconnected.set()
        self.task.cancel()
        await asyncio.gather(self.task, return_exceptions=True)
