import asyncio
import logging
from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager, suppress

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from psycopg_pool import AsyncConnectionPool
from redis.asyncio import Redis
from redis.exceptions import ConnectionError as RedisConnectionError
from redis.exceptions import ResponseError
from starlette.datastructures import MutableHeaders
from starlette.types import ASGIApp, Message, Receive, Scope, Send

from app import __version__, runner
from app.db import apply_schema, auth, open_pool, runs
from app.redis_names import GROUP, PENDING_RUNS
from app.routes import router
from app.settings import load_settings
from app.supervisor import cancel_all, start_supervisor

# One per open SSE viewer (each blocked XREAD holds a connection) and per supervisor, plus headroom
REDIS_MAX_CONNECTIONS = 200
SESSION_CLEANUP_SECONDS = 3600

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
logger = logging.getLogger(__name__)

settings = load_settings()


async def _cleanup_sessions(pool: AsyncConnectionPool) -> None:
    while True:
        try:
            await auth.delete_expired_sessions(pool)
        except Exception:
            logger.exception("expired session cleanup failed")
        await asyncio.sleep(SESSION_CLEANUP_SECONDS)


# Browsers keep any response without no-store in their disk cache, where it stays readable after logout.
class NoStoreMiddleware:
    def __init__(self, app: ASGIApp) -> None:
        self.app = app

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        async def send_no_store(message: Message) -> None:
            if message["type"] == "http.response.start":
                MutableHeaders(scope=message)["Cache-Control"] = "no-store"
            await send(message)

        await self.app(scope, receive, send_no_store if scope["type"] == "http" else send)


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncGenerator[None]:
    pool = await open_pool(settings.database_url)
    await apply_schema(pool)
    # A pooled connection that died (e.g. Redis restarted) is reopened and the command retried once
    redis = Redis.from_url(settings.redis_url, decode_responses=False, max_connections=REDIS_MAX_CONNECTIONS, retry_on_error=[RedisConnectionError])
    try:
        await redis.xgroup_create(PENDING_RUNS, GROUP, id="0", mkstream=True)
    except ResponseError as e:
        if "BUSYGROUP" not in str(e):
            raise
    client = runner.make_client(settings.runner_url)
    app.state.pool = pool
    app.state.redis = redis
    app.state.runner = client

    for run_id in await runs.list_unexpired(pool):
        start_supervisor(run_id, pool, redis, client)

    cleanup = asyncio.create_task(_cleanup_sessions(pool), name="session-cleanup")
    try:
        yield
    finally:
        cleanup.cancel()
        with suppress(asyncio.CancelledError):
            await cleanup
        await cancel_all()
        await client.aclose()
        await redis.aclose()
        await pool.close()


app = FastAPI(title="archipelago-backend", version=__version__, lifespan=lifespan)
app.state.settings = settings
app.add_middleware(NoStoreMiddleware)
app.add_middleware(
    CORSMiddleware,
    allow_origins=[settings.cors_origin],
    allow_credentials=True,
    allow_methods=["GET", "POST"],
    allow_headers=["Content-Type", "X-CSRF-Token", "Last-Event-ID"],
)
app.include_router(router)
