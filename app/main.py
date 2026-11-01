import logging
from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from redis.asyncio import Redis
from redis.exceptions import ConnectionError as RedisConnectionError
from redis.exceptions import ResponseError

from app import __version__, runner
from app.db import queries
from app.redis_names import GROUP, PENDING_RUNS
from app.routes import router
from app.settings import load_settings
from app.supervisor import cancel_all, start_supervisor

# one per open SSE viewer (each blocked XREAD holds a connection) and per supervisor, plus headroom
REDIS_MAX_CONNECTIONS = 200

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")

settings = load_settings()


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncGenerator[None]:
    pool = await queries.open_pool(settings.database_url)
    await queries.apply_schema(pool)
    # a pooled connection that died (e.g. Redis restarted) is reopened and the command retried once
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

    for run_id in await queries.list_unexpired(pool):
        start_supervisor(run_id, pool, redis, client)

    yield

    await cancel_all()
    await client.aclose()
    await redis.aclose()
    await pool.close()


app = FastAPI(title="archipelago-backend", version=__version__, lifespan=lifespan)
app.add_middleware(CORSMiddleware, allow_origins=settings.cors_origins, allow_methods=["*"], allow_headers=["*"])
app.include_router(router)
