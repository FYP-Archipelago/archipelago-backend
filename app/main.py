import logging
from collections.abc import AsyncGenerator
from contextlib import AsyncExitStack, asynccontextmanager

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from redis.asyncio import Redis
from redis.exceptions import ConnectionError as RedisConnectionError
from redis.exceptions import ResponseError

from app import __version__, runner
from app.db import queries
from app.redis_names import GROUP, PENDING_RUNS
from app.routes import router
from app.settings import DEFAULT_RUNNER_KEY, load_settings
from app.supervisor import cancel_all, start_supervisor

# one per open SSE viewer (each blocked XREAD holds a connection) and per supervisor, plus headroom
REDIS_MAX_CONNECTIONS = 200

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
logger = logging.getLogger(__name__)

settings = load_settings()


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncGenerator[None]:
    async with AsyncExitStack() as stack:
        pool = await queries.open_pool(settings.database_url)
        stack.push_async_callback(pool.close)
        await queries.apply_schema(pool)
        # a pooled connection that died (e.g. Redis restarted) is reopened and the command retried once
        redis = Redis.from_url(
            settings.redis_url, decode_responses=False, max_connections=REDIS_MAX_CONNECTIONS, retry_on_error=[RedisConnectionError]
        )
        stack.push_async_callback(redis.aclose)
        try:
            await redis.xgroup_create(PENDING_RUNS, GROUP, id="0", mkstream=True)
        except ResponseError as e:
            if "BUSYGROUP" not in str(e):
                raise
        runners = {key: await stack.enter_async_context(runner.make_client(url)) for key, url in settings.runner_urls.items()}
        app.state.pool = pool
        app.state.redis = redis
        app.state.runners = runners
        app.state.runner = runners[DEFAULT_RUNNER_KEY]
        stack.push_async_callback(cancel_all)

        for run_id, runner_key in (await queries.list_unexpired(pool)).items():
            client = runners.get(runner_key)
            if client is None:
                logger.warning("runner %r for run %s is not configured", runner_key, run_id)
                continue
            start_supervisor(run_id, pool, redis, client)

        yield


app = FastAPI(title="archipelago-backend", version=__version__, lifespan=lifespan)
app.add_middleware(CORSMiddleware, allow_origins=settings.cors_origins, allow_methods=["*"], allow_headers=["*"])
app.include_router(router)
