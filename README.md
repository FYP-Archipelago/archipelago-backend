# archipelago-backend

**Archipelago** is an explainability platform for distributed (island-model) evolutionary algorithms.

This is the backend for Archipelago. It starts runs on the runner service, hands each run's logs to clustering workers over Redis streams, streams the live search trajectory network to the dashboard over server-sent events (SSE), and stores finished results in the DB. The clustering itself is done by the [archipelago](https://github.com/FYP-Archipelago/archipelago) module.

## Development

1. Clone the repository:

   ```bash
   git clone https://github.com/FYP-Archipelago/archipelago-backend
   cd archipelago-backend
   ```

2. Install the dependencies:

   ```bash
   uv sync
   ```

3. Install the pre-commit hooks:

   ```bash
   uv run pre-commit install
   ```

4. Create your local `.env` from the example:

   ```bash
   cp .env.example .env
   ```

5. Start everything (Postgres, Redis, API and 2 clustering workers):

   ```bash
   docker compose up -d --build
   ```

   To also start the Drizzle Gateway and RedisInsight viewers:

   ```bash
   docker compose --profile dev up -d --build
   ```

## Tests

1. The tests require Postgres and Redis to be running:

   ```bash
   docker run -d --rm --name test-pg -p 127.0.0.1:55432:5432 -e POSTGRES_USER=test -e POSTGRES_PASSWORD=test -e POSTGRES_DB=test postgres:16-alpine
   docker run -d --rm --name test-redis -p 127.0.0.1:56379:6379 redis:7-alpine
   ```

2. Wait a few seconds for the containers to start, then run:

   ```bash
   export ARCHIPELAGO_TEST_DATABASE_URL=postgresql://test:test@localhost:55432/test
   export ARCHIPELAGO_TEST_REDIS_URL=redis://localhost:56379/0
   uv run pytest
   ```

3. Stop the containers when you are done:

   ```bash
   docker stop test-pg test-redis
   ```

## LICENSE

The project is licensed under the [MIT License](LICENSE).
