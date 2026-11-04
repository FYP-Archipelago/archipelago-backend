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

## Shapley attribution

Workers retain compact individual lineage while clustering streaming or final-only runs, including stream replay. At completion, Python computes observational island and migration-route Shapley games. The supervisor stores the versioned explanation and graph in the same transaction; the module's public `Result` schema is unchanged. No genomes are included in explanations.

`GET /runs/{run_id}/explanation` returns:

- HTTP 202, `status: "pending"`, while the run is queued, running or clustering.
- HTTP 200, `status: "ready"`, with an `explanation` artifact after final persistence.
- HTTP 200, `status: "unavailable"`, with `detail` for legacy runs, stopped runs, insufficient lineage or analysis limits.
- HTTP 404 for unknown or invalid IDs.

Each response includes `schema_version: 1` and `run_id`. Responses are uncached and support gzip. API startup applies the additive `explanations` table schema. Restart both API and workers after upgrading. Existing saved runs are not backfilled; older worker final entries remain compatible.

The game measures recorded ancestry support, not causal effects of rerunning an optimiser with an island or route disabled. Initial founders must be explicit; migration arrivals require origin IDs. Incomplete ancestry is excluded and reported in coverage. Island and route games have different baselines and their credits must not be added together.

Analysis retains at most 100,000 records and 64 islands. Games with at most ten players use exact coalition enumeration; larger games use 1,024 fixed-seed permutations with a 50-million record-coalition comparison limit. Limits return an unavailable explanation without failing the graph. See [the implementation plan](SHAPLEY_PLAN.md).

The isolated analysis, real-module worker-boundary, API and transaction tests need no external services:

```sh
uv run pytest tests/shapley --confcutdir=tests/shapley -q
uv run ruff check app tests/shapley
uv run ruff format --check app tests/shapley
```

These tests do not replace the Postgres/Redis integration suite above.

## LICENSE

The project is licensed under the [MIT License](LICENSE).
