# archipelago-backend

**Archipelago** is an explainability platform for distributed (island-model) evolutionary algorithms.

This is its backend. It starts runs on the runner service, hands each run's logs to clustering workers over Redis streams, streams the live search trajectory network to the dashboard over server-sent events, and stores finished results in Postgres. The clustering itself is done by the [archipelago](https://github.com/FYP-Archipelago/archipelago) module.

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

4. Create your local `.env` from the example (dev values; use long random passwords on a server):

   ```bash
   cp .env.example .env
   ```

5. Start everything (Postgres, Redis, the API on port 8000, two clustering workers, and the Drizzle Gateway and RedisInsight viewers):

   ```bash
   docker compose up -d --build
   ```

## LICENSE

The project is licensed under the [MIT License](LICENSE).
