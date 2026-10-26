import os
from dataclasses import dataclass


@dataclass(frozen=True)
class Settings:
    database_url: str
    redis_url: str
    runner_url: str
    cors_origins: list[str]


def _env(name: str, default: str | None = None) -> str:
    value = os.environ.get(name, default)
    if value is None:
        raise RuntimeError(f"missing required environment variable {name}")
    return value


def load_settings() -> Settings:
    return Settings(
        database_url=_env("ARCHIPELAGO_DATABASE_URL"),
        redis_url=_env("ARCHIPELAGO_REDIS_URL"),
        runner_url=_env("ARCHIPELAGO_RUNNER_URL").rstrip("/"),
        cors_origins=[origin.strip() for origin in _env("ARCHIPELAGO_CORS_ORIGINS", "*").split(",") if origin.strip()],
    )
