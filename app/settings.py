import os
from dataclasses import dataclass


@dataclass(frozen=True)
class Settings:
    database_url: str
    redis_url: str
    runner_url: str
    cors_origin: str
    cookie_secure: bool


def _env(name: str, default: str | None = None) -> str:
    value = os.environ.get(name, default)
    if value is None:
        raise RuntimeError(f"missing required environment variable {name}")
    return value


def load_settings() -> Settings:
    origin = _env("ARCHIPELAGO_CORS_ORIGIN").strip()
    if not origin.startswith(("http://", "https://")) or "*" in origin or "," in origin:
        raise RuntimeError("ARCHIPELAGO_CORS_ORIGIN must be one HTTP(S) origin without wildcards")

    return Settings(
        database_url=_env("ARCHIPELAGO_DATABASE_URL"),
        redis_url=_env("ARCHIPELAGO_REDIS_URL"),
        runner_url=_env("ARCHIPELAGO_RUNNER_URL").rstrip("/"),
        cors_origin=origin,
        cookie_secure=_env("ARCHIPELAGO_COOKIE_SECURE", "true") != "false",
    )
