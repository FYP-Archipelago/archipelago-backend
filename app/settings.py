import json
import os
import re
from dataclasses import dataclass
from urllib.parse import urlsplit

DEFAULT_RUNNER_KEY = "dea"
RUNNER_KEY = re.compile(r"[a-z][a-z0-9_-]{0,31}")


@dataclass(frozen=True)
class Settings:
    database_url: str
    redis_url: str
    runner_urls: dict[str, str]
    cors_origins: list[str]


def _env(name: str, default: str | None = None) -> str:
    value = os.environ.get(name, default)
    if value is None:
        raise RuntimeError(f"missing required environment variable {name}")
    return value


def _runner_urls() -> dict[str, str]:
    runner_urls = {DEFAULT_RUNNER_KEY: _env("ARCHIPELAGO_RUNNER_URL").rstrip("/")}
    try:
        extra = json.loads(os.environ.get("ARCHIPELAGO_RUNNERS") or "{}")
    except json.JSONDecodeError as e:
        raise RuntimeError("ARCHIPELAGO_RUNNERS must be a JSON object of runner keys and URLs") from e
    if not isinstance(extra, dict):
        raise TypeError("ARCHIPELAGO_RUNNERS must be a JSON object of runner keys and URLs")
    for key, url in extra.items():
        if key == DEFAULT_RUNNER_KEY or not RUNNER_KEY.fullmatch(key):
            raise RuntimeError("ARCHIPELAGO_RUNNERS keys must match [a-z][a-z0-9_-]{0,31} and cannot replace dea")
        try:
            parsed = urlsplit(url) if isinstance(url, str) else None
            valid = parsed is not None and parsed.scheme in ("http", "https") and bool(parsed.hostname)
            if valid:
                _ = parsed.port
        except ValueError:
            valid = False
        if not valid:
            raise RuntimeError(f"ARCHIPELAGO_RUNNERS entry {key!r} must have an absolute HTTP or HTTPS URL")
        runner_urls[key] = url.rstrip("/")
    return runner_urls


def load_settings() -> Settings:
    return Settings(
        database_url=_env("ARCHIPELAGO_DATABASE_URL"),
        redis_url=_env("ARCHIPELAGO_REDIS_URL"),
        runner_urls=_runner_urls(),
        cors_origins=[origin.strip() for origin in _env("ARCHIPELAGO_CORS_ORIGINS", "*").split(",") if origin.strip()],
    )
