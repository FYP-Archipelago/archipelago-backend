import logging
from dataclasses import dataclass
from typing import Any, Literal

import httpx

RUNNER_TIMEOUT_SECONDS = 10

logger = logging.getLogger(__name__)

RunnerState = Literal["queued", "running", "finished", "failed", "cancelled", "unknown"]


@dataclass(frozen=True)
class RunnerStatus:
    state: RunnerState
    error: str | None


class RunnerRejected(Exception):
    pass


class RunnerUnavailable(Exception):
    pass


def make_client(base_url: str) -> httpx.AsyncClient:
    return httpx.AsyncClient(base_url=base_url, timeout=RUNNER_TIMEOUT_SECONDS)


async def _request(client: httpx.AsyncClient, method: str, path: str, **kwargs: Any) -> httpx.Response:
    try:
        return await client.request(method, path, **kwargs)
    except httpx.HTTPError as e:
        raise RunnerUnavailable(f"runner unreachable: {type(e).__name__}: {e}") from e


def _error_text(response: httpx.Response) -> str:
    try:
        body = response.json()
    except ValueError:
        return response.text
    return str(body["error"]) if isinstance(body, dict) and "error" in body else response.text


async def presets(client: httpx.AsyncClient) -> bytes:
    response = await _request(client, "GET", "/presets")
    if response.status_code != 200:
        raise RunnerUnavailable(f"runner answered {response.status_code} to /presets")
    return response.content


async def start(client: httpx.AsyncClient, run_id: str, preset: str, overrides: dict[str, Any]) -> RunnerStatus:
    response = await _request(client, "POST", "/runs", json={"run_id": run_id, "preset": preset, "overrides": overrides})
    if response.status_code == 202:
        return RunnerStatus(state=response.json()["state"], error=None)
    if response.status_code == 422:
        raise RunnerRejected(_error_text(response))
    raise RunnerUnavailable(f"runner answered {response.status_code} to the start: {_error_text(response)}")


async def status(client: httpx.AsyncClient, run_id: str) -> RunnerStatus:
    response = await _request(client, "GET", f"/runs/{run_id}")
    if response.status_code == 404:
        return RunnerStatus(state="unknown", error=None)
    if response.status_code != 200:
        raise RunnerUnavailable(f"runner answered {response.status_code} to the status poll")
    body = response.json()
    return RunnerStatus(state=body["state"], error=body.get("error"))


async def cancel(client: httpx.AsyncClient, run_id: str) -> None:
    try:
        response = await _request(client, "POST", f"/runs/{run_id}/cancel")
    except RunnerUnavailable as e:
        logger.warning("cancel of %s failed: %s", run_id, e)
        return
    if not (response.is_success or response.status_code == 404):
        logger.warning("cancel of %s answered %s", run_id, response.status_code)
