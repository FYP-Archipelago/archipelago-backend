from datetime import datetime
from typing import Annotated, Any, Literal

from archipelago import Config
from pydantic import BaseModel, ConfigDict, StringConstraints

Status = Literal["queued", "running", "clustering", "done", "failed", "cancelling", "cancelled"]
Phase = Literal["runner", "clustering", "internal"]

RunName = Annotated[str, StringConstraints(strip_whitespace=True, min_length=1, max_length=100)]


class CreateRun(BaseModel):
    model_config = ConfigDict(extra="forbid")

    run_name: RunName
    preset: str
    overrides: dict[str, Any] = {}
    stream: bool = False
    clustering: Config


class RunError(BaseModel):
    phase: Phase
    message: str


class RunStatus(BaseModel):
    run_id: str
    run_name: str
    status: Status
    stream: bool
    preset: str
    overrides: dict[str, Any]
    clustering: Config
    error: RunError | None
    created_at: datetime
    updated_at: datetime


class RunPage(BaseModel):
    runs: list[RunStatus]
    next_cursor: str | None
