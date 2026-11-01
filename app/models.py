from datetime import datetime
from typing import Any, Literal

from archipelago import Config
from pydantic import BaseModel, ConfigDict

Status = Literal["queued", "running", "clustering", "done", "failed", "cancelled"]
Phase = Literal["runner", "clustering", "internal"]


class CreateRun(BaseModel):
    model_config = ConfigDict(extra="forbid")

    preset: str
    overrides: dict[str, Any] = {}
    stream: bool = False
    clustering: Config


class RunError(BaseModel):
    phase: Phase
    message: str


class RunStatus(BaseModel):
    run_id: str
    status: Status
    stream: bool
    preset: str
    overrides: dict[str, Any]
    clustering: Config
    error: RunError | None
    created_at: datetime
    updated_at: datetime
