from datetime import datetime
from typing import Annotated, Any, Literal

from archipelago import Config
from pydantic import BaseModel, ConfigDict, StringConstraints

Status = Literal["queued", "running", "clustering", "done", "failed", "cancelling", "cancelled"]
Phase = Literal["runner", "clustering", "internal"]

PROGRESS_STATUSES: list[Status] = ["queued", "running", "clustering"]
FINISHED_STATUSES: list[Status] = ["done", "failed", "cancelled"]

RunName = Annotated[str, StringConstraints(strip_whitespace=True, min_length=1, max_length=100, pattern=r"^[^\x00]+$")]


class CreateRun(BaseModel):
    model_config = ConfigDict(extra="forbid")

    run_name: RunName
    preset: str
    run_settings: dict[str, Any]
    stream: bool = False
    clustering: Config


class RenameRun(BaseModel):
    model_config = ConfigDict(extra="forbid")

    run_name: RunName


class RunError(BaseModel):
    phase: Phase
    message: str


class RunStatus(BaseModel):
    run_id: str
    run_name: str
    status: Status
    stream: bool
    preset: str
    run_settings: dict[str, Any]
    clustering: Config
    error: RunError | None
    created_at: datetime
    updated_at: datetime
    started_at: datetime | None
    finished_at: datetime | None


class RunPage(BaseModel):
    runs: list[RunStatus]
    next_cursor: str | None
