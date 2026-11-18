from dataclasses import dataclass
from datetime import datetime
from typing import Annotated
from uuid import UUID

from pydantic import AfterValidator, BaseModel, ConfigDict, EmailStr, StringConstraints, field_validator

Email = Annotated[EmailStr, AfterValidator(str.lower)]
PasswordDigest = Annotated[str, StringConstraints(pattern=r"^[0-9a-f]{64}$")]


@dataclass(frozen=True)
class Session:
    id: UUID
    user_id: UUID
    csrf_token: str
    access_expires_at: datetime
    refresh_expires_at: datetime


class RegisterRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    email: Email
    password_digest: PasswordDigest
    full_name: Annotated[str, StringConstraints(strip_whitespace=True, min_length=1, max_length=120)]
    institution: Annotated[str, StringConstraints(strip_whitespace=True, max_length=200)] | None = None

    @field_validator("institution")
    @classmethod
    def empty_institution(cls, value: str | None) -> str | None:
        return value or None


class LoginRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    email: Email
    password_digest: PasswordDigest


class User(BaseModel):
    id: UUID
    email: str
    full_name: str
    institution: str | None
    created_at: datetime


class SessionResponse(BaseModel):
    user: User
    csrf_token: str
    access_expires_at: datetime
    refresh_expires_at: datetime
