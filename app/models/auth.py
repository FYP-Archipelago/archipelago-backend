from dataclasses import dataclass, field
from datetime import datetime
from typing import Annotated
from uuid import UUID

from pydantic import BaseModel, ConfigDict, EmailStr, Field, StringConstraints, field_validator


@dataclass(frozen=True)
class User:
    id: UUID
    email: str
    password_hash: str = field(repr=False)
    full_name: str
    institution: str | None
    created_at: datetime


@dataclass(frozen=True)
class Session:
    id: UUID
    user_id: UUID
    access_token_hash: bytes = field(repr=False)
    refresh_token_hash: bytes = field(repr=False)
    csrf_token: str = field(repr=False)
    created_at: datetime
    access_expires_at: datetime
    refresh_expires_at: datetime


Email = Annotated[EmailStr, Field(max_length=254)]
PasswordDigest = Annotated[str, Field(repr=False, min_length=64, max_length=64, pattern=r"^[0-9a-f]+$")]


class RegisterRequest(BaseModel):
    model_config = ConfigDict(extra="forbid", strict=True, hide_input_in_errors=True)

    email: Email
    password_digest: PasswordDigest
    full_name: Annotated[str, StringConstraints(strip_whitespace=True, min_length=1, max_length=120)]
    institution: Annotated[str, StringConstraints(strip_whitespace=True, max_length=200)] | None = None

    @field_validator("institution")
    @classmethod
    def empty_institution(cls, value: str | None) -> str | None:
        return value or None


class LoginRequest(BaseModel):
    model_config = ConfigDict(extra="forbid", strict=True, hide_input_in_errors=True)

    email: Email
    password_digest: PasswordDigest


class UserResponse(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: UUID
    email: str
    full_name: str
    institution: str | None
    created_at: datetime


class SessionResponse(BaseModel):
    user: UserResponse
    csrf_token: str = Field(repr=False)
    access_expires_at: datetime
    refresh_expires_at: datetime
