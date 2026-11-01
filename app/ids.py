import re
import secrets
from datetime import UTC, datetime

RUN_ID_PATTERN = re.compile(r"^run-\d{8}T\d{6}Z-[0-9a-f]{8}$")


def mint_run_id() -> str:
    return f"run-{datetime.now(UTC):%Y%m%dT%H%M%SZ}-{secrets.token_hex(4)}"


def is_run_id(value: str) -> bool:
    return RUN_ID_PATTERN.fullmatch(value) is not None
