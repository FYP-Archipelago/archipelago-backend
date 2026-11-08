import hashlib
import re
import secrets

from pwdlib import PasswordHash
from pwdlib.exceptions import UnknownHashError
from pwdlib.hashers.argon2 import Argon2Hasher

PASSWORD_DIGEST_PATTERN = re.compile(r"[0-9a-f]{64}")
PASSWORD_HASHER = PasswordHash((Argon2Hasher(),))
TOKEN_BYTES = 32


def validate_password_digest(value: str) -> str:
    if not isinstance(value, str) or PASSWORD_DIGEST_PATTERN.fullmatch(value) is None:
        raise ValueError("password_digest must be exactly 64 lowercase hexadecimal characters")
    return value


def hash_password(password_digest: str) -> str:
    return PASSWORD_HASHER.hash(validate_password_digest(password_digest))


def verify_password(password_digest: str, password_hash: str) -> bool:
    validate_password_digest(password_digest)
    try:
        return PASSWORD_HASHER.verify(password_digest, password_hash)
    except UnknownHashError:
        return False


def mint_token() -> str:
    return secrets.token_urlsafe(TOKEN_BYTES)


def hash_token(token: str) -> bytes:
    return hashlib.sha256(token.encode("utf-8")).digest()
