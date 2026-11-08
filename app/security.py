import hashlib
import secrets

from pwdlib import PasswordHash
from pwdlib.exceptions import UnknownHashError
from pwdlib.hashers.argon2 import Argon2Hasher

PASSWORD_HASHER = PasswordHash((Argon2Hasher(),))
TOKEN_BYTES = 32


def hash_password(password_digest: str) -> str:
    return PASSWORD_HASHER.hash(password_digest)


def verify_password(password_digest: str, password_hash: str) -> bool:
    try:
        return PASSWORD_HASHER.verify(password_digest, password_hash)
    except UnknownHashError:
        return False


def mint_token() -> str:
    return secrets.token_urlsafe(TOKEN_BYTES)


def hash_token(token: str) -> bytes:
    return hashlib.sha256(token.encode("utf-8")).digest()
