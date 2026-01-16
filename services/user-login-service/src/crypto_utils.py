"""Utility functions for encrypting/decrypting sensitive fields.

This module uses a symmetric key derived from the ENCRYPTION_KEY
environment variable. For production, set a strong random value e.g.:

    openssl rand -base64 32

If ENCRYPTION_KEY is not set, the functions become no-ops so the
service still works in development without breaking existing data.
"""

import os
import base64
import hashlib
from typing import Optional

from cryptography.fernet import Fernet, InvalidToken


_raw_key = os.getenv("ENCRYPTION_KEY", "")
_fernet: Optional[Fernet]

if not _raw_key:
    # Development / non-encrypted mode
    _fernet = None
else:
    # Derive a 32-byte key from the provided secret using SHA-256 and
    # convert it to a url-safe base64 key compatible with Fernet.
    key_bytes = hashlib.sha256(_raw_key.encode("utf-8")).digest()
    fernet_key = base64.urlsafe_b64encode(key_bytes)
    _fernet = Fernet(fernet_key)


def encrypt_str(value: Optional[str]) -> Optional[str]:
    """Encrypt a string using the configured Fernet key.

    If ENCRYPTION_KEY is not set or value is empty, the input is
    returned as-is. This keeps behaviour backward-compatible while
    allowing you to enable encryption per environment.
    """

    if not value:
        return value
    if _fernet is None:
        return value
    token = _fernet.encrypt(value.encode("utf-8"))
    return token.decode("utf-8")


def decrypt_str(value: Optional[str]) -> Optional[str]:
    """Decrypt a previously encrypted string.

    If ENCRYPTION_KEY is not set or decryption fails (e.g. legacy
    plaintext values), the original value is returned unchanged.
    """

    if not value:
        return value
    if _fernet is None:
        return value
    try:
        plaintext = _fernet.decrypt(value.encode("utf-8"))
        return plaintext.decode("utf-8")
    except InvalidToken:
        # Likely stored as plaintext or encrypted with a different key.
        return value

