"""
streamrelay.crypto — AES-256-GCM end-to-end encryption for relay messages.

The relay server is a dumb forwarder: it routes opaque blobs between producer
and consumer without being able to read them. This module provides the
encrypt/decrypt pair so sensitive token payloads never appear in plaintext
on the relay host.

Wire format:
    {"type": "enc", "d": "<base64url(nonce[12] + ciphertext + authtag[16])>"}

If no key is configured, messages pass through unchanged (backward compatible).
"""

import base64
import json
import os

from cryptography.hazmat.primitives.ciphers.aead import AESGCM


def generate_key() -> str:
    """Generate a random AES-256 key, base64-encoded. Run once and store in .env."""
    return base64.b64encode(os.urandom(32)).decode()


def encrypt_message(key_b64: str, plaintext_json: str) -> str:
    """Encrypt a JSON string and return the relay wire format.

    Args:
        key_b64: Base64-encoded 32-byte AES-256 key.
        plaintext_json: Any JSON string, e.g. '{"type":"token","content":"Hi"}'.

    Returns:
        JSON string: '{"type": "enc", "d": "<base64(nonce+ciphertext+tag)>"}'
    """
    key = base64.b64decode(key_b64)
    nonce = os.urandom(12)
    aesgcm = AESGCM(key)
    ciphertext_with_tag = aesgcm.encrypt(nonce, plaintext_json.encode(), None)
    blob = base64.b64encode(nonce + ciphertext_with_tag).decode()
    return json.dumps({"type": "enc", "d": blob})


def decrypt_message(key_b64: str, msg_str: str) -> str:
    """Decrypt a relay message; pass through unchanged if not encrypted.

    Args:
        key_b64: Base64-encoded 32-byte AES-256 key.
        msg_str: JSON string received from the relay.

    Returns:
        Decrypted inner JSON string, or original msg_str if not encrypted.

    Raises:
        cryptography.exceptions.InvalidTag: if ciphertext was tampered with.
    """
    msg = json.loads(msg_str)
    if msg.get("type") != "enc":
        return msg_str

    key = base64.b64decode(key_b64)
    blob = base64.b64decode(msg["d"])
    nonce = blob[:12]
    ciphertext_with_tag = blob[12:]
    aesgcm = AESGCM(key)
    plaintext = aesgcm.decrypt(nonce, ciphertext_with_tag, None)
    return plaintext.decode()
