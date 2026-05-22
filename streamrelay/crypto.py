"""
streamrelay.crypto — AES-256-GCM end-to-end encryption for relay messages.

WHAT THIS FILE DOES
===================
The relay server is a public intermediary: it sees every message that flows
between the producer (HPC node) and the consumer (your application). By default
that means the relay operator can read token payloads.

This module adds optional end-to-end encryption so that the relay only ever
sees opaque ciphertext. The producer encrypts before sending; the consumer
decrypts after receiving. The relay cannot read anything.

WHY AES-256-GCM
===============
AES-256-GCM is the standard choice for this use case:

  - AES-256: 256-bit key — computationally unbreakable with current hardware.
  - GCM (Galois/Counter Mode): "authenticated encryption" — provides both
    confidentiality (nobody can read the message) AND integrity (any tampering
    at the relay is detected and raises an exception at decrypt time).
  - Fresh nonce per message: GCM requires a unique nonce (number-used-once)
    for every encryption. We use os.urandom(12) — cryptographically random
    12 bytes — so even if you send the same token twice, the ciphertexts are
    different. This prevents replay and pattern-analysis attacks.

WIRE FORMAT
===========
An encrypted message is a JSON string wrapping a single base64-encoded blob:

  {"type": "enc", "d": "<base64(nonce[12 bytes] + ciphertext + tag[16 bytes])>"}

The nonce (12 bytes) and GCM authentication tag (16 bytes) are packed together
with the ciphertext into a single base64 blob. This makes it easy to pass over
JSON without any binary escaping.

The relay forwards this JSON string unchanged. It doesn't know or care that
it contains encrypted data.

BACKWARD COMPATIBILITY
======================
If decrypt_message() receives a message that is NOT of type "enc" (i.e. an
unencrypted message), it passes it through unchanged. This means you can
enable encryption on a running system without breaking existing unencrypted
connections.

SETUP
=====
Generate a key once and share it between the producer and consumer via
environment variables or a secrets manager:

  python -c "from streamrelay import generate_key; print(generate_key())"
  # Outputs 64 hex characters, e.g.: 10f203a90e9f55549169c6af...
  # Hex is used (not base64) to avoid +/= characters that break shell exports and .env files.

Store it in your .env file:
  RELAY_ENCRYPTION_KEY=10f203a90e9f55549169c6af...

Then pass it to both sides:
  RelayProducer(relay_url, channel_id, encryption_key=os.getenv("RELAY_ENCRYPTION_KEY"))
  RelayConsumer(relay_url, channel_id, encryption_key=os.getenv("RELAY_ENCRYPTION_KEY"))
"""

import base64
import json
import os

from cryptography.hazmat.primitives.ciphers.aead import AESGCM

# Standard sizes for AES-GCM (defined by NIST SP 800-38D)
_NONCE_SIZE = 12  # 96 bits — the recommended nonce length for GCM
_TAG_SIZE = 16    # 128 bits — GCM appends this authentication tag automatically


def generate_key() -> str:
    """
    Generate a random AES-256 encryption key.

    Returns a hex-encoded string (64 characters, 0-9 and a-f only) suitable for
    storing in a .env file or passing as an environment variable without quoting.
    Hex is used rather than base64 to avoid the +, /, and = characters that cause
    problems in shell variable exports, YAML worker_init blocks, and some .env parsers.

    Run this once per deployment and keep the key secret.

    Returns:
        str: Hex-encoded 32-byte (256-bit) key, e.g. ``"10f203a90e9f5554..."``

    Example::

        from streamrelay import generate_key
        key = generate_key()
        print(key)   # store this in your .env as RELAY_ENCRYPTION_KEY
    """
    return os.urandom(32).hex()
    # os.urandom(32): 32 cryptographically random bytes from the OS entropy pool
    # .hex(): converts raw bytes to a 64-character lowercase hex string


def encrypt_message(key_hex: str, plaintext_json: str) -> str:
    """
    Encrypt a JSON string and return the relay wire format.

    Takes a plaintext JSON message (e.g. ``'{"type":"token","content":"Hello"}'``)
    and returns an encrypted JSON string in the relay wire format:
    ``'{"type": "enc", "d": "<base64blob>"}'``

    The relay forwards this opaque blob. The consumer calls decrypt_message()
    to recover the original plaintext.

    Args:
        key_hex: Hex-encoded 32-byte AES-256 key (from generate_key()).
        plaintext_json: Any JSON string to encrypt.

    Returns:
        str: JSON string ``{"type": "enc", "d": "<base64(nonce+ciphertext+tag)>"}``
    """
    key = bytes.fromhex(key_hex)             # decode hex key → 32 raw bytes
    nonce = os.urandom(_NONCE_SIZE)          # fresh random nonce for every message

    aesgcm = AESGCM(key)
    # aesgcm.encrypt() returns ciphertext + authentication tag (tag is appended
    # automatically by the GCM implementation — we don't need to handle it separately)
    ciphertext_with_tag = aesgcm.encrypt(nonce, plaintext_json.encode(), None)

    # Pack nonce + ciphertext+tag into one base64 string.
    # The recipient needs the nonce to decrypt, so it must travel with the message.
    blob = base64.b64encode(nonce + ciphertext_with_tag).decode()
    return json.dumps({"type": "enc", "d": blob})


def decrypt_message(key_hex: str, msg_str: str) -> str:
    """
    Decrypt a relay message, or pass through if it is not encrypted.

    If the message has ``"type": "enc"``, decrypt it and return the original
    plaintext JSON string. If the message has any other type, return it unchanged
    (backward-compatible passthrough for unencrypted messages).

    Args:
        key_hex: Hex-encoded 32-byte AES-256 key (must match the producer's key).
        msg_str: JSON string received from the relay.

    Returns:
        str: Decrypted inner JSON string, or original ``msg_str`` if not encrypted.

    Raises:
        cryptography.exceptions.InvalidTag: If the ciphertext was tampered with.
            This means the relay (or someone in between) modified the message.
            Treat this as a security event.
    """
    msg = json.loads(msg_str)

    if msg.get("type") != "enc":
        # Not an encrypted message — pass through unchanged.
        # This allows the consumer to handle both encrypted and unencrypted
        # messages on the same channel (useful during a rolling migration).
        return msg_str

    # Decode the blob back into raw bytes
    blob = base64.b64decode(msg["d"])

    # Unpack: first 12 bytes are the nonce, the rest is ciphertext+tag
    nonce = blob[:_NONCE_SIZE]
    ciphertext_with_tag = blob[_NONCE_SIZE:]

    key = bytes.fromhex(key_hex)
    aesgcm = AESGCM(key)

    # aesgcm.decrypt() verifies the GCM authentication tag before decrypting.
    # If the ciphertext was modified in any way, it raises InvalidTag instead
    # of returning corrupted plaintext — this is the integrity guarantee.
    plaintext = aesgcm.decrypt(nonce, ciphertext_with_tag, None)
    return plaintext.decode()
