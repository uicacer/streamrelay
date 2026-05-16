"""Tests for AES-256-GCM encryption in streamrelay.crypto."""

import base64
import json
import os

import pytest

from streamrelay.crypto import decrypt_message, encrypt_message, generate_key


def make_key() -> str:
    return base64.b64encode(os.urandom(32)).decode()


def test_round_trip():
    key = make_key()
    original = json.dumps({"type": "token", "content": "Hello, HPC!"})
    assert decrypt_message(key, encrypt_message(key, original)) == original


def test_encrypted_format():
    key = make_key()
    enc = json.loads(encrypt_message(key, json.dumps({"type": "token", "content": "x"})))
    assert enc["type"] == "enc"
    assert "d" in enc


def test_fresh_nonce_each_call():
    key = make_key()
    msg = json.dumps({"type": "token", "content": "same"})
    enc1 = json.loads(encrypt_message(key, msg))["d"]
    enc2 = json.loads(encrypt_message(key, msg))["d"]
    assert enc1 != enc2


def test_passthrough_non_enc():
    key = make_key()
    msg = json.dumps({"type": "done"})
    assert decrypt_message(key, msg) == msg


def test_wrong_key_raises():
    key1 = make_key()
    key2 = make_key()
    msg = json.dumps({"type": "token", "content": "secret"})
    encrypted = encrypt_message(key1, msg)
    with pytest.raises(Exception):
        decrypt_message(key2, encrypted)


def test_generate_key_length():
    key = generate_key()
    assert len(base64.b64decode(key)) == 32
