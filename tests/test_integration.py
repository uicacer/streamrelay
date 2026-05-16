"""
End-to-end integration tests: producer → relay → consumer.

Starts a local relay server on port 18765, then runs produce/consume pairs
through it to verify the full pipeline including encryption and buffering.
"""

import asyncio
import base64
import os
import uuid

import pytest

from streamrelay.consumer import RelayConsumer
from streamrelay.producer import RelayProducer
from streamrelay.server import start_relay

RELAY_PORT = 18765
RELAY_URL = f"ws://127.0.0.1:{RELAY_PORT}"


@pytest.fixture(scope="session", autouse=True)
async def relay_server():
    task = asyncio.create_task(
        start_relay(host="127.0.0.1", port=RELAY_PORT, secret="", max_buffer=100, channel_timeout=10)
    )
    await asyncio.sleep(0.2)
    yield
    task.cancel()
    try:
        await task
    except asyncio.CancelledError:
        pass


async def _run(tokens: list[str], key: str = "", secret: str = "") -> list[str]:
    """Helper: produce `tokens` and collect them via consumer."""
    channel_id = str(uuid.uuid4())
    received = []

    async def consume():
        async for token in RelayConsumer(RELAY_URL, channel_id, encryption_key=key, relay_secret=secret):
            received.append(token)

    async def produce():
        await asyncio.sleep(0.05)
        async with RelayProducer(RELAY_URL, channel_id, encryption_key=key, relay_secret=secret) as p:
            for t in tokens:
                await p._async_send_raw({"type": "token", "content": t})
        # done is sent by __aexit__

    await asyncio.gather(consume(), produce())
    return received


async def test_basic():
    tokens = ["Hello", " ", "world", "!"]
    assert await _run(tokens) == tokens


async def test_buffering_producer_first():
    """Producer completes before consumer connects — buffered tokens must be delivered."""
    channel_id = str(uuid.uuid4())
    tokens = ["a", "b", "c"]
    received = []

    async def produce():
        async with RelayProducer(RELAY_URL, channel_id) as p:
            for t in tokens:
                await p._async_send_raw({"type": "token", "content": t})

    async def consume():
        await asyncio.sleep(0.4)
        async for token in RelayConsumer(RELAY_URL, channel_id):
            received.append(token)

    await asyncio.gather(produce(), consume())
    assert received == tokens


async def test_encrypted():
    key = base64.b64encode(os.urandom(32)).decode()
    tokens = ["secret", " ", "data"]
    assert await _run(tokens, key=key) == tokens


async def test_empty_stream():
    assert await _run([]) == []
