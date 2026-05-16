"""
streamrelay.consumer — Receive tokens from a relay channel.

This module runs on the client side (middleware, notebook, CLI) and connects
to the relay as a consumer. It yields tokens in real time as the producer
(running on HPC) sends them.

Both ``sync`` and ``async`` iterators are provided:

- ``RelayConsumer.stream()``  — synchronous, works in plain Python scripts
- ``RelayConsumer.astream()`` — async generator, for FastAPI / asyncio

Usage (sync)::

    from streamrelay import RelayConsumer

    consumer = RelayConsumer(relay_url, channel_id, encryption_key)
    for token in consumer.stream():
        print(token, end="", flush=True)

Usage (async)::

    async for token in consumer.astream():
        yield f"data: {token}\\n\\n"
"""

import json
import logging
from typing import AsyncIterator, Iterator

logger = logging.getLogger(__name__)


class RelayConsumer:
    """
    WebSocket consumer that receives tokens from a relay channel.

    Args:
        relay_url: WebSocket URL of the relay server, e.g. ``ws://relay.example.com:8765``.
        channel_id: Unique channel identifier (UUID). Must match the producer's channel_id.
        encryption_key: Optional base64-encoded 32-byte AES-256 key.
            Must match the producer's key for decryption.
        relay_secret: Optional shared secret for relay authentication.
    """

    def __init__(
        self,
        relay_url: str,
        channel_id: str,
        encryption_key: str = "",
        relay_secret: str = "",
    ):
        self.relay_url = relay_url.rstrip("/")
        self.channel_id = channel_id
        self.encryption_key = encryption_key
        self.relay_secret = relay_secret

    def _consume_url(self) -> str:
        url = f"{self.relay_url}/consume/{self.channel_id}"
        if self.relay_secret:
            url += f"?secret={self.relay_secret}"
        return url

    def _decrypt(self, msg_str: str) -> str:
        """Decrypt if a key is configured; otherwise pass through."""
        if self.encryption_key:
            from streamrelay.crypto import decrypt_message
            return decrypt_message(self.encryption_key, msg_str)
        return msg_str

    # ------------------------------------------------------------------
    # Synchronous iterator
    # ------------------------------------------------------------------

    def stream(self) -> Iterator[str]:
        """Yield token strings synchronously until the stream is done.

        Connects to the relay, yields each token as it arrives, and returns
        when the producer sends a ``done`` message.

        Yields:
            str: Each token string in arrival order.

        Raises:
            RuntimeError: If the producer sent an error message.
        """
        from websockets.sync.client import connect as ws_connect

        url = self._consume_url()
        logger.debug(f"[streamrelay] consumer connecting: channel={self.channel_id[:8]}")
        with ws_connect(url) as ws:
            for raw in ws:
                msg_str = self._decrypt(raw)
                msg = json.loads(msg_str)
                msg_type = msg.get("type")

                if msg_type == "token":
                    yield msg["content"]

                elif msg_type == "done":
                    break

                elif msg_type == "error":
                    logger.error(
                        f"[streamrelay] remote error on channel {self.channel_id[:8]}: "
                        f"{msg.get('message')}"
                    )
                    raise RuntimeError(f"Producer error: {msg.get('message')}")

    # ------------------------------------------------------------------
    # Asynchronous iterator
    # ------------------------------------------------------------------

    def __aiter__(self):
        """Enable ``async for token in RelayConsumer(...)`` syntax."""
        return self.astream()

    async def astream(self) -> AsyncIterator[str]:
        """Yield token strings asynchronously until the stream is done.

        Connects to the relay using the async websockets client. Suitable
        for use inside FastAPI route handlers and other asyncio contexts.

        Yields:
            str: Each token string in arrival order.

        Raises:
            RuntimeError: If the producer sent an error message.
        """
        from websockets.asyncio.client import connect as ws_connect

        url = self._consume_url()
        logger.debug(f"[streamrelay] async consumer connecting: channel={self.channel_id[:8]}")
        async with ws_connect(url) as ws:
            async for raw in ws:
                msg_str = self._decrypt(raw)
                msg = json.loads(msg_str)
                msg_type = msg.get("type")

                if msg_type == "token":
                    yield msg["content"]

                elif msg_type == "done":
                    break

                elif msg_type == "error":
                    logger.error(
                        f"[streamrelay] remote error on channel {self.channel_id[:8]}: "
                        f"{msg.get('message')}"
                    )
                    raise RuntimeError(f"Producer error: {msg.get('message')}")

    # ------------------------------------------------------------------
    # Convenience: collect all tokens into a string
    # ------------------------------------------------------------------

    def collect(self) -> str:
        """Stream all tokens and join them into a single string (blocking).

        Returns:
            str: The complete generated text.
        """
        return "".join(self.stream())

    async def acollect(self) -> str:
        """Async version of :meth:`collect`.

        Returns:
            str: The complete generated text.
        """
        parts = []
        async for token in self.astream():
            parts.append(token)
        return "".join(parts)
