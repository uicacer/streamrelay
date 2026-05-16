"""
streamrelay.consumer — Receive tokens from the relay on the client side.

WHAT THIS FILE DOES
===================
This module runs on YOUR MACHINE (or your web server) — the application side
that wants to display or process tokens as they arrive from the HPC compute node.

RelayConsumer connects to the relay as a consumer and yields token strings one
by one as the producer sends them. From the caller's perspective, it looks just
like iterating over any Python generator:

  for token in consumer.stream():
      print(token, end="", flush=True)

All the WebSocket complexity — connecting, receiving, decrypting, parsing JSON,
detecting the end of the stream — is handled internally.

TWO ITERATION STYLES
====================
  # Synchronous — use in plain scripts, notebooks, CLI tools
  consumer = RelayConsumer(relay_url, channel_id)
  for token in consumer.stream():
      print(token, end="", flush=True)

  # Asynchronous — use in FastAPI, aiohttp, or any asyncio application
  async for token in RelayConsumer(relay_url, channel_id):
      yield f"data: {token}\\n\\n"   # forward as Server-Sent Events to browser

The async version also supports ``await consumer.acollect()`` to get the full
response as a single string.

TIMING
======
You should connect the consumer BEFORE submitting the HPC job. The relay
buffers any tokens that arrive before you connect, so you won't miss the
beginning of the response even if the job starts faster than expected.

  channel_id = str(uuid.uuid4())

  # Submit job first, consumer second — or consumer first, doesn't matter.
  # The relay handles both orderings via buffering.
  submit_slurm_job(relay_url, channel_id)
  for token in RelayConsumer(relay_url, channel_id).stream():
      ...
"""

import json
import logging
from typing import AsyncIterator, Iterator

logger = logging.getLogger(__name__)


class RelayConsumer:
    """
    WebSocket client that receives tokens from the relay.

    Args:
        relay_url: WebSocket URL of the relay server.
            Example: ``"wss://relay.example.com"`` (production)
                  or ``"ws://localhost:8765"`` (local development)
        channel_id: UUID string that pairs this consumer with its producer.
            Must be the same value that was passed to RelayProducer.
        encryption_key: Optional base64-encoded AES-256 key for decryption.
            Must match the key used by the producer. When set, each received
            message is decrypted before being parsed and yielded.
        relay_secret: Optional shared secret for relay authentication.
            Must match the relay server's ``--secret`` flag.
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

    # -----------------------------------------------------------------------
    # Internal helpers
    # -----------------------------------------------------------------------

    def _consume_url(self) -> str:
        """Build the /consume/{channel_id} URL, appending ?secret= if needed."""
        url = f"{self.relay_url}/consume/{self.channel_id}"
        if self.relay_secret:
            url += f"?secret={self.relay_secret}"
        return url

    def _decrypt(self, msg_str: str) -> str:
        """
        Decrypt a message if encryption is configured; otherwise pass through.

        The relay forwards messages as-is. If the producer encrypted them with
        AES-256-GCM (wrapping in {"type":"enc","d":"<base64blob>"}), this
        function unwraps and decrypts them back to the original JSON string.
        """
        if self.encryption_key:
            from streamrelay.crypto import decrypt_message
            return decrypt_message(self.encryption_key, msg_str)
        return msg_str

    def _parse_and_yield(self, raw: str):
        """
        Parse a raw WebSocket message and return the appropriate action.

        Returns:
          ("token", content)   — yield this token string to the caller
          ("done", None)       — stop iteration
          ("error", message)   — raise RuntimeError
          ("skip", None)       — ignore this message (unknown type)
        """
        msg_str = self._decrypt(raw)
        msg = json.loads(msg_str)
        msg_type = msg.get("type")

        if msg_type == "token":
            return ("token", msg["content"])
        elif msg_type == "done":
            return ("done", None)
        elif msg_type == "error":
            return ("error", msg.get("message", "unknown error from producer"))
        else:
            return ("skip", None)

    # -----------------------------------------------------------------------
    # Synchronous iterator
    # -----------------------------------------------------------------------

    def stream(self) -> Iterator[str]:
        """
        Connect to the relay and yield token strings synchronously.

        Blocks until each token arrives. Returns (stops iteration) when the
        producer sends a "done" message. The WebSocket connection is closed
        automatically when the generator exits.

        Yields:
            str: Each token string in arrival order.

        Raises:
            RuntimeError: If the producer sent an "error" message.

        Example::

            for token in RelayConsumer(relay_url, channel_id).stream():
                print(token, end="", flush=True)
        """
        from websockets.sync.client import connect as ws_connect

        url = self._consume_url()
        logger.debug(f"[streamrelay] consumer connecting: channel={self.channel_id[:8]}")

        with ws_connect(url) as ws:
            for raw in ws:
                # Each raw message from the relay is one JSON string.
                action, value = self._parse_and_yield(raw)
                if action == "token":
                    yield value
                elif action == "done":
                    return  # clean end of stream
                elif action == "error":
                    raise RuntimeError(f"Producer error: {value}")
                # "skip": unknown message type, ignore and continue

    # -----------------------------------------------------------------------
    # Asynchronous iterator
    # -----------------------------------------------------------------------

    def __aiter__(self):
        """
        Enable ``async for token in RelayConsumer(...)`` syntax.

        Returns the async generator from astream(). This lets you use a
        RelayConsumer directly in an ``async for`` loop without calling
        .astream() explicitly.
        """
        return self.astream()

    async def astream(self) -> AsyncIterator[str]:
        """
        Connect to the relay and yield token strings asynchronously.

        Non-blocking: yields control to the event loop while waiting for
        each token. Suitable for FastAPI route handlers, aiohttp servers,
        or any asyncio application.

        Yields:
            str: Each token string in arrival order.

        Raises:
            RuntimeError: If the producer sent an "error" message.

        Example (FastAPI SSE endpoint)::

            @app.get("/stream")
            async def stream():
                async def generate():
                    async for token in RelayConsumer(relay_url, channel_id):
                        yield f"data: {token}\\n\\n"
                return StreamingResponse(generate(), media_type="text/event-stream")
        """
        from websockets.asyncio.client import connect as ws_connect

        url = self._consume_url()
        logger.debug(f"[streamrelay] async consumer connecting: channel={self.channel_id[:8]}")

        async with ws_connect(url) as ws:
            async for raw in ws:
                action, value = self._parse_and_yield(raw)
                if action == "token":
                    yield value
                elif action == "done":
                    return
                elif action == "error":
                    raise RuntimeError(f"Producer error: {value}")

    # -----------------------------------------------------------------------
    # Convenience: collect the full response as a single string
    # -----------------------------------------------------------------------

    def collect(self) -> str:
        """
        Stream all tokens and join them into a single string (blocking).

        Useful when you want the complete response but don't need to display
        it incrementally.

        Returns:
            str: The complete generated text.
        """
        return "".join(self.stream())

    async def acollect(self) -> str:
        """
        Async version of collect().

        Returns:
            str: The complete generated text.
        """
        parts = []
        async for token in self.astream():
            parts.append(token)
        return "".join(parts)
