"""
streamrelay.producer — Send tokens from an HPC compute node to the relay.

WHAT THIS FILE DOES
===================
This module runs on the HPC COMPUTE NODE — the machine actually running your
model or computation. Its job is to:

  1. Connect outbound to the relay server (WebSocket connection).
  2. Send each token (or any piece of output) to the relay as it is generated.
  3. Signal when generation is complete.
  4. Handle errors gracefully so the consumer is never left waiting.

The relay then forwards each message to the consumer (your application) in
real time.

WHO USES THIS
=============
You use RelayProducer inside whatever code runs on the compute node:

  - A SLURM batch script calling a vLLM or HuggingFace model
  - A PBS job running scientific simulation code
  - A Globus Compute function dispatched to a remote HPC endpoint
  - Any subprocess that has outbound network access

The key requirement is just that the compute node can make outbound TCP
connections to the relay server's port — which is true at most HPC centers
(it's the same mechanism Globus Compute uses for its own AMQP connections).

SYNC VS ASYNC
=============
Two context managers are provided:

  with RelayProducer(...) as relay:        ← synchronous (blocking)
      relay.send_token(token)

  async with RelayProducer(...) as relay:  ← asynchronous (asyncio)
      await relay._async_send_raw(...)

Use the synchronous version inside plain Python functions (e.g. a Globus
Compute function, a SLURM job script). Use the async version if your code
already runs in an asyncio event loop.

EXAMPLE (synchronous, inside a SLURM job or Globus Compute function)
=====================================================================
  from streamrelay import RelayProducer

  def my_inference_function(prompt, relay_url, channel_id, relay_secret=""):
      # All imports must be inside the function body for Globus Compute,
      # which serializes and ships this function to the remote endpoint.
      import requests

      with RelayProducer(relay_url, channel_id, relay_secret=relay_secret) as relay:
          response = requests.post(vllm_url, json={...}, stream=True)
          for line in response.iter_lines():
              token = parse_sse_line(line)
              if token:
                  relay.send_token(token)
      # The "done" signal is sent automatically when the `with` block exits.
"""

import json
import logging

logger = logging.getLogger(__name__)


class RelayProducer:
    """
    WebSocket client that sends tokens to the relay from the compute node.

    Args:
        relay_url: WebSocket URL of the relay server.
            Example: ``"wss://relay.example.com"`` (production)
                  or ``"ws://localhost:8765"`` (local development)
        channel_id: UUID string that pairs this producer with its consumer.
            Generate with ``str(uuid.uuid4())`` before submitting the job,
            then pass the same value to RelayConsumer on the client side.
        encryption_key: Optional base64-encoded 32-byte AES-256 key.
            When set, every message is encrypted with AES-256-GCM before
            being sent to the relay, so the relay server itself cannot read
            the token content. Generate a key with:
            ``python -c "from streamrelay import generate_key; print(generate_key())"``
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
        self._ws = None       # synchronous WebSocket connection
        self._ws_cm = None    # async WebSocket context manager

    # -----------------------------------------------------------------------
    # Internal helpers
    # -----------------------------------------------------------------------

    def _produce_url(self) -> str:
        """Build the /produce/{channel_id} URL."""
        return f"{self.relay_url}/produce/{self.channel_id}"

    def _encode(self, payload: dict) -> str:
        """
        Serialize a dict to JSON, then optionally encrypt it.

        If encryption_key is set, the JSON string is encrypted with
        AES-256-GCM and wrapped in {"type": "enc", "d": "<base64blob>"}.
        The relay forwards this opaque blob without being able to read it.
        The consumer decrypts it using the same key.
        """
        raw = json.dumps(payload)
        if self.encryption_key:
            from streamrelay.crypto import encrypt_message
            return encrypt_message(self.encryption_key, raw)
        return raw

    # -----------------------------------------------------------------------
    # Synchronous context manager — for use in plain Python functions
    # -----------------------------------------------------------------------

    def __enter__(self):
        """
        Open a synchronous WebSocket connection to the relay.
        Called when entering a ``with RelayProducer(...) as relay:`` block.
        """
        from websockets.sync.client import connect as ws_connect
        self._ws = ws_connect(self._produce_url())
        if self.relay_secret:
            self._ws.send(json.dumps({"type": "auth", "secret": self.relay_secret}))
        logger.debug(f"[streamrelay] producer connected: channel={self.channel_id[:8]}")
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """
        Called when leaving the ``with`` block — whether normally or due to
        an exception. Sends the appropriate closing message and closes the
        WebSocket.

        If an exception occurred (exc_type is not None), sends an error
        message followed by "done" so the consumer knows to stop waiting.
        If no exception, sends "done" normally.
        """
        if exc_type is not None:
            # Something went wrong inside the with block — notify the consumer.
            try:
                self._send_raw({"type": "error", "message": f"{exc_type.__name__}: {exc_val}"})
                self._send_raw({"type": "done"})
            except Exception:
                pass  # best effort — connection may already be broken
        else:
            try:
                self.send_done()
            except Exception:
                pass
        self.close()
        return False  # do not suppress the exception

    def connect(self):
        """Explicitly open the synchronous WebSocket connection.
        Only needed if not using the context manager."""
        from websockets.sync.client import connect as ws_connect
        self._ws = ws_connect(self._produce_url())
        if self.relay_secret:
            self._ws.send(json.dumps({"type": "auth", "secret": self.relay_secret}))
        logger.debug(f"[streamrelay] producer connected: channel={self.channel_id[:8]}")

    def close(self):
        """Close the synchronous WebSocket connection."""
        if self._ws is not None:
            try:
                self._ws.close()
            except Exception:
                pass
            self._ws = None

    def _send_raw(self, payload: dict):
        """Encode (and optionally encrypt) a dict and send it over the WebSocket."""
        if self._ws is None:
            raise RuntimeError("Not connected. Use 'with RelayProducer(...) as relay:'")
        self._ws.send(self._encode(payload))

    def send_token(self, content: str):
        """
        Send a single token (text chunk) to the relay.

        Call this inside the ``with`` block for each piece of output your
        model generates. The relay forwards it to the consumer immediately.

        Args:
            content: The token text, e.g. ``"Hello"`` or ``" world"``.
        """
        self._send_raw({"type": "token", "content": content})

    def send_done(self, usage: dict | None = None):
        """
        Signal that generation is complete.

        This is called automatically when the ``with`` block exits normally.
        You only need to call it explicitly if you are not using the context
        manager.

        Args:
            usage: Optional token usage statistics from your model, e.g.:
                ``{"prompt_tokens": 10, "completion_tokens": 50, "total_tokens": 60}``
        """
        self._send_raw({"type": "done", "usage": usage or {}})

    def send_error(self, message: str):
        """
        Report an error to the consumer.

        Also sends "done" immediately after so the consumer is not left
        waiting indefinitely for a stream that will never arrive.

        Args:
            message: Human-readable error description.
        """
        self._send_raw({"type": "error", "message": message})
        self._send_raw({"type": "done"})

    # -----------------------------------------------------------------------
    # Asynchronous context manager — for use in asyncio code
    # -----------------------------------------------------------------------

    async def __aenter__(self):
        """
        Open an async WebSocket connection to the relay.
        Called when entering an ``async with RelayProducer(...) as relay:`` block.
        Used when your code already runs in an asyncio event loop.
        """
        from websockets.asyncio.client import connect as ws_connect
        self._ws_cm = ws_connect(self._produce_url())
        self._ws = await self._ws_cm.__aenter__()
        if self.relay_secret:
            await self._ws.send(json.dumps({"type": "auth", "secret": self.relay_secret}))
        logger.debug(f"[streamrelay] async producer connected: channel={self.channel_id[:8]}")
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Async equivalent of __exit__ — sends done/error and closes."""
        if exc_type is not None:
            try:
                await self._async_send_raw({"type": "error", "message": f"{exc_type.__name__}: {exc_val}"})
                await self._async_send_raw({"type": "done"})
            except Exception:
                pass
        else:
            try:
                await self._async_send_raw({"type": "done", "usage": {}})
            except Exception:
                pass
        await self._ws_cm.__aexit__(exc_type, exc_val, exc_tb)
        self._ws = None
        return False

    async def _async_send_raw(self, payload: dict):
        """Async version of _send_raw — used inside the async context manager."""
        raw = json.dumps(payload)
        if self.encryption_key:
            from streamrelay.crypto import encrypt_message
            raw = encrypt_message(self.encryption_key, raw)
        await self._ws.send(raw)
