"""
streamrelay.executor — High-level API for streaming from Globus Compute.

This is the primary user-facing class. It wraps channel ID management, Globus
job submission, and relay consumption into a single ``async for`` loop::

    from streamrelay import StreamingExecutor

    async with StreamingExecutor(endpoint_id, relay_url, secret, key) as executor:
        async for token in executor.stream(my_vllm_function, prompt="Hello"):
            print(token, end="", flush=True)

The ``stream()`` method:
  1. Generates a random channel ID.
  2. Submits ``fn`` to the Globus Compute endpoint with the channel ID and relay
     URL passed as extra keyword arguments.
  3. Immediately connects to the relay as a consumer and yields tokens as they
     arrive — without waiting for Globus to complete.

``fn`` must accept two extra kwargs automatically injected by the executor:
  ``relay_url`` (str) and ``channel_id`` (str).
  Optionally also ``relay_secret`` and ``encryption_key`` if you set those.

If ``streamrelay`` is installed on the HPC endpoint workers, ``fn`` can use
``RelayProducer`` directly. If not, embed the inline pattern from
``remote_vllm_streaming`` in STREAM's ``globus_compute_client.py``.
"""

import uuid
from collections.abc import AsyncIterator
from typing import Callable


class StreamingExecutor:
    """Submit a Globus Compute function and receive its output via relay.

    Args:
        endpoint_id: Globus Compute endpoint UUID.
        relay_url: WebSocket URL of the relay server.
        relay_secret: Optional shared secret (must match relay's ``--secret``).
        encryption_key: Optional base64 AES-256 key for E2E encryption.
        consumer_timeout: Seconds to wait for the first token before timing out.
    """

    def __init__(
        self,
        endpoint_id: str,
        relay_url: str,
        relay_secret: str = "",
        encryption_key: str = "",
        consumer_timeout: float = 300.0,
    ):
        self.endpoint_id = endpoint_id
        self.relay_url = relay_url
        self.relay_secret = relay_secret
        self.encryption_key = encryption_key
        self.consumer_timeout = consumer_timeout
        self._executor = None

    # ------------------------------------------------------------------
    # Context manager
    # ------------------------------------------------------------------

    async def __aenter__(self):
        return self

    async def __aexit__(self, *args):
        self.close()

    # ------------------------------------------------------------------
    # Lazy Globus executor
    # ------------------------------------------------------------------

    def _get_gc_executor(self):
        if self._executor is None:
            from globus_compute_sdk import Executor

            self._executor = Executor(endpoint_id=self.endpoint_id)
        return self._executor

    def close(self):
        """Shut down the underlying Globus Compute executor."""
        if self._executor is not None:
            try:
                self._executor.shutdown(wait=False)
            except Exception:
                pass
            self._executor = None

    # ------------------------------------------------------------------
    # Main API
    # ------------------------------------------------------------------

    async def stream(self, fn: Callable, *args, **kwargs) -> AsyncIterator[str]:
        """Submit ``fn`` to the endpoint and stream its output token by token.

        ``fn`` will be called on the HPC node with ``*args, **kwargs`` PLUS
        these additional keyword arguments injected automatically:

        - ``relay_url`` — where to send tokens
        - ``channel_id`` — this request's unique channel
        - ``relay_secret`` — auth secret (if configured)
        - ``encryption_key`` — E2E encryption key (if configured)

        Args:
            fn: Callable to submit. Must send tokens to the relay
                (e.g., use :class:`~streamrelay.producer.RelayProducer`).
            *args: Positional arguments forwarded to ``fn``.
            **kwargs: Keyword arguments forwarded to ``fn``.

        Yields:
            str: Token strings in arrival order.
        """
        channel_id = str(uuid.uuid4())

        # Inject relay coordinates into the function's kwargs
        kwargs["relay_url"] = self.relay_url
        kwargs["channel_id"] = channel_id
        if self.relay_secret:
            kwargs["relay_secret"] = self.relay_secret
        if self.encryption_key:
            kwargs["encryption_key"] = self.encryption_key

        # Submit to Globus Compute (non-blocking — returns a Future immediately)
        gc = self._get_gc_executor()
        future = gc.submit(fn, *args, **kwargs)

        # Connect as consumer and yield tokens in real time.
        # The relay buffers any tokens that arrive before we connect.
        from streamrelay.consumer import RelayConsumer

        consumer = RelayConsumer(
            relay_url=self.relay_url,
            channel_id=channel_id,
            encryption_key=self.encryption_key,
            relay_secret=self.relay_secret,
        )
        async for token in consumer.astream():
            yield token

        # After streaming, check for Globus-level errors (infrastructure faults).
        # By this point the HPC function has already completed.
        try:
            future.result(timeout=10)
        except Exception as e:
            raise RuntimeError(f"Globus Compute reported an error: {e}") from e
