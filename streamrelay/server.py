"""
streamrelay.server — The WebSocket relay server.

WHAT THIS FILE DOES
===================
This is the central piece of the streamrelay architecture. It is a lightweight
server that sits between two parties:

  - The PRODUCER: a process running on an HPC compute node that generates tokens
    (e.g., an LLM running via vLLM on a GPU node).

  - The CONSUMER: the user-facing application that wants to display those tokens
    in real time (e.g., a web server, a Jupyter notebook, a CLI).

The relay's only job is to forward messages from the producer to the consumer.
It does not compute anything, store data permanently, or understand the content
of the messages.

WHY A RELAY AT ALL?
===================
Both parties face a firewall problem:

  - HPC compute nodes are behind institutional firewalls. They cannot accept
    inbound connections. Only outbound connections are allowed (this is how
    Globus Compute's own AMQP connections work too).

  - The user's application may also be behind a NAT or firewall and cannot
    accept inbound connections either.

The solution: put a relay server on a machine that both sides CAN reach (a small
cloud VM or campus server with a public IP), and have BOTH sides connect OUTBOUND
to it. The relay then bridges the two connections.

  HPC node  ──outbound──►  relay  ◄──outbound──  your app
                              │
                         forwards tokens

This is the same principle used by TURN servers in WebRTC video calls.

THE CHANNEL CONCEPT
===================
A "channel" is a matched pair of connections identified by a random UUID
(the channel_id). The producer connects to /produce/{channel_id} and the
consumer connects to /consume/{channel_id}. The relay matches them by ID and
forwards all messages from producer to consumer.

Channel IDs have 122 bits of entropy (UUID4) — they are computationally
impossible to guess, so channel isolation is guaranteed without any additional
access control (though a shared secret adds an extra layer for production use).

THE MESSAGE PROTOCOL
====================
All messages are JSON strings. The relay forwards them unchanged:

  {"type": "token",  "content": "Hello"}    ← one chunk of generated text
  {"type": "done",   "usage": {...}}         ← generation is complete
  {"type": "error",  "message": "..."}       ← something went wrong on HPC

When end-to-end encryption is enabled (see crypto.py), each message is wrapped:
  {"type": "enc", "d": "<base64(nonce + ciphertext + GCM auth tag)>"}

BUFFERING
=========
The consumer (your app) typically connects to the relay BEFORE the producer
(HPC node), because submitting the job takes a few seconds. To handle the
opposite case — where the producer starts sending tokens before the consumer
connects — the relay buffers messages in memory and flushes them when the
consumer arrives.

URL SCHEME
==========
  /produce/{channel_id}   — register as producer
  /consume/{channel_id}   — register as consumer
  /health                 — health check (no auth required)

AUTH PROTOCOL
=============
When a shared secret is configured, the client sends it as the FIRST JSON
message after the WebSocket handshake (not as a URL query parameter):

  {"type": "auth", "secret": "<value>"}

URL query parameters (?secret=) are logged by reverse proxies even over
wss://, so they must not carry secrets. Post-handshake auth keeps the
secret out of all log files.

USAGE
=====
  # From Python
  import asyncio
  from streamrelay import start_relay
  asyncio.run(start_relay(port=8765, secret="my-secret"))

  # From the command line
  streamrelay --host 0.0.0.0 --port 8765 --secret my-secret

  # Development: expose localhost via a free Cloudflare tunnel
  streamrelay --port 8765 &
  cloudflared tunnel --url http://localhost:8765
"""

import argparse
import asyncio
import json
import logging
import os
import time as _time
from datetime import UTC, datetime
from urllib.parse import parse_qs, urlparse

import websockets
from websockets.asyncio.server import serve

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Module-level configuration — set by start_relay() or the CLI at startup.
# These are module globals so handle_connection() can read them without
# passing them as arguments through every call.
# ---------------------------------------------------------------------------

# Shared secret for authenticating producers and consumers.
# When non-empty, every /produce and /consume connection must send
# {"type": "auth", "secret": "<value>"} as the first WebSocket message.
# Connections that fail to authenticate within 10 seconds are rejected.
_RELAY_SECRET: str = ""

# Seconds to wait for the auth message after WebSocket handshake.
_AUTH_TIMEOUT_SECONDS: float = 10.0

# Maximum number of messages to buffer per channel when the consumer has not
# yet connected. Prevents a runaway producer from filling all available RAM.
# At ~1 KB per average token message, 1000 messages ≈ 1 MB per channel.
_MAX_BUFFER_MESSAGES: int = 1000

# How long (seconds) to keep a channel alive when only one side is connected.
# After this timeout, the channel is deleted by the reaper. Covers the worst-
# case Globus Compute cold-start delay (~5 minutes for a cold endpoint).
_CHANNEL_TIMEOUT_SECONDS: int = 300

# ---------------------------------------------------------------------------
# Channel registry
# ---------------------------------------------------------------------------
# Maps channel_id (UUID string) → channel dict:
#   {
#     "producer": WebSocket | None,   # connection from HPC compute node
#     "consumer": WebSocket | None,   # connection from user's application
#     "buffer":   list[str],          # messages queued before consumer arrives
#     "created":  float,              # monotonic timestamp for timeout tracking
#   }
#
# Typical lifecycle:
#   1. Consumer connects first (immediately after job submission)
#   2. Producer connects a few seconds later (after Globus/SLURM routing)
#   3. Producer sends token messages → relay forwards to consumer
#   4. Producer sends "done" → both disconnect → channel is deleted
#
channels: dict[str, dict] = {}


# ---------------------------------------------------------------------------
# Background channel reaper
# ---------------------------------------------------------------------------

async def _channel_reaper():
    """
    Background task that periodically deletes abandoned channels.

    An "abandoned" channel is one where only one side ever connected and
    the other side never arrived within _CHANNEL_TIMEOUT_SECONDS. Without
    this reaper, a failed HPC job (or a consumer that crashed before the
    producer connected) would leave a stale entry in the channels dict
    forever, slowly leaking memory.

    Runs every 60 seconds. The overhead is negligible — it just iterates
    the channels dict and checks timestamps.
    """
    while True:
        await asyncio.sleep(60)
        now = _time.monotonic()
        stale = [
            cid
            for cid, ch in list(channels.items())
            if (
                # "one-sided": exactly one of producer/consumer is None
                (ch["producer"] is None) != (ch["consumer"] is None)
                # "both gone": neither side is connected (e.g. both crashed)
                or (ch["producer"] is None and ch["consumer"] is None)
            )
            and (now - ch["created"]) > _CHANNEL_TIMEOUT_SECONDS
        ]
        for cid in stale:
            channels.pop(cid, None)
            logger.warning(
                f"[{cid[:8]}] abandoned channel reaped after "
                f"{_CHANNEL_TIMEOUT_SECONDS}s"
            )


# ---------------------------------------------------------------------------
# WebSocket connection handler
# ---------------------------------------------------------------------------

async def handle_connection(websocket):
    """
    Entry point for every incoming WebSocket connection.

    Called by the websockets library for each new connection. Determines
    whether this connection is a producer, consumer, or health check based
    on the URL path, authenticates it if a shared secret is configured, and
    delegates to the appropriate handler.
    """
    # Parse the full request path (e.g. "/produce/abc123?secret=mytoken")
    full_path = websocket.request.path
    parsed = urlparse(full_path)
    path = parsed.path  # just the path without query string

    # ------------------------------------------------------------------
    # Health check endpoint
    # ------------------------------------------------------------------
    # Any monitoring tool (or streamrelay's own health checks) can hit
    # /health to verify the relay is running. Returns a JSON response and
    # closes immediately. No authentication required — health checks carry
    # no user data.
    if path == "/health":
        await websocket.send(json.dumps({
            "status": "healthy",
            "active_channels": len(channels),
            "timestamp": datetime.now(UTC).isoformat(),
        }))
        await websocket.close()
        return

    # ------------------------------------------------------------------
    # Parse role and channel ID from the path
    # ------------------------------------------------------------------
    # Valid paths: /produce/{channel_id} or /consume/{channel_id}
    parts = path.strip("/").split("/")
    if len(parts) != 2 or parts[0] not in ("produce", "consume"):
        await websocket.close(4000, "Invalid path. Use /produce/{id} or /consume/{id}")
        return

    role = parts[0]       # "produce" or "consume"
    channel_id = parts[1] # UUID string identifying this channel

    # ------------------------------------------------------------------
    # Shared-secret authentication
    # ------------------------------------------------------------------
    # Auth is transmitted as the FIRST JSON message after the WebSocket
    # handshake: {"type": "auth", "secret": "<value>"}
    # URL query parameters (?secret=) are intentionally NOT used because
    # they appear in HTTP access logs even over wss://, leaking the secret.
    #
    # Legacy fallback: if the secret was passed as ?secret= in the URL,
    # accept it with a deprecation warning so old clients keep working.
    if _RELAY_SECRET:
        # Check legacy URL param first (backwards compat)
        qs = parse_qs(parsed.query)
        legacy_secret = qs.get("secret", [None])[0]
        if legacy_secret == _RELAY_SECRET:
            logger.warning(
                f"[{channel_id[:8]}] DEPRECATED: secret passed as URL query param "
                f"(?secret=). Switch to sending {{\"type\":\"auth\",\"secret\":\"...\"}} "
                f"as the first WebSocket message. URL params appear in server logs."
            )
        else:
            # Expect auth as first message within timeout
            try:
                raw_auth = await asyncio.wait_for(
                    websocket.recv(), timeout=_AUTH_TIMEOUT_SECONDS
                )
                auth_msg = json.loads(raw_auth)
                provided = auth_msg.get("secret") if auth_msg.get("type") == "auth" else None
            except (asyncio.TimeoutError, json.JSONDecodeError, Exception):
                provided = None

            if provided != _RELAY_SECRET:
                logger.warning(
                    f"[{channel_id[:8]}] rejected {role}r: invalid or missing secret"
                )
                await websocket.close(4003, "Forbidden: invalid or missing secret")
                return

    logger.info(f"[{channel_id[:8]}] {role}r connected")

    # ------------------------------------------------------------------
    # Initialize channel if this is the first side to connect
    # ------------------------------------------------------------------
    if channel_id not in channels:
        channels[channel_id] = {
            "producer": None,
            "consumer": None,
            "buffer": [],           # messages queued before consumer arrives
            "created": _time.monotonic(),
        }

    channel = channels[channel_id]

    # ------------------------------------------------------------------
    # Register this connection and delegate to the appropriate handler
    # ------------------------------------------------------------------
    # Each channel allows exactly one producer and one consumer. A second
    # producer (or consumer) connecting to the same channel_id is rejected —
    # this would indicate a bug or a replay attack.
    if role == "produce":
        if channel["producer"] is not None:
            await websocket.close(4001, "Producer already connected for this channel")
            return
        channel["producer"] = websocket
        await _handle_producer(websocket, channel, channel_id)
    else:  # consume
        if channel["consumer"] is not None:
            await websocket.close(4001, "Consumer already connected for this channel")
            return
        channel["consumer"] = websocket
        await _handle_consumer(websocket, channel, channel_id)


async def _handle_producer(websocket, channel, channel_id):
    """
    Handle the producer side (HPC compute node).

    Runs for the lifetime of the producer's connection. For each message
    received from the producer:
      - If the consumer is connected: forward the message immediately.
      - If no consumer yet: buffer the message (flushed when consumer arrives).

    When the producer disconnects (normally after sending "done", or due to
    a network error), cleans up and potentially removes the channel.
    """
    try:
        async for message in websocket:
            # Each iteration receives one complete WebSocket message —
            # one token, one "done", or one "error" from the compute node.
            consumer = channel.get("consumer")

            if consumer is not None:
                # Fast path: consumer is connected, forward immediately.
                # This is the normal case during active streaming.
                try:
                    await consumer.send(message)
                except websockets.ConnectionClosed:
                    # Consumer disconnected mid-stream (user closed browser tab,
                    # network dropped, etc.). The HPC job keeps running — we
                    # can't stop it — but there's no one to forward to anymore.
                    logger.warning(
                        f"[{channel_id[:8]}] consumer disconnected mid-stream, "
                        f"dropping remaining tokens"
                    )
                    break
            else:
                # Slow path: consumer hasn't connected yet. Buffer the message
                # so it isn't lost. The consumer will receive it when it connects
                # (see _handle_consumer below).
                if len(channel["buffer"]) >= _MAX_BUFFER_MESSAGES:
                    # Buffer full — drop the oldest message to make room.
                    # This is a sliding-window policy that bounds memory usage.
                    channel["buffer"].pop(0)
                    logger.warning(
                        f"[{channel_id[:8]}] buffer full ({_MAX_BUFFER_MESSAGES} "
                        f"messages) — dropping oldest"
                    )
                channel["buffer"].append(message)

    except websockets.ConnectionClosed:
        # Normal: producer finished and closed the connection (or network error).
        logger.info(f"[{channel_id[:8]}] producer disconnected")
    finally:
        # Always unregister the producer. If both sides are now gone and there
        # are no buffered messages, delete the channel to free memory.
        channel["producer"] = None
        _maybe_cleanup_channel(channel_id)


async def _handle_consumer(websocket, channel, channel_id):
    """
    Handle the consumer side (the user's application).

    When the consumer connects:
      1. Flush any messages that were buffered before the consumer arrived.
      2. Keep the connection alive — _handle_producer() pushes new messages
         directly to this websocket as they arrive.

    The consumer is receive-only: it does not send messages to the producer.
    It stays connected until the producer sends "done" and closes, which
    causes the producer's handler to close the consumer's connection too.
    """
    try:
        # Flush buffered messages — these are tokens that the producer sent
        # before the consumer connected. Deliver them in order so the consumer
        # doesn't miss the beginning of the response.
        if channel["buffer"]:
            logger.debug(
                f"[{channel_id[:8]}] flushing {len(channel['buffer'])} "
                f"buffered messages to consumer"
            )
            for msg in channel["buffer"]:
                await websocket.send(msg)
            channel["buffer"].clear()

        # Keep the connection open. New messages from the producer are forwarded
        # here directly by _handle_producer() via consumer.send(). This loop
        # just keeps the coroutine alive until the connection closes.
        # We ignore any messages sent BY the consumer (reserved for future use,
        # e.g. a "cancel" signal).
        async for _ in websocket:
            pass

    except websockets.ConnectionClosed:
        logger.info(f"[{channel_id[:8]}] consumer disconnected")
    finally:
        channel["consumer"] = None
        _maybe_cleanup_channel(channel_id)


def _maybe_cleanup_channel(channel_id):
    """
    Delete the channel from the registry if both sides have disconnected
    and there are no buffered messages waiting for a future consumer.

    Called whenever either side disconnects. If buffered messages remain,
    the channel is kept alive so a late-arriving consumer can still receive
    them. Without this guard, those tokens would be silently lost.
    """
    channel = channels.get(channel_id)
    if channel and channel["producer"] is None and channel["consumer"] is None:
        if channel["buffer"]:
            # Edge case: producer sent tokens and disconnected, but the consumer
            # hasn't arrived yet. Keep the channel alive so the consumer can
            # still receive the buffered tokens when it connects.
            logger.info(
                f"[{channel_id[:8]}] both sides gone but "
                f"{len(channel['buffer'])} buffered messages remain — "
                f"keeping channel alive for consumer"
            )
            return
        del channels[channel_id]
        logger.info(f"[{channel_id[:8]}] channel cleaned up")


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

async def start_relay(
    host: str = "0.0.0.0",
    port: int = 8765,
    secret: str = "",
    max_buffer: int = 1000,
    channel_timeout: int = 300,
):
    """
    Start the WebSocket relay server.

    This is an async function that runs forever (until cancelled). Call it
    with asyncio.run() from a script, or await it inside an existing event
    loop.

    Args:
        host: Bind address. "0.0.0.0" accepts connections on all network
            interfaces. Use "127.0.0.1" to restrict to localhost only.
        port: TCP port to listen on. Default 8765.
        secret: Shared secret for authentication. When non-empty, all
            /produce and /consume connections must send
            {"type": "auth", "secret": "<value>"} as their first message.
            Leave empty only for local development.
        max_buffer: Maximum number of messages to buffer per channel when
            the consumer has not yet connected. Oldest messages are dropped
            when the limit is reached. Default 1000.
        channel_timeout: Seconds before an abandoned one-sided channel is
            deleted by the reaper. Default 300 (5 minutes).
    """
    global _RELAY_SECRET, _MAX_BUFFER_MESSAGES, _CHANNEL_TIMEOUT_SECONDS
    _RELAY_SECRET = secret
    _MAX_BUFFER_MESSAGES = max_buffer
    _CHANNEL_TIMEOUT_SECONDS = channel_timeout

    if not secret:
        logger.warning(
            "Auth disabled — set secret= for any deployment beyond localhost. "
            "Without a secret, anyone who knows a channel_id can connect."
        )

    # serve() starts the WebSocket server and calls handle_connection() for
    # every new incoming connection.
    async with serve(handle_connection, host, port):
        # Start the background reaper as a concurrent task.
        asyncio.create_task(_channel_reaper())
        logger.info(f"streamrelay listening on ws://{host}:{port}")
        logger.info(f"  Produce: ws://{host}:{port}/produce/{{channel_id}}")
        logger.info(f"  Consume: ws://{host}:{port}/consume/{{channel_id}}")
        logger.info(f"  Health:  ws://{host}:{port}/health")
        # run_forever() equivalent: create a future that never resolves.
        # This keeps the server running until the process is killed.
        await asyncio.get_running_loop().create_future()


# ---------------------------------------------------------------------------
# CLI entry point
# ---------------------------------------------------------------------------

def main():
    """
    Command-line interface for the relay server.

    Installed as the ``streamrelay`` command by pyproject.toml:
        streamrelay --host 0.0.0.0 --port 8765 --secret mytoken
    """
    parser = argparse.ArgumentParser(
        description=(
            "streamrelay — WebSocket relay for real-time token streaming "
            "from HPC compute nodes."
        )
    )
    parser.add_argument(
        "--host", default="0.0.0.0",
        help="Bind address (default: 0.0.0.0 = all interfaces)",
    )
    parser.add_argument(
        "--port", type=int, default=8765,
        help="Port to listen on (default: 8765)",
    )
    parser.add_argument(
        "--secret", default="",
        help=(
            "Shared secret for authentication. All produce/consume connections "
            "must send {\"type\":\"auth\",\"secret\":\"...\"} as their first message. "
            "Also reads RELAY_SECRET env var. Omit only for local development."
        ),
    )
    parser.add_argument(
        "--max-buffer", type=int, default=1000,
        help="Max messages buffered per channel before oldest is dropped (default: 1000)",
    )
    parser.add_argument(
        "--channel-timeout", type=int, default=300,
        help="Seconds before abandoned channels are reaped (default: 300)",
    )
    parser.add_argument(
        "--log-level", default="INFO",
        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
        help="Logging verbosity (default: INFO)",
    )
    args = parser.parse_args()

    # RELAY_SECRET env var as an alternative to --secret flag, so the secret
    # doesn't appear in process listings or shell history.
    secret = args.secret or os.getenv("RELAY_SECRET", "")

    logging.basicConfig(
        level=getattr(logging, args.log_level),
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
        datefmt="%H:%M:%S",
    )

    try:
        asyncio.run(start_relay(
            host=args.host,
            port=args.port,
            secret=secret,
            max_buffer=args.max_buffer,
            channel_timeout=args.channel_timeout,
        ))
    except KeyboardInterrupt:
        logger.info("Relay stopped.")


if __name__ == "__main__":
    main()
