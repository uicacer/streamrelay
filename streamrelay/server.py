"""
streamrelay.server — Lightweight WebSocket relay for token streaming.

The relay server is a stateless forwarder: it routes messages between a
**producer** (running on an HPC compute node) and a **consumer** (running on
the user's machine or middleware), without reading or interpreting them.

Both sides connect **outbound** to the relay — no inbound ports, no VPN, no
SSH tunnels required. This makes it work behind strict campus firewalls.

Protocol (all messages are JSON strings):

    Producer → Consumer:
        {"type": "token",  "content": "Hello"}     — one text chunk
        {"type": "done",   "usage": {...}}          — generation complete
        {"type": "error",  "message": "..."}        — something went wrong

URL scheme:

    /produce/{channel_id}[?secret=<token>]   — register as producer
    /consume/{channel_id}[?secret=<token>]   — register as consumer
    /health                                  — health check (no auth required)

Usage::

    # Start from Python
    import asyncio
    from streamrelay import start_relay
    asyncio.run(start_relay(port=8765, secret="my-secret"))

    # Or from the command line
    python -m streamrelay.server --port 8765 --secret my-secret
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

# Module-level config set by start_relay() / CLI
_RELAY_SECRET: str = ""
_MAX_BUFFER_MESSAGES: int = 1000
_CHANNEL_TIMEOUT_SECONDS: int = 300

# Channel registry: channel_id → {producer, consumer, buffer, created}
channels: dict[str, dict] = {}


# =============================================================================
# BACKGROUND CHANNEL REAPER
# =============================================================================


async def _channel_reaper():
    """Periodically remove abandoned channels (one side never connected)."""
    while True:
        await asyncio.sleep(60)
        now = _time.monotonic()
        stale = [
            cid
            for cid, ch in list(channels.items())
            if (
                (ch["producer"] is None) != (ch["consumer"] is None)
                or (ch["producer"] is None and ch["consumer"] is None)
            )
            and (now - ch["created"]) > _CHANNEL_TIMEOUT_SECONDS
        ]
        for cid in stale:
            channels.pop(cid, None)
            logger.warning(f"[{cid[:8]}] abandoned channel reaped after {_CHANNEL_TIMEOUT_SECONDS}s")


# =============================================================================
# WEBSOCKET HANDLER
# =============================================================================


async def handle_connection(websocket):
    """Route an incoming WebSocket to a producer or consumer handler."""
    full_path = websocket.request.path
    parsed = urlparse(full_path)
    path = parsed.path

    if path == "/health":
        await websocket.send(
            json.dumps(
                {
                    "status": "healthy",
                    "active_channels": len(channels),
                    "timestamp": datetime.now(UTC).isoformat(),
                }
            )
        )
        await websocket.close()
        return

    parts = path.strip("/").split("/")
    if len(parts) != 2 or parts[0] not in ("produce", "consume"):
        await websocket.close(4000, "Invalid path. Use /produce/{id} or /consume/{id}")
        return

    role, channel_id = parts[0], parts[1]

    if _RELAY_SECRET:
        qs = parse_qs(parsed.query)
        if qs.get("secret", [None])[0] != _RELAY_SECRET:
            logger.warning(f"[{channel_id[:8]}] rejected {role}r: invalid or missing secret")
            await websocket.close(4003, "Forbidden: invalid or missing secret")
            return

    logger.info(f"[{channel_id[:8]}] {role}r connected")

    if channel_id not in channels:
        channels[channel_id] = {
            "producer": None,
            "consumer": None,
            "buffer": [],
            "created": _time.monotonic(),
        }

    channel = channels[channel_id]

    if role == "produce":
        if channel["producer"] is not None:
            await websocket.close(4001, "Producer already connected for this channel")
            return
        channel["producer"] = websocket
        await _handle_producer(websocket, channel, channel_id)
    else:
        if channel["consumer"] is not None:
            await websocket.close(4001, "Consumer already connected for this channel")
            return
        channel["consumer"] = websocket
        await _handle_consumer(websocket, channel, channel_id)


async def _handle_producer(websocket, channel, channel_id):
    try:
        async for message in websocket:
            consumer = channel.get("consumer")
            if consumer is not None:
                try:
                    await consumer.send(message)
                except websockets.ConnectionClosed:
                    logger.warning(f"[{channel_id[:8]}] consumer disconnected, dropping tokens")
                    break
            else:
                if len(channel["buffer"]) >= _MAX_BUFFER_MESSAGES:
                    channel["buffer"].pop(0)
                    logger.warning(f"[{channel_id[:8]}] buffer full — dropping oldest message")
                channel["buffer"].append(message)
    except websockets.ConnectionClosed:
        logger.info(f"[{channel_id[:8]}] producer disconnected")
    finally:
        channel["producer"] = None
        _maybe_cleanup_channel(channel_id)


async def _handle_consumer(websocket, channel, channel_id):
    try:
        if channel["buffer"]:
            logger.debug(f"[{channel_id[:8]}] flushing {len(channel['buffer'])} buffered messages")
            for msg in channel["buffer"]:
                await websocket.send(msg)
            channel["buffer"].clear()

        async for _ in websocket:
            pass  # consumers are receive-only; ignore any messages they send

    except websockets.ConnectionClosed:
        logger.info(f"[{channel_id[:8]}] consumer disconnected")
    finally:
        channel["consumer"] = None
        _maybe_cleanup_channel(channel_id)


def _maybe_cleanup_channel(channel_id):
    channel = channels.get(channel_id)
    if channel and channel["producer"] is None and channel["consumer"] is None:
        if channel["buffer"]:
            return  # buffered messages still waiting for a consumer
        del channels[channel_id]
        logger.info(f"[{channel_id[:8]}] channel cleaned up")


# =============================================================================
# PUBLIC API
# =============================================================================


async def start_relay(
    host: str = "0.0.0.0",
    port: int = 8765,
    secret: str = "",
    max_buffer: int = 1000,
    channel_timeout: int = 300,
):
    """Start the WebSocket relay server (async, blocks until stopped).

    Args:
        host: Bind address. ``"0.0.0.0"`` accepts all interfaces.
        port: Port to listen on.
        secret: Shared secret for authentication. Empty = auth disabled (dev only).
        max_buffer: Max messages buffered per channel before oldest is dropped.
        channel_timeout: Seconds before an abandoned channel is cleaned up.
    """
    global _RELAY_SECRET, _MAX_BUFFER_MESSAGES, _CHANNEL_TIMEOUT_SECONDS
    _RELAY_SECRET = secret
    _MAX_BUFFER_MESSAGES = max_buffer
    _CHANNEL_TIMEOUT_SECONDS = channel_timeout

    if not secret:
        logger.warning("Auth disabled — set secret= for production deployments")

    async with serve(handle_connection, host, port):
        asyncio.create_task(_channel_reaper())
        logger.info(f"streamrelay listening on ws://{host}:{port}")
        await asyncio.get_running_loop().create_future()  # run forever


# =============================================================================
# CLI ENTRY POINT
# =============================================================================


def main():
    parser = argparse.ArgumentParser(
        description="streamrelay — WebSocket relay for streaming tokens from HPC compute nodes."
    )
    parser.add_argument("--host", default="0.0.0.0", help="Bind address (default: 0.0.0.0)")
    parser.add_argument("--port", type=int, default=8765, help="Port (default: 8765)")
    parser.add_argument(
        "--secret",
        default="",
        help="Shared secret for authentication. Also reads RELAY_SECRET env var.",
    )
    parser.add_argument(
        "--max-buffer",
        type=int,
        default=1000,
        help="Max buffered messages per channel (default: 1000)",
    )
    parser.add_argument(
        "--channel-timeout",
        type=int,
        default=300,
        help="Seconds before abandoned channels are reaped (default: 300)",
    )
    parser.add_argument(
        "--log-level",
        default="INFO",
        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
    )
    args = parser.parse_args()

    secret = args.secret or os.getenv("RELAY_SECRET", "")

    logging.basicConfig(
        level=getattr(logging, args.log_level),
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
        datefmt="%H:%M:%S",
    )

    try:
        asyncio.run(
            start_relay(
                host=args.host,
                port=args.port,
                secret=secret,
                max_buffer=args.max_buffer,
                channel_timeout=args.channel_timeout,
            )
        )
    except KeyboardInterrupt:
        logger.info("Relay stopped.")


if __name__ == "__main__":
    main()
