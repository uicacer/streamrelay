# Contributing & Testing Guide

Three levels of testing are available, each building on the previous:

1. **Unit + integration tests** — no network, no credentials, runs in < 2s
2. **Local end-to-end** — full producer → relay → consumer on your laptop via Cloudflare tunnel
3. **Live relay** — test against a real public relay server with authentication and encryption

---

## Level 1 — Unit and integration tests (no network needed)

```bash
git clone https://github.com/uicacer/streamrelay
cd streamrelay
pip install -e ".[dev]"
pytest tests/ -v
```

Expected output:
```
tests/test_crypto.py::test_round_trip                  PASSED
tests/test_crypto.py::test_encrypted_format            PASSED
tests/test_crypto.py::test_fresh_nonce_each_call       PASSED
tests/test_crypto.py::test_passthrough_non_enc         PASSED
tests/test_crypto.py::test_wrong_key_raises            PASSED
tests/test_crypto.py::test_generate_key_length         PASSED
tests/test_integration.py::test_basic                  PASSED
tests/test_integration.py::test_buffering_producer_first PASSED
tests/test_integration.py::test_encrypted              PASSED
tests/test_integration.py::test_empty_stream           PASSED
10 passed in ~1s
```

The integration tests spin up a real relay on `localhost:18765` and run
producer/consumer pairs through it, including buffering and AES-256-GCM encryption.
No HPC, no public server, no credentials required.

---

## Level 2 — Local end-to-end with Cloudflare Tunnel

Tests the complete flow across a real network, simulating HPC and client on
separate machines — entirely on your laptop.

**Prerequisites:**
```bash
pip install streamrelay
brew install cloudflared        # macOS
# Linux: https://pkg.cloudflare.com/index.html
```

**Terminal 1 — Start the relay:**
```bash
streamrelay --port 8765 --secret demo-secret
# Output: streamrelay listening on ws://0.0.0.0:8765
```

**Terminal 2 — Expose it publicly:**
```bash
cloudflared tunnel --url http://localhost:8765
# Output: ... Your quick Tunnel has been created! Visit it at (it's https, not http):
#         https://random-name.trycloudflare.com
```
Copy that URL and replace `https://` with `wss://` → your `RELAY_URL`.

**Terminal 3 — Consumer (simulates your application):**
```python
# consumer_test.py
from streamrelay import RelayConsumer

RELAY_URL = "wss://random-name.trycloudflare.com"   # from Terminal 2
CHANNEL   = "test-channel-001"
SECRET    = "demo-secret"

print("Waiting for tokens...")
for token in RelayConsumer(RELAY_URL, CHANNEL, relay_secret=SECRET).stream():
    print(token, end="", flush=True)
print()  # newline after last token
```
```bash
python consumer_test.py
```

**Terminal 4 — Producer (simulates the HPC compute node):**
```python
# producer_test.py
from streamrelay import RelayProducer

RELAY_URL = "wss://random-name.trycloudflare.com"   # same URL
CHANNEL   = "test-channel-001"
SECRET    = "demo-secret"

with RelayProducer(RELAY_URL, CHANNEL, relay_secret=SECRET) as relay:
    for word in ["Hello", " ", "from", " ", "the", " ", "compute", " ", "node", "!"]:
        relay.send_token(word)
```
```bash
python producer_test.py
```

You should see `Hello from the compute node!` appear token-by-token in Terminal 3
as Terminal 4 sends each word.

**What this proves:** both sides connect outbound to a public relay across the real
internet, with shared-secret authentication, without any inbound ports open.

---

## Level 3 — Live relay test (authentication + encryption)

Run the following script against a real relay server to verify all three security
layers work end-to-end. Replace `RELAY_URL` and `SECRET` with your relay's values.

```python
# live_relay_test.py
import asyncio
import uuid
from streamrelay import RelayProducer, RelayConsumer, generate_key


RELAY_URL = "wss://your-relay.example.com"
SECRET    = "your-relay-secret"


async def test_plain():
    """Plain streaming — no encryption."""
    channel_id = str(uuid.uuid4())
    received = []

    async def consume():
        async for token in RelayConsumer(RELAY_URL, channel_id, relay_secret=SECRET):
            received.append(token)

    async def produce():
        await asyncio.sleep(0.3)
        async with RelayProducer(RELAY_URL, channel_id, relay_secret=SECRET) as p:
            for word in ["Hello", " ", "from", " ", "live", " ", "relay", "!"]:
                await p._async_send_raw({"type": "token", "content": word})

    await asyncio.gather(consume(), produce())
    result = "".join(received)
    assert result == "Hello from live relay!", f"Got: {repr(result)}"
    print(f"[PASS] plain streaming: {repr(result)}")


async def test_encrypted():
    """AES-256-GCM end-to-end encryption — relay sees only ciphertext."""
    channel_id = str(uuid.uuid4())
    key = generate_key()
    received = []

    async def consume():
        async for token in RelayConsumer(RELAY_URL, channel_id,
                                         relay_secret=SECRET, encryption_key=key):
            received.append(token)

    async def produce():
        await asyncio.sleep(0.3)
        async with RelayProducer(RELAY_URL, channel_id,
                                  relay_secret=SECRET, encryption_key=key) as p:
            for word in ["encrypted", " ", "live", " ", "test"]:
                await p._async_send_raw({"type": "token", "content": word})

    await asyncio.gather(consume(), produce())
    result = "".join(received)
    assert result == "encrypted live test", f"Got: {repr(result)}"
    print(f"[PASS] encrypted streaming: {repr(result)}")


async def test_wrong_secret_rejected():
    """Wrong secret must be rejected by the relay with code 4003."""
    channel_id = str(uuid.uuid4())
    try:
        async for _ in RelayConsumer(RELAY_URL, channel_id, relay_secret="wrong"):
            pass
        print("[FAIL] wrong secret was not rejected")
    except Exception as e:
        assert "4003" in str(e) or "Forbidden" in str(e), f"Unexpected error: {e}"
        print(f"[PASS] wrong secret rejected: {type(e).__name__}")


async def main():
    print(f"Testing relay: {RELAY_URL}\n")
    await test_plain()
    await test_encrypted()
    await test_wrong_secret_rejected()
    print("\nAll live relay tests passed.")


asyncio.run(main())
```

```bash
python live_relay_test.py
```

Expected output:
```
Testing relay: wss://your-relay.example.com

[PASS] plain streaming: 'Hello from live relay!'
[PASS] encrypted streaming: 'encrypted live test'
[PASS] wrong secret rejected: ConnectionClosedError

All live relay tests passed.
```

---

## Development setup

```bash
git clone https://github.com/uicacer/streamrelay
cd streamrelay
pip install -e ".[dev,globus]"
pytest tests/ -v
ruff check streamrelay/
```

---

## Submitting changes

1. Fork the repo and create a feature branch.
2. `pytest tests/ -v` — all 10 tests must pass.
3. `ruff check streamrelay/` — no lint errors.
4. Open a pull request with a description of what and why.
