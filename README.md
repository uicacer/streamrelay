# streamrelay

**Real-time incremental output from batch HPC executors via WebSocket relay.**

[![PyPI](https://img.shields.io/pypi/v/streamrelay)](https://pypi.org/project/streamrelay/)
[![License](https://img.shields.io/badge/license-Apache%202.0-blue)](https://github.com/uicacer/streamrelay/blob/main/LICENSE)
[![Tests](https://github.com/uicacer/streamrelay/actions/workflows/tests.yml/badge.svg)](https://github.com/uicacer/streamrelay/actions)
[![JOSS](https://joss.theoj.org/papers/TODO/status.svg)](https://joss.theoj.org/papers/TODO)

**New here? Start with the [full tutorial](https://github.com/uicacer/streamrelay/blob/main/docs/tutorial.md)** — deploy the relay,
write a producer on your HPC node, stream output to your app, add encryption.
All in one place.

---

## The problem

HPC batch systems execute jobs to completion and return a single result. Any job
that produces output incrementally — LLM inference generating tokens, an iterative
solver emitting convergence metrics, a simulation producing trajectory frames, a
pipeline processing streaming data — is forced to wait until completion before
sending anything back. The user (or downstream application) sees nothing until the
job finishes.

**streamrelay solves this with a dual-channel architecture:**

- **Control plane** (unchanged): your existing execution framework — a SLURM or PBS
  job script, an SSH command, a Globus Compute function call — handles job
  submission and authentication exactly as before.
- **Data plane** (streamrelay): a lightweight WebSocket relay through which the
  compute node streams incremental output back in real time as it is produced.

Both the compute node (producer) and your application (consumer) connect
**outbound** to the relay. Neither side accepts an inbound connection — no firewall
exceptions, no VPN, no tunnels required.

```
Your application              Relay server              HPC compute node
────────────────              ────────────              ────────────────
1. Submit job via         2. Both connect               3. Job starts
   SLURM / PBS /             outbound here              4. Output streams
   Globus Compute /          (this is streamrelay)         to relay →
   SSH / anything    ◄─────────────────────────────────────────────────
5. Output arrives
   incrementally
```

Measured in the [STREAM system](https://github.com/uicacer/stream) at UIC (LLM inference use case):
**0.85 s** median time-to-first-output from HPC with streaming, vs. **15.68 s**
in batch mode.

---

## Installation

```bash
pip install streamrelay
```

Optional — Globus Compute integration (adds `StreamingExecutor`):

```bash
pip install streamrelay[globus]
```

Requires Python ≥ 3.11.

---

## Quick start

### 1. Start the relay server

Run this on any machine with a public IP — a small cloud VM, a campus server, or
your laptop with a Cloudflare tunnel for development:

```bash
streamrelay --host 0.0.0.0 --port 8765 --secret my-shared-secret
```

Development shortcut (no public server needed):

```bash
streamrelay --port 8765 &
cloudflared tunnel --url http://localhost:8765   # gives you a public wss:// URL
```

### 2. On the HPC compute node — producer

Inside your job script or remote function, send tokens as your model generates them:

```python
from streamrelay import RelayProducer

# relay_url and channel_id are passed in as job arguments or env vars
with RelayProducer(relay_url, channel_id, relay_secret="my-shared-secret") as relay:
    for token in your_model.stream(prompt):
        relay.send_token(token)
# done signal is sent automatically on exit
```

This works in any execution context: a SLURM batch script, a PBS job, a plain SSH
command, a Globus Compute function, or any subprocess.

### 3. On your application — consumer

```python
import uuid
from streamrelay import RelayConsumer

channel_id = str(uuid.uuid4())   # generate before submitting the job

# submit the job here — SLURM, PBS, Globus Compute, SSH — your choice
# pass relay_url and channel_id to the job as arguments or env vars

for token in RelayConsumer(relay_url, channel_id, relay_secret="my-shared-secret").stream():
    print(token, end="", flush=True)
```

Async version for FastAPI or other asyncio applications:

```python
async for token in RelayConsumer(relay_url, channel_id):
    yield f"data: {token}\n\n"   # forward as Server-Sent Events to a browser
```

---

## Example: SLURM job

**Submitter (your laptop or login node):**

```python
# submit.py
import subprocess, uuid
from streamrelay import RelayConsumer

relay_url = "wss://your-relay.example.com"
channel_id = str(uuid.uuid4())

subprocess.run([
    "sbatch",
    f"--export=ALL,RELAY_URL={relay_url},CHANNEL_ID={channel_id}",
    "inference_job.sh",
])

for token in RelayConsumer(relay_url, channel_id).stream():
    print(token, end="", flush=True)
```

**Job script (SLURM compute node):**

```bash
#!/bin/bash
#SBATCH --partition=gpu --gres=gpu:1

python - <<'EOF'
import os
from streamrelay import RelayProducer

with RelayProducer(os.environ["RELAY_URL"], os.environ["CHANNEL_ID"]) as relay:
    for token in your_model.stream(prompt):
        relay.send_token(token)
EOF
```

---

## Globus Compute integration

[Globus Compute](https://www.globus.org/compute) is a federated function execution
service that dispatches Python functions to remote HPC endpoints (which themselves
run on SLURM or PBS clusters). Because Globus Compute returns a single result when
the function completes, it has no native mechanism for streaming incremental output.
streamrelay adds that capability.

```bash
pip install streamrelay[globus]
```

`StreamingExecutor` wraps any Globus Compute function and streams its output:

```python
from streamrelay import StreamingExecutor

async with StreamingExecutor(
    endpoint_id="your-globus-endpoint-uuid",
    relay_url="wss://your-relay.example.com",
    relay_secret="my-shared-secret",
) as executor:
    async for token in executor.stream(my_inference_fn, prompt="Explain quantum entanglement"):
        print(token, end="", flush=True)
```

Your remote function receives `relay_url` and `channel_id` as keyword arguments
automatically:

```python
def my_inference_fn(prompt, relay_url, channel_id, relay_secret=""):
    # all imports must be inline — Globus Compute serializes this function
    from streamrelay import RelayProducer
    with RelayProducer(relay_url, channel_id, relay_secret=relay_secret) as relay:
        for token in call_vllm_streaming(prompt):
            relay.send_token(token)
```

---

## End-to-end encryption

By default, the relay server can see the token payloads it forwards. For sensitive
workloads (medical, financial, or personal data), enable AES-256-GCM end-to-end
encryption. The relay then forwards opaque ciphertext and cannot read the content.

Generate a key once and store it in your `.env`:

```bash
python -c "from streamrelay import generate_key; print(generate_key())"
```

Pass the same key to both producer and consumer:

```python
# Producer (HPC node)
with RelayProducer(relay_url, channel_id, encryption_key=KEY) as relay:
    relay.send_token(token)

# Consumer (your application)
for token in RelayConsumer(relay_url, channel_id, encryption_key=KEY).stream():
    print(token, end="", flush=True)
```

Each message uses a fresh random 12-byte nonce. The GCM authentication tag detects
any tampering in transit.

---

## Security model

`streamrelay` enforces three independent security layers:

### Layer 1 — Transport encryption (TLS)

Deploy the relay behind a TLS-terminating reverse proxy (Caddy, nginx) so all
connections use `wss://` (WebSocket over TLS). This encrypts traffic between each
client and the relay server. See [docs/deployment.md](https://github.com/uicacer/streamrelay/blob/main/docs/deployment.md) for a
Caddy setup with auto-provisioned Let's Encrypt certificates.

### Layer 2 — Access control (shared secret)

Start the relay with `--secret MY_SECRET`. Every producer and consumer must supply
the same value as the **first JSON message** sent after the WebSocket handshake
completes. Connections that do not supply a valid auth message within 10 seconds are
rejected with close code 4003.

```bash
# Server
streamrelay --port 8765 --secret MY_SECRET

# Producer (HPC node) — same secret
with RelayProducer(relay_url, channel_id, relay_secret="MY_SECRET") as relay: ...

# Consumer (your app) — same secret
RelayConsumer(relay_url, channel_id, relay_secret="MY_SECRET").stream()
```

The secret is transmitted as application-layer data after the handshake, not as a
URL query parameter. This is important: query parameters such as `?secret=...` appear
in HTTP access logs (even over `wss://`) because the HTTP Upgrade request path is
logged before TLS is applied. Post-handshake transmission keeps the secret out of
all log files.

**How to share the secret with the HPC node:** pass it as a job argument, an
environment variable in your SLURM/PBS script, or as a keyword argument to your
Globus Compute function:

```bash
# SLURM — pass via --export
sbatch --export=ALL,RELAY_URL=wss://...,RELAY_SECRET=MY_SECRET job.sh

# Globus Compute — inject as a kwarg
executor.submit(my_fn, relay_url=relay_url, relay_secret=MY_SECRET, ...)
```

In addition to the shared secret, each request uses a **unique UUID channel ID**
(122 bits of entropy). Even if an attacker knows the relay address, guessing a valid
channel ID is computationally infeasible. The relay holds no persistent state —
all channel state is discarded once both sides disconnect. No OAuth2 credentials or
user identity information traverse the relay at any point.

### Layer 3 — End-to-end payload encryption (AES-256-GCM)

TLS protects the link to the relay, but the relay operator can still see plaintext
token payloads. For sensitive workloads (medical, financial, or personal data),
enable AES-256-GCM end-to-end encryption. The relay then forwards opaque ciphertext
and cannot read the content.

**Generate a key once** and store it securely (e.g., in your `.env`):

```bash
python -c "from streamrelay import generate_key; print(generate_key())"
# Outputs a base64-encoded 32-byte key, e.g.: xK3mP9vQ2rL...
```

**Pass the same key to both producer and consumer:**

```python
KEY = os.getenv("RELAY_ENCRYPTION_KEY")

# Producer (HPC node)
with RelayProducer(relay_url, channel_id, encryption_key=KEY) as relay:
    relay.send_token(token)

# Consumer (your app)
for token in RelayConsumer(relay_url, channel_id, encryption_key=KEY).stream():
    print(token, end="", flush=True)
```

Each message is encrypted with a **fresh random 12-byte nonce** (per NIST SP
800-38D). The GCM authentication tag detects any tampering in transit — if the relay
or any intermediary modifies a message, decryption raises an `InvalidTag` exception
rather than silently returning corrupted data. Encryption is opt-in and
backward-compatible: an unencrypted consumer connecting to an encrypted producer
will receive ciphertext it cannot parse, but no silent data corruption occurs.

### Summary

| Layer | Mechanism | Protects against | How to enable |
|-------|-----------|-----------------|---------------|
| TLS (`wss://`) | Reverse proxy (Caddy) | Network eavesdropping | Deploy behind Caddy/nginx |
| Shared secret | WebSocket handshake | Unauthorized connections | `--secret` flag on server |
| AES-256-GCM | Per-message encryption | Relay operator reading payloads | `encryption_key=` on producer + consumer |
| UUID channel isolation | 122-bit random ID | Channel collision / guessing | Always on |

See [docs/deployment.md](https://github.com/uicacer/streamrelay/blob/main/docs/deployment.md) for a production deployment guide
(cloud VM + Caddy + systemd).

---

## Relay protocol

All messages are JSON strings. The relay forwards them without interpretation:

```
{"type": "token",  "content": "Hello"}        ← one text chunk
{"type": "done",   "usage": {...}}             ← generation complete
{"type": "error",  "message": "..."}           ← something went wrong
```

When encryption is enabled, each message is wrapped before transmission:

```
{"type": "enc", "d": "<base64(nonce + ciphertext + GCM tag)>"}
```

---

## API reference

### `RelayProducer`

Runs on the HPC compute node. Connects outbound to the relay and sends tokens.

```python
from streamrelay import RelayProducer

# Synchronous — use inside SLURM jobs, PBS scripts, Globus Compute functions
with RelayProducer(
    relay_url,          # str: "wss://relay.example.com" or "ws://localhost:8765"
    channel_id,         # str: uuid.uuid4() generated before submitting the job
    relay_secret="",    # str: must match --secret on the relay server
    encryption_key="",  # str: base64 AES-256 key from generate_key(); "" = no encryption
) as relay:
    relay.send_token("Hello")            # send one text chunk
    relay.send_token(" world")
    # send_done() called automatically when the with block exits normally
    # send_error() called automatically if an exception is raised inside the block

# Asynchronous — use when your code already runs in an asyncio event loop
async with RelayProducer(relay_url, channel_id) as relay:
    await relay._async_send_raw({"type": "token", "content": "Hello"})
```

**Explicit methods (when not using the context manager):**

```python
p = RelayProducer(relay_url, channel_id)
p.connect()                              # open the synchronous WebSocket
p.send_token("chunk")                   # send a token
p.send_done(usage={"total_tokens": 50}) # signal completion with optional usage stats
p.send_error("something broke")         # report an error (also sends done)
p.close()                               # close the connection
```

---

### `RelayConsumer`

Runs on your application side. Connects outbound to the relay and yields tokens.

```python
from streamrelay import RelayConsumer

consumer = RelayConsumer(
    relay_url,          # str: same relay URL as the producer
    channel_id,         # str: same channel_id passed to RelayProducer
    relay_secret="",    # str: same secret as the producer
    encryption_key="",  # str: same encryption key as the producer
)

# --- Synchronous iteration (CLI scripts, Jupyter notebooks) ---
for token in consumer.stream():
    print(token, end="", flush=True)

# --- Asynchronous iteration (FastAPI, aiohttp, any asyncio application) ---
async for token in consumer:            # uses __aiter__ → astream()
    yield f"data: {token}\n\n"         # forward as Server-Sent Events

# --- Collect the full response as a single string ---
text = consumer.collect()               # blocking
text = await consumer.acollect()        # async
```

**Connect the consumer before (or at the same time as) submitting the HPC job.**
Any tokens that arrive before you connect are buffered by the relay (default 1,000
messages) and flushed when you connect — you will not miss the beginning of the response.

---

### `start_relay` / `streamrelay` CLI

Start the relay server — run this once on any machine with a public IP.

```bash
# CLI
streamrelay --host 0.0.0.0 --port 8765 --secret MY_SECRET

# All options:
streamrelay --help
#   --host HOST            bind address (default: 0.0.0.0)
#   --port PORT            port to listen on (default: 8765)
#   --secret SECRET        shared auth secret; also reads RELAY_SECRET env var
#   --max-buffer N         max buffered messages per channel (default: 1000)
#   --channel-timeout N    seconds before abandoned channels are reaped (default: 300)
#   --log-level LEVEL      DEBUG / INFO / WARNING / ERROR (default: INFO)
```

```python
# Python API — embed the relay inside an existing asyncio application
import asyncio
from streamrelay import start_relay

asyncio.run(start_relay(
    host="0.0.0.0",
    port=8765,
    secret="MY_SECRET",
    max_buffer=1000,
    channel_timeout=300,
))
```

**Health check** — the relay exposes `/health` (no auth required):

```python
import asyncio, websockets, json

async def check(relay_url):
    async with websockets.connect(f"{relay_url}/health") as ws:
        status = json.loads(await ws.recv())
        print(status)  # {"status": "healthy", "active_channels": 0, "timestamp": "..."}

asyncio.run(check("wss://relay.example.com"))
```

---

### `generate_key`

```python
from streamrelay import generate_key

key = generate_key()          # base64-encoded 32-byte AES-256 key
print(key)                    # e.g. "xK3mP9vQ2rL8nJ6w..."
# Store in .env as RELAY_ENCRYPTION_KEY=<key>
# Pass the same key to both RelayProducer and RelayConsumer
```

Or from the shell:
```bash
python -c "from streamrelay import generate_key; print(generate_key())"
```

---

### `StreamingExecutor` (Globus Compute)

High-level wrapper for Globus Compute users. Handles channel ID generation,
function submission with relay coordinates injected, and relay consumption.

```python
from streamrelay import StreamingExecutor

async with StreamingExecutor(
    endpoint_id="your-globus-endpoint-uuid",
    relay_url="wss://relay.example.com",
    relay_secret="MY_SECRET",
    encryption_key="",          # optional AES-256 key
    consumer_timeout=300.0,     # seconds to wait for first token
) as executor:
    async for token in executor.stream(my_inference_fn, prompt="Hello"):
        print(token, end="", flush=True)
```

Your remote function automatically receives `relay_url`, `channel_id`, and
optionally `relay_secret` / `encryption_key` as extra kwargs:

```python
def my_inference_fn(prompt, relay_url, channel_id, relay_secret="", encryption_key=""):
    # All imports must be inline — Globus Compute serializes only the function body
    from streamrelay import RelayProducer        # if streamrelay is installed on the endpoint
    with RelayProducer(relay_url, channel_id, relay_secret=relay_secret) as relay:
        for token in call_vllm_streaming(prompt):
            relay.send_token(token)
    return "ok"
```

If `streamrelay` is not installed on the endpoint workers, use the **inline producer
pattern** from [docs/tutorial.md](https://github.com/uicacer/streamrelay/blob/main/docs/tutorial.md) (Pattern B / Pattern C) — it requires
only `websockets` and `cryptography`, which are available on most HPC environments.

---

## Troubleshooting

**Consumer hangs and never receives any tokens**

1. Check the relay is reachable from both sides: `ws://your-relay:8765/health`
2. Check the `channel_id` matches exactly between producer and consumer — a mismatch
   means they connect to different channels and never find each other
3. Check the `relay_secret` matches — a wrong secret is rejected at handshake with
   WebSocket close code 4003; catch with `"4003" in str(e)`
4. Check the producer actually ran — if the Globus job failed before connecting,
   the consumer waits until the channel timeout (default 5 minutes)

**`ConnectionRefusedError` or `ConnectionClosedError`**

- Relay server is not running, or the URL/port is wrong
- For `wss://` connections: the TLS certificate must be valid (use Caddy or Let's Encrypt)
- For development: use `ws://` (unencrypted) with a local relay + Cloudflare tunnel for
  the public URL

**`InvalidTag` when decrypting**

- The `encryption_key` does not match between producer and consumer — generate once and
  store in both environments: `python -c "from streamrelay import generate_key; print(generate_key())"`

**Tokens arrive out of order**

- The relay forwards messages in arrival order — this should not happen
- If using the buffering path (producer connects first), messages are flushed in FIFO order

**`streamrelay` command not found after `pip install`**

- The `streamrelay` CLI is installed into your Python environment's bin directory
- Activate your virtual environment first, or use `python -m streamrelay.server`

**`ModuleNotFoundError: No module named 'streamrelay'` on the HPC node**

- The HPC endpoint workers may not have `streamrelay` installed
- Use the **inline producer pattern** (no install needed): see Pattern B in
  [docs/tutorial.md](https://github.com/uicacer/streamrelay/blob/main/docs/tutorial.md)

---

## Documentation

| Guide | What it covers |
|-------|---------------|
| [docs/tutorial.md](https://github.com/uicacer/streamrelay/blob/main/docs/tutorial.md) | **Start here.** Zero-to-streaming walkthrough: deploy relay, three producer patterns (pip install / inline / Globus Compute exec), consumer patterns, passing credentials to HPC jobs, E2E encryption, production checklist |
| [docs/deployment.md](https://github.com/uicacer/streamrelay/blob/main/docs/deployment.md) | Relay server deployment: Cloudflare tunnel, VM + Caddy + systemd, Docker Compose, health monitoring |
| [CONTRIBUTING.md](https://github.com/uicacer/streamrelay/blob/main/CONTRIBUTING.md) | Testing at three levels: unit tests, local end-to-end via Cloudflare, live relay test script |

---

## Citation

If you use streamrelay in your research, please cite the JOSS paper:

```bibtex
@article{nassar2026streamrelay,
  title   = {{streamrelay}: A {WebSocket} Relay for Real-Time Token Streaming
             from Batch {HPC} Executors},
  author  = {Nassar, Anas and Mohr, Steve and Apanasevich, Leonard and Sharma, Himanshu},
  journal = {Journal of Open Source Software},
  year    = {2026},
  doi     = {10.21105/joss.TODO},
}
```

streamrelay was developed as part of the STREAM system:

```bibtex
@inproceedings{nassar2026stream,
  title     = {{STREAM}: Smart Tiered Routing Engine for {AI} Models},
  author    = {Nassar, Anas and Mohr, Steve and Apanasevich, Leonard and Sharma, Himanshu},
  booktitle = {Proceedings of PEARC '26},
  year      = {2026},
}
```

---

## License

Apache 2.0 — see [LICENSE](https://github.com/uicacer/streamrelay/blob/main/LICENSE).
