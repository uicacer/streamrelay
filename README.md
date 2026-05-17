# streamrelay

**Real-time token streaming from batch HPC executors via WebSocket relay.**

[![PyPI](https://img.shields.io/pypi/v/streamrelay)](https://pypi.org/project/streamrelay/)
[![License](https://img.shields.io/badge/license-Apache%202.0-blue)](LICENSE)
[![Tests](https://github.com/uicacer/streamrelay/actions/workflows/tests.yml/badge.svg)](https://github.com/uicacer/streamrelay/actions)
[![JOSS](https://joss.theoj.org/papers/TODO/status.svg)](https://joss.theoj.org/papers/TODO)

**New here? Start with the [full tutorial](docs/tutorial.md)** — deploy the relay,
write a producer on your HPC node, consume tokens in your app, add encryption.
All in one place.

---

## The problem

HPC batch systems execute jobs to completion and return a single result. When that
job is an LLM inference request, the user stares at a blank screen for the full
generation time — often 15–20 seconds — before seeing any output.

**streamrelay solves this with a dual-channel architecture:**

- **Control plane** (unchanged): your existing execution framework — a SLURM or PBS
  job script, an SSH command, a Globus Compute function call — handles job
  submission and authentication exactly as before.
- **Data plane** (streamrelay): a lightweight WebSocket relay through which the
  compute node streams tokens back in real time as the GPU generates them.

Both the compute node (producer) and your application (consumer) connect
**outbound** to the relay. Neither side accepts an inbound connection — no firewall
exceptions, no VPN, no tunnels required.

```
Your application              Relay server              HPC compute node
────────────────              ────────────              ────────────────
1. Submit job via         2. Both connect               3. Job starts
   SLURM / PBS /             outbound here              4. Tokens stream
   Globus Compute /          (this is streamrelay)         to relay →
   SSH / anything    ◄─────────────────────────────────────────────────
5. Tokens arrive,
   first one in < 1s
```

Measured in the [STREAM system](https://github.com/uicacer/stream) at UIC:
**0.85 s** median time-to-first-token from HPC with streaming, vs. **15.68 s**
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
client and the relay server. See [docs/deployment.md](docs/deployment.md) for a
Caddy setup with auto-provisioned Let's Encrypt certificates.

### Layer 2 — Access control (shared secret)

Start the relay with `--secret MY_SECRET`. Every producer and consumer must supply
the same value as a query parameter (`?secret=MY_SECRET`). Connections without the
correct secret are rejected at the WebSocket handshake before any channel state is
created.

```bash
# Server
streamrelay --port 8765 --secret MY_SECRET

# Producer (HPC node) — same secret
with RelayProducer(relay_url, channel_id, relay_secret="MY_SECRET") as relay: ...

# Consumer (your app) — same secret
RelayConsumer(relay_url, channel_id, relay_secret="MY_SECRET").stream()
```

**How to share the secret with the HPC node:** pass it as a job argument, an
environment variable in your SLURM/PBS script, or as a keyword argument to your
Globus Compute function. It does not need to be embedded in code:

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

See [docs/deployment.md](docs/deployment.md) for a production deployment guide
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

## Documentation

| Guide | What it covers |
|-------|---------------|
| [docs/tutorial.md](docs/tutorial.md) | **Start here.** Zero-to-streaming walkthrough: deploy relay, three producer patterns (pip install / inline / Globus Compute exec), consumer patterns, passing credentials to HPC jobs, E2E encryption, production checklist |
| [docs/deployment.md](docs/deployment.md) | Relay server deployment: Cloudflare tunnel, VM + Caddy + systemd, Docker Compose, health monitoring |
| [CONTRIBUTING.md](CONTRIBUTING.md) | Testing at three levels: unit tests, local end-to-end via Cloudflare, live relay test script |

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

Apache 2.0 — see [LICENSE](LICENSE).
