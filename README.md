# streamrelay

**Real-time token streaming from batch HPC executors via WebSocket relay.**

[![PyPI](https://img.shields.io/pypi/v/streamrelay)](https://pypi.org/project/streamrelay/)
[![License](https://img.shields.io/badge/license-Apache%202.0-blue)](LICENSE)
[![Tests](https://github.com/uicacer/streamrelay/actions/workflows/tests.yml/badge.svg)](https://github.com/uicacer/streamrelay/actions)
[![JOSS](https://joss.theoj.org/papers/TODO/status.svg)](https://joss.theoj.org/papers/TODO)

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

| Layer | Mechanism | How to enable |
|-------|-----------|---------------|
| Transport encryption | TLS (`wss://`) via reverse proxy | Deploy relay behind Caddy or nginx |
| Access control | Shared secret at WebSocket handshake | `--secret` flag on server |
| Payload privacy | AES-256-GCM end-to-end encryption | `encryption_key=` on producer + consumer |
| Channel isolation | Random UUID per request (122 bits entropy) | Always on |

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

## Deployment

See [docs/deployment.md](docs/deployment.md) for:

- Local development with Cloudflare Tunnel
- Production on a cloud VM (Caddy + systemd service)
- Docker Compose setup
- Monitoring via the `/health` endpoint

---

## Citation

If you use streamrelay in your research, please cite the JOSS paper:

```bibtex
@article{nassar2026streamrelay,
  title   = {{streamrelay}: A {WebSocket} Relay for Real-Time Token Streaming
             from Batch {HPC} Executors},
  author  = {Nassar, Anas and Mohr, Steve and Adio, Lanre and Apanasevich, Leonard
             and Sharma, Himanshu and Horga, Marius},
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
