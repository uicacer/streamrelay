# Tutorial: Zero to Streaming

This tutorial walks through the complete setup — from deploying the relay server to
sending your first token from an HPC compute node to a live application. It covers
three producer patterns depending on what you can install on the compute node.

---

## Overview

The full loop has three independent pieces:

```
[1] Relay server          [2] Producer                [3] Consumer
────────────────          ────────────                ────────────
A small VM or             Your code running            Your application
campus server with        on the HPC compute           (web server, notebook,
a public IP.              node — sends tokens          CLI) — receives tokens
Runs once, stays up.      outbound to the relay.       outbound from the relay.
```

Both producer and consumer connect **outbound** to the relay. Neither needs an
inbound port open. The relay is the only machine that needs a public IP.

---

## Part 1: Deploy the relay server

You need one machine with a public IP where both HPC and your app can reach port
443. A $6/month cloud VM (DigitalOcean, AWS t3.micro, etc.) is sufficient. The
relay uses ~10 MB RAM and negligible CPU at single-user load.

### Option A: Quick start (HTTP, development only)

```bash
pip install streamrelay
streamrelay --host 0.0.0.0 --port 8765 --secret YOUR_RELAY_SECRET
```

Clients connect with `ws://your-server-ip:8765`. Fine for development; use Option B
for any real deployment.

### Option B: Production (HTTPS/WSS with Caddy)

Caddy automatically provisions a free TLS certificate from Let's Encrypt.

**1. Install:**
```bash
pip install streamrelay
# Install Caddy: https://caddyserver.com/docs/install
```

**2. Systemd service** (`/etc/systemd/system/streamrelay.service`):
```ini
[Unit]
Description=streamrelay WebSocket relay
After=network.target

[Service]
ExecStart=/usr/local/bin/streamrelay --host 127.0.0.1 --port 8765
Restart=always
Environment=RELAY_SECRET=YOUR_RELAY_SECRET

[Install]
WantedBy=multi-user.target
```
```bash
systemctl enable --now streamrelay
```

**3. Caddyfile** (`/etc/caddy/Caddyfile`):
```
relay.your-domain.com {
    reverse_proxy localhost:8765
}
```
```bash
systemctl reload caddy
```

Clients now connect with `wss://relay.your-domain.com` (TLS, port 443).

**Generate your relay secret** (do this once, keep it private):
```bash
python -c "import secrets; print(secrets.token_urlsafe(32))"
```

**Verify the relay is up:**
```bash
# Should return: {"status": "healthy", "active_channels": 0, ...}
python -c "
import asyncio, websockets, json
async def check():
    async with websockets.connect('wss://relay.your-domain.com/health') as ws:
        print(json.loads(await ws.recv()))
asyncio.run(check())
"
```

---

## Part 2: The producer — sending tokens from HPC

The producer runs inside your HPC job (SLURM batch script, PBS job, or Globus
Compute function) and sends tokens outbound to the relay as they are generated.

The key requirement: the compute node must be able to make **outbound TCP
connections to port 443**. This is standard policy at most HPC centers — it is the
same mechanism Globus Compute uses for its own task routing.

There are three patterns depending on what you can install on the compute node:

---

### Pattern A: `pip install streamrelay` on the compute node

If your HPC environment allows pip installs (e.g., inside a conda environment, a
virtualenv, or an Apptainer/Singularity container):

```bash
pip install streamrelay
```

Then in your job script or remote function:

```python
from streamrelay import RelayProducer

RELAY_URL = "wss://relay.your-domain.com"
SECRET    = "YOUR_RELAY_SECRET"

with RelayProducer(RELAY_URL, channel_id, relay_secret=SECRET) as relay:
    for token in your_model.stream(prompt):
        relay.send_token(token)
# "done" is sent automatically when the with block exits
```

---

### Pattern B: Inline producer (no installation needed)

Most HPC nodes can't `pip install` inside a running job. This pattern embeds the
producer logic directly in your function — no external packages beyond `websockets`
and `cryptography`, which are commonly available on HPC environments.

Copy this inline block into your job code:

```python
def stream_via_relay(tokens, relay_url, channel_id, relay_secret="", encryption_key=""):
    """
    Send tokens to the relay without requiring streamrelay to be installed.
    Requires only: websockets, cryptography (standard on most HPC environments).

    Args:
        tokens:        Iterable of token strings (e.g. from your model's stream)
        relay_url:     wss://relay.your-domain.com
        channel_id:    UUID string shared with the consumer
        relay_secret:  Shared secret for relay auth (must match --secret on server)
        encryption_key: Optional base64 AES-256 key for E2E encryption
    """
    import json
    from websockets.sync.client import connect as ws_connect

    def _encrypt(plaintext_json):
        import base64, os
        from cryptography.hazmat.primitives.ciphers.aead import AESGCM
        key = base64.b64decode(encryption_key)
        nonce = os.urandom(12)
        blob = base64.b64encode(
            nonce + AESGCM(key).encrypt(nonce, plaintext_json.encode(), None)
        ).decode()
        return json.dumps({"type": "enc", "d": blob})

    def _send(ws, payload):
        raw = json.dumps(payload)
        ws.send(_encrypt(raw) if encryption_key else raw)

    ws_url = f"{relay_url}/produce/{channel_id}"
    if relay_secret:
        ws_url += f"?secret={relay_secret}"

    with ws_connect(ws_url) as ws:
        try:
            for token in tokens:
                _send(ws, {"type": "token", "content": token})
            _send(ws, {"type": "done"})
        except Exception as e:
            _send(ws, {"type": "error", "message": str(e)})
            _send(ws, {"type": "done"})
```

Usage inside a SLURM job or any Python function:

```python
# Example: stream vLLM output
import requests

def call_vllm_streaming(vllm_url, prompt):
    resp = requests.post(f"{vllm_url}/v1/completions",
                         json={"prompt": prompt, "stream": True}, stream=True)
    for line in resp.iter_lines():
        if line.startswith(b"data: ") and line != b"data: [DONE]":
            chunk = json.loads(line[6:])
            token = chunk["choices"][0].get("text", "")
            if token:
                yield token

stream_via_relay(
    tokens=call_vllm_streaming("http://localhost:8000", prompt),
    relay_url=relay_url,
    channel_id=channel_id,
    relay_secret=relay_secret,
)
```

---

### Pattern C: Globus Compute function (inline, exec pattern)

Globus Compute serializes your function and ships it to the HPC endpoint. If you
use PyInstaller or the STREAM desktop app, bytecode references can break
deserialization on the remote endpoint. The safe pattern is to define the function
from a source string via `exec()` at startup, which produces clean bytecode:

```python
_REMOTE_FN_SOURCE = """\
def remote_streaming(prompt, relay_url, channel_id, relay_secret="", encryption_key=""):
    # ALL imports must be inside the function — Globus Compute serializes only
    # the function body, not the surrounding module context.
    import json, requests
    from websockets.sync.client import connect as ws_connect

    def _encrypt(plaintext_json):
        import base64, os
        from cryptography.hazmat.primitives.ciphers.aead import AESGCM
        key = base64.b64decode(encryption_key)
        nonce = os.urandom(12)
        blob = base64.b64encode(
            nonce + AESGCM(key).encrypt(nonce, plaintext_json.encode(), None)
        ).decode()
        return json.dumps({"type": "enc", "d": blob})

    def _send(ws, payload):
        raw = json.dumps(payload)
        ws.send(_encrypt(raw) if encryption_key else raw)

    ws_url = f"{relay_url}/produce/{channel_id}"
    if relay_secret:
        ws_url += f"?secret={relay_secret}"

    with ws_connect(ws_url) as ws:
        try:
            resp = requests.post(
                "http://localhost:8000/v1/completions",
                json={"prompt": prompt, "stream": True},
                stream=True, timeout=180,
            )
            for line in resp.iter_lines():
                if line.startswith(b"data: ") and line != b"data: [DONE]":
                    token = json.loads(line[6:])["choices"][0].get("text", "")
                    if token:
                        _send(ws, {"type": "token", "content": token})
            _send(ws, {"type": "done"})
        except Exception as e:
            _send(ws, {"type": "error", "message": str(e)})
            _send(ws, {"type": "done"})
    return "ok"
"""

# Compile at startup — produces clean bytecode, works on any endpoint
_ns = {}
exec(compile(_REMOTE_FN_SOURCE, "<remote_streaming>", "exec"), _ns)
remote_streaming = _ns["remote_streaming"]
```

Submit and consume:

```python
import asyncio, uuid
from globus_compute_sdk import Executor
from streamrelay import RelayConsumer

async def run(prompt):
    channel_id = str(uuid.uuid4())

    with Executor(endpoint_id="YOUR_ENDPOINT_ID") as gc:
        future = gc.submit(
            remote_streaming,
            prompt=prompt,
            relay_url="wss://relay.your-domain.com",
            channel_id=channel_id,
            relay_secret="YOUR_RELAY_SECRET",
        )
        async for token in RelayConsumer(
            "wss://relay.your-domain.com", channel_id,
            relay_secret="YOUR_RELAY_SECRET"
        ):
            print(token, end="", flush=True)

asyncio.run(run("Explain quantum entanglement in one paragraph."))
```

---

## Part 3: The consumer — receiving tokens in your application

The consumer runs on your application side and receives tokens as they arrive.

### Simple CLI / script (synchronous)

```python
from streamrelay import RelayConsumer

for token in RelayConsumer(relay_url, channel_id, relay_secret=SECRET).stream():
    print(token, end="", flush=True)
print()
```

### FastAPI Server-Sent Events (async)

```python
from fastapi import FastAPI
from fastapi.responses import StreamingResponse
from streamrelay import RelayConsumer

app = FastAPI()

@app.get("/stream/{channel_id}")
async def stream(channel_id: str):
    async def generate():
        async for token in RelayConsumer(relay_url, channel_id, relay_secret=SECRET):
            yield f"data: {token}\n\n"
    return StreamingResponse(generate(), media_type="text/event-stream")
```

### Jupyter notebook

```python
from streamrelay import RelayConsumer
from IPython.display import display, HTML
import ipywidgets as widgets

out = widgets.Output()
display(out)

with out:
    for token in RelayConsumer(relay_url, channel_id, relay_secret=SECRET).stream():
        print(token, end="", flush=True)
```

---

## Part 4: Passing relay coordinates to the compute node

The producer needs three values: `relay_url`, `channel_id`, and `relay_secret`.
Generate `channel_id` fresh for each request on the consumer side, then pass all
three to the job. Common patterns:

**SLURM — `sbatch --export`:**
```bash
sbatch \
  --export=ALL,RELAY_URL=wss://relay.your-domain.com,CHANNEL_ID=$CHANNEL_ID,RELAY_SECRET=$SECRET \
  job.sh
```
Inside `job.sh`: `os.environ["RELAY_URL"]`, etc.

**Globus Compute — kwargs:**
```python
gc.submit(remote_fn, relay_url=RELAY_URL, channel_id=channel_id, relay_secret=SECRET)
```

**PBS — `-v`:**
```bash
qsub -v RELAY_URL=wss://...,CHANNEL_ID=$CHANNEL_ID,RELAY_SECRET=$SECRET job.sh
```

**Environment file (any scheduler):**
```bash
echo "RELAY_URL=wss://relay.your-domain.com" >> /tmp/relay_$CHANNEL_ID.env
echo "RELAY_SECRET=$SECRET" >> /tmp/relay_$CHANNEL_ID.env
# Job reads the file and deletes it after connecting
```

---

## Part 5: Adding E2E encryption

If your tokens contain sensitive data (medical, financial, personal), add
AES-256-GCM encryption so the relay operator cannot read the content.

**Generate a key once** (store in your `.env` or secrets manager, never in code):
```bash
python -c "from streamrelay import generate_key; print(generate_key())"
```

**Pass the same key to producer and consumer.** The relay sees only ciphertext.

Producer:
```python
with RelayProducer(relay_url, channel_id, relay_secret=SECRET,
                   encryption_key=KEY) as relay:
    relay.send_token(token)
```

Consumer:
```python
for token in RelayConsumer(relay_url, channel_id, relay_secret=SECRET,
                            encryption_key=KEY).stream():
    print(token, end="", flush=True)
```

Pass `KEY` to the compute node the same way as `relay_secret` — via job args or
environment variable.

---

## Checklist

Before going to production:

- [ ] Relay deployed on a VM with TLS (`wss://`) via Caddy or nginx
- [ ] `--secret` set on the relay and matching on all producers/consumers
- [ ] `channel_id` is a fresh UUID per request (never reuse)
- [ ] `encryption_key` set if handling sensitive data
- [ ] Relay secret and encryption key are in environment variables, not hardcoded
- [ ] Verified relay is reachable from both the HPC login node and your app server
- [ ] Tested with the live relay test script in `CONTRIBUTING.md`
