# Deployment Guide

This guide covers four deployment scenarios, from local development to production.

---

## 1. Local development with Cloudflare Tunnel

This is the recommended way to test the full producer → relay → consumer flow on
your laptop, **without a public server or HPC access**. You simulate both sides
locally.

**Prerequisites:**
```bash
pip install streamrelay
brew install cloudflared        # macOS
# or: curl -L https://github.com/cloudflare/cloudflared/releases/latest/download/cloudflared-linux-amd64 -o cloudflared && chmod +x cloudflared
```

**Step 1 — Start the relay locally:**
```bash
streamrelay --port 8765 --secret test-secret
```

**Step 2 — Expose it via Cloudflare Tunnel (new terminal):**
```bash
cloudflared tunnel --url http://localhost:8765
```
This prints a public `wss://` URL like `wss://random-name.trycloudflare.com`.
Copy it — this is your `relay_url`.

**Step 3 — Run the consumer (new terminal):**
```python
# consumer.py
import uuid
from streamrelay import RelayConsumer

relay_url = "wss://random-name.trycloudflare.com"   # from Step 2
channel_id = "test-channel-001"

print("Waiting for tokens...")
for token in RelayConsumer(relay_url, channel_id, relay_secret="test-secret").stream():
    print(token, end="", flush=True)
print()
```
```bash
python consumer.py
```

**Step 4 — Run the producer (new terminal, simulates the HPC node):**
```python
# producer.py
from streamrelay import RelayProducer

relay_url = "wss://random-name.trycloudflare.com"   # same URL
channel_id = "test-channel-001"

with RelayProducer(relay_url, channel_id, relay_secret="test-secret") as relay:
    for word in ["Hello", " ", "from", " ", "the", " ", "compute", " ", "node", "!"]:
        relay.send_token(word)
```
```bash
python producer.py
```

You should see tokens appear in the consumer terminal as the producer sends them.

---

## 2. Production on a cloud VM (Caddy + systemd)

Any small VM with a public IP works (e.g., a $6/month DigitalOcean droplet or an
AWS t3.micro).

**Install:**
```bash
pip install streamrelay
```

**Systemd service** (`/etc/systemd/system/streamrelay.service`):
```ini
[Unit]
Description=streamrelay WebSocket relay
After=network.target

[Service]
ExecStart=/usr/local/bin/streamrelay --host 127.0.0.1 --port 8765 --secret YOUR_SECRET
Restart=always
RestartSec=5
Environment=RELAY_SECRET=YOUR_SECRET

[Install]
WantedBy=multi-user.target
```
```bash
systemctl enable --now streamrelay
```

**Caddy** (`/etc/caddy/Caddyfile`) for TLS termination — gives you `wss://`:
```
relay.your-domain.com {
    reverse_proxy localhost:8765
}
```
Caddy auto-provisions a Let's Encrypt certificate. Your clients connect to
`wss://relay.your-domain.com`.

---

## 3. Docker Compose

```yaml
# docker-compose.yml
services:
  streamrelay:
    image: python:3.12-slim
    command: >
      sh -c "pip install streamrelay &&
             streamrelay --host 0.0.0.0 --port 8765 --secret ${RELAY_SECRET}"
    ports:
      - "8765:8765"
    environment:
      - RELAY_SECRET=${RELAY_SECRET}
    restart: unless-stopped
```

```bash
RELAY_SECRET=my-secret docker compose up -d
```

For TLS, put a Caddy or Traefik container in front.

---

## 4. Monitoring

The relay exposes a `/health` WebSocket endpoint (no auth required):

```python
import asyncio, websockets, json

async def check():
    async with websockets.connect("wss://relay.your-domain.com/health") as ws:
        print(json.loads(await ws.recv()))

asyncio.run(check())
# {"status": "healthy", "active_channels": 3, "timestamp": "2026-05-17T..."}
```

Or via `websocat`:
```bash
websocat wss://relay.your-domain.com/health
```

The `active_channels` field shows how many channel pairs are currently active
(producer + consumer connected). Under normal load this should be low; a large
number could indicate orphaned channels or a channel reaper misconfiguration.
