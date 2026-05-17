# Contributing & Testing Guide

## Running the tests

```bash
pip install -e ".[dev]"
pytest tests/ -v
```

All tests run locally — no HPC access, no public server, no credentials needed.
The integration tests spin up a relay on `localhost:18765`, run producer/consumer
pairs through it, and verify correctness including buffering and encryption.

---

## Testing the full end-to-end flow locally

JOSS reviewers and contributors can test the complete producer → relay → consumer
pipeline **entirely on a laptop** using a free Cloudflare tunnel. No HPC cluster
or public server required.

**Prerequisites:**
```bash
pip install streamrelay
# Install cloudflared (the Cloudflare tunnel CLI):
brew install cloudflared                     # macOS
# Linux: https://pkg.cloudflare.com/index.html
```

**Step 1 — Start a local relay:**
```bash
streamrelay --port 8765 --secret demo-secret
```

**Step 2 — Expose it publicly (new terminal):**
```bash
cloudflared tunnel --url http://localhost:8765
```
Copy the `wss://` URL it prints (e.g. `wss://xyz.trycloudflare.com`).

**Step 3 — Start the consumer (new terminal):**
```python
from streamrelay import RelayConsumer

for token in RelayConsumer("wss://xyz.trycloudflare.com", "ch1", relay_secret="demo-secret").stream():
    print(token, end="", flush=True)
```

**Step 4 — Run the producer (new terminal, simulates the HPC node):**
```python
from streamrelay import RelayProducer

with RelayProducer("wss://xyz.trycloudflare.com", "ch1", relay_secret="demo-secret") as relay:
    for word in ["Hello", " ", "world", "!"]:
        relay.send_token(word)
```

You should see `Hello world!` appear token-by-token in Step 3's terminal.

**Why this works:** The compute node only needs outbound TCP access to the relay,
which is the same requirement Globus Compute's own AMQP connections have. The
Cloudflare tunnel simulates a public relay server without requiring a VM.

---

## Development setup

```bash
git clone https://github.com/uicacer/streamrelay
cd streamrelay
pip install -e ".[dev,globus]"
pytest tests/ -v
```

---

## Submitting changes

1. Fork the repo and create a feature branch.
2. Run `pytest tests/ -v` — all 10 tests must pass.
3. Run `ruff check streamrelay/` — no lint errors.
4. Open a pull request with a description of what and why.
