---
title: 'streamrelay: A WebSocket Relay for Real-Time Incremental Output from Batch HPC Executors'
tags:
  - Python
  - HPC
  - streaming
  - WebSocket
  - LLM
  - Globus Compute
  - SLURM
  - scientific computing
authors:
  - name: Anas Nassar
    orcid: 0009-0008-4225-5745
    corresponding: true
    affiliation: 1
  - name: Steve Mohr
    orcid: 0009-0009-0455-8216
    affiliation: 1
  - name: Leonard Apanasevich
    orcid: 0000-0002-5685-5871
    affiliation: 1
  - name: Himanshu Sharma
    orcid: 0000-0002-7498-8053
    affiliation: 1
affiliations:
  - name: Advanced Cyberinfrastructure for Education and Research (ACER), University of Illinois Chicago, USA
    index: 1
date: 2026-05-16
bibliography: paper.bib
---

# Summary

`streamrelay` is a Python library that solves a structural mismatch between HPC
batch execution and applications that require incremental output. HPC job schedulers —
SLURM, PBS, Globus Compute [@globuscompute2024] — execute jobs to completion and
return a single result. Applications that need to observe output as it is produced
(LLM chat interfaces, real-time simulation monitors, live analysis dashboards,
interactive notebooks) cannot use this model directly: the user sees nothing until
the job finishes.

`streamrelay` adds a lightweight WebSocket relay server that both the HPC compute
node (producer) and the requesting application (consumer) connect to **outbound**.
Neither side accepts an inbound connection, so no firewall exceptions, VPN, or
network reconfiguration are required. The relay is entirely scheduler-agnostic: the
same producer code works inside a SLURM batch script, a PBS job, a Globus Compute
function, or any subprocess that has outbound network access.

The library provides: a relay server (`streamrelay` CLI or `start_relay()` API);
producer and consumer client classes (`RelayProducer`, `RelayConsumer`) with
synchronous and asynchronous interfaces; optional AES-256-GCM end-to-end encryption
so the relay operator cannot read message payloads [@nist_gcm]; and a high-level
`StreamingExecutor` class for Globus Compute users that manages channel IDs and
function submission automatically.

# Statement of Need

Many HPC workloads produce output incrementally. Large language models generate
text token by token at 20–30 tokens per second; iterative solvers emit
convergence metrics at each step; molecular dynamics simulations produce
trajectory frames continuously; real-time data pipelines emit processed records as
they arrive. In all these cases, the value of streaming is the same: the consumer
observes progress immediately rather than waiting for the job to complete.

HPC clusters are the natural home for computation-intensive workloads: they provide
GPU and CPU resources unavailable on personal hardware at no marginal cost to
researchers. But HPC batch execution is fundamentally incompatible with streaming.
Globus Compute [@globuscompute2024], a widely used federated function execution
service for HPC, returns a single result when a function completes — there is no
mechanism for incremental output delivery during execution. Several HPC centers have
deployed LLM inference services [@first2025; @dartmouth2025; @purdue2025; @chatai2024];
a recurring limitation is the absence of streaming. The FIRST system [@first2025] at
Argonne National Laboratory reports 16.3 s median time-to-first-token as a direct
consequence of batch execution.

`streamrelay` provides a general-purpose, scheduler-agnostic solution to this
problem that requires no changes to existing job submission infrastructure. The
relay connection (`ws_connect`) is entirely independent of how the producer
obtains its data: replacing the LLM HTTP stream with a subprocess pipe, a file
iterator, or a socket yields the same architecture with no changes to the relay
or consumer. The protocol is payload-agnostic: producers send arbitrary JSON
messages and consumers receive them in order. LLM token streaming is the
motivating use case and is used for performance evaluation here (see the STREAM
system [@nassar2026stream]), but the same library is equally applicable to
simulation checkpoints, solver convergence metrics, molecular dynamics trajectory
frames, real-time pipeline records, or any other incrementally produced data.

Existing solutions either require inbound connections (precluded by HPC firewalls),
depend on specific middleware, or are tightly coupled to a single scheduler.
`streamrelay` is designed to be embedded in any HPC application as a library, or
used standalone as a relay server, with no scheduler dependencies.

# Design and Implementation

## Architecture

`streamrelay` separates concerns into two independent planes (Figure 1):

- **Control plane**: the user's existing job submission framework handles
  authentication, job dispatch, and final result retrieval. `streamrelay` does not
  touch this.
- **Data plane**: `streamrelay` carries incremental output from the compute node
  to the application in real time, independently of when the job completes.

![Dual-channel streaming architecture. The control plane (e.g., Globus Compute) handles task dispatch. The data plane (streamrelay relay server) carries incremental output in real time. Both the Task (HPC node) and the Application or Server connect outbound to the relay — no inbound ports required.](architecture.png){ width=70% }

The relay server maintains a **channel registry**. A channel is a matched pair of
WebSocket connections identified by a UUID (122 bits of entropy, computationally
infeasible to guess). The producer connects to `/produce/{channel_id}` and the
consumer connects to `/consume/{channel_id}`. The relay forwards JSON messages from
producer to consumer without interpretation.

**Buffering.** Messages arriving before the consumer connects are held in memory
(configurable limit, default 1,000 messages) and flushed when the consumer connects.
This handles the common case where the producer (HPC node) begins generating output
before the consumer (application) has established its connection.

**Orphan reaping.** A background task periodically removes channels where one side
never connected within a configurable timeout (default 300 seconds), preventing
memory leaks from failed or cancelled jobs.

## Message protocol

All messages are JSON strings forwarded by the relay without interpretation. The
protocol defines three control messages; the content of `"data"` is opaque to the
relay and can carry any application payload:

```
{"type": "data",   "payload": <any JSON>}  ← one incremental result
{"type": "done",   "meta": {...}}          ← job complete (optional metadata)
{"type": "error",  "message": "..."}       ← something went wrong on the producer
```

For backward compatibility with the LLM streaming convention used in STREAM
[@nassar2026stream], producers may also send `{"type": "token", "content": "..."}`,
which consumers treat identically to `"data"`.

When end-to-end encryption is enabled, each message is wrapped before transmission:

```
{"type": "enc", "d": "<base64(nonce + ciphertext + GCM tag)>"}
```

## Security

`streamrelay` enforces three independent security layers:

**Transport security (TLS).** Deploying the relay behind a TLS-terminating reverse
proxy (e.g., Caddy with auto-provisioned Let's Encrypt certificates) encrypts all
traffic in transit via `wss://`. See `docs/deployment.md` for a production setup.

**Access control (shared secret).** The relay accepts an optional pre-shared secret
(`--secret` flag or `RELAY_SECRET` environment variable). Every producer and
consumer must supply the same value at the WebSocket handshake; connections without
the correct secret are rejected before any channel state is created. The relay holds
no persistent state — all channel information is discarded once both sides
disconnect, and no authentication credentials traverse the relay.

**End-to-end payload encryption (AES-256-GCM).** TLS protects the link to the
relay but leaves message payloads visible to the relay operator. For sensitive
workloads — medical data, financial computations, proprietary simulation results —
`streamrelay` optionally encrypts each message with AES-256-GCM [@nist_gcm]: the
producer encrypts with a fresh 12-byte random nonce, the relay forwards opaque
ciphertext, and the consumer decrypts. The GCM authentication tag detects any
in-transit tampering. The relay operator sees only ciphertext. This layer is opt-in
and backward-compatible with unencrypted connections on the same channel.

The shared secret and encryption key are delivered to the compute node as job
arguments or environment variables — the same mechanism used for all other job
parameters in SLURM, PBS, and Globus Compute workflows. No changes to cluster
authentication infrastructure are required.

## Scheduler-agnostic design

The relay protocol places one requirement on the compute node: outbound TCP
access to the relay server's port (443 for `wss://`). This is standard policy at
most institutional HPC centers — the same outbound access that Globus Compute uses
for its own AMQP task routing. Because `streamrelay` does not interact with the
scheduler, it is compatible with any execution model, including environments where
installing Python packages on compute nodes is restricted: the inline producer pattern
(documented in `docs/tutorial.md`) requires only `websockets` and `cryptography`,
which are commonly available on HPC environments without additional installation.

## Globus Compute integration

An optional `StreamingExecutor` class (`pip install streamrelay[globus]`) provides
a high-level API for Globus Compute users. It generates a channel ID, submits the
remote function with relay coordinates injected as keyword arguments, and immediately
connects as a consumer — reducing a Globus Compute streaming integration to a
standard `async for` loop. The underlying `RelayProducer` and `RelayConsumer`
classes are fully independent of Globus Compute.

# Performance

`streamrelay` has been deployed in the STREAM system [@nassar2026stream] at the
University of Illinois Chicago for LLM inference on the Lakeshore HPC cluster
(NVIDIA A100 GPUs, SLURM scheduler, accessed via Globus Compute). This deployment
provides end-to-end measurements of the relay overhead in a representative
production-like workload. Measurements with a Qwen 2.5 72B model:

| Metric | Value |
|--------|-------|
| Median time-to-first-output with `streamrelay` | **0.85 s** |
| Median latency without streaming (batch mode) | **15.68 s** |
| Speedup | **18×** |
| Relay server RAM at single-user load | ~10 MB |
| Relay CPU overhead | negligible (dumb forwarder) |

The 0.85 s latency includes Globus Compute authentication and job dispatch latency.
The relay itself adds no measurable per-message overhead: it is a memory-copy
operation with no parsing or computation on the message content. This overhead
profile is independent of the payload type — simulation checkpoints, sensor
readings, or any other JSON-serializable data would observe the same relay
characteristics.

# Acknowledgements

`streamrelay` was developed as part of the STREAM project at the Advanced
Cyberinfrastructure for Education and Research (ACER) group at the University of
Illinois Chicago. We thank Lanre Adio (Cloud Engineer, ACER) for providing and
configuring the relay server infrastructure, and Marius Horga (Assistant Director
of Advanced Platforms for Research, ACER) for his support of this work. We also
thank the UIC ACER team for providing and maintaining the Lakeshore HPC cluster
used in development and evaluation.

# References
