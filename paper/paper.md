---
title: 'streamrelay: A WebSocket Relay for Real-Time Token Streaming from Batch HPC Executors'
tags:
  - Python
  - HPC
  - streaming
  - WebSocket
  - LLM
  - Globus Compute
  - SLURM
authors:
  - name: Anas Nassar
    orcid: 0009-0008-4225-5745
    corresponding: true
    affiliation: 1
  - name: Steve Mohr
    orcid: 0009-0009-0455-8216
    affiliation: 1
  - name: Lanre Adio
    affiliation: 1
  - name: Leonard Apanasevich
    orcid: 0000-0002-5685-5871
    affiliation: 1
  - name: Himanshu Sharma
    orcid: 0000-0002-7498-8053
    affiliation: 1
  - name: Marius Horga
    affiliation: 1
affiliations:
  - name: Advanced Cyberinfrastructure for Education and Research (ACER), University of Illinois Chicago, USA
    index: 1
date: 2026-05-16
bibliography: paper.bib
---

# Summary

`streamrelay` is a Python library that enables real-time token streaming from HPC
compute nodes to interactive user applications. It provides a lightweight WebSocket
relay server and client classes (`RelayProducer`, `RelayConsumer`) that allow any
application to receive incremental output from a remote HPC job as it is generated,
rather than waiting for the job to complete.

The library implements the data plane of a dual-channel architecture: the
existing job submission framework — SLURM, PBS, Globus Compute
[@globuscompute2024], or any other mechanism — continues to handle job dispatch and
authentication (control plane), while `streamrelay` handles real-time token
delivery (data plane). Both the HPC compute node and the client application connect
**outbound** to the relay, traversing institutional firewalls without requiring
inbound connection permissions, VPN access, or network reconfiguration. Optional
AES-256-GCM end-to-end encryption [@nist_gcm] ensures that the relay operator
cannot read the content of the messages it forwards.

# Statement of Need

Modern large language models (LLMs) generate text token by token, typically at
20–30 tokens per second. Interactive applications — chat interfaces, code
assistants, scientific Q&A systems — require each token to be delivered to the user
immediately as it is generated. This property, known as *streaming*, reduces
perceived latency from tens of seconds to under one second and is now a baseline
expectation for users of LLM systems.

HPC batch systems are designed for the opposite execution model: a job is submitted,
runs to completion, and the complete output is retrieved. Globus Compute
[@globuscompute2024] — a widely used federated function execution service that
dispatches Python functions to HPC endpoints running SLURM, PBS, or similar
schedulers — exemplifies this model: a function call returns a single result when
the function completes, with no mechanism for incremental output delivery during
execution.

Several HPC centers have deployed LLM inference services for their user communities
[@first2025; @dartmouth2025; @purdue2025; @chatai2024]. A recurring limitation in
these deployments is the absence of streaming: because HPC batch execution returns
only a final result, users experience multi-second delays before seeing any output.
The FIRST system [@first2025] at Argonne National Laboratory reports a median
time-to-first-token of 16.3 seconds under batch execution.

`streamrelay` addresses this gap directly. By adding a WebSocket relay between the
compute node and the requesting application, both sides can communicate in real time
without modifying the underlying job submission workflow. The relay is entirely
agnostic to how the job was submitted: the same `RelayProducer` class works inside
a SLURM batch script, a PBS job, a Globus Compute function, or any other execution
context where the compute node can make outbound network connections. No changes to
cluster network configuration are required, because compute nodes at most HPC
centers can already make outbound connections (the same mechanism used by Globus
Compute's own AMQP-based task routing).

# Design and Implementation

## Architecture

`streamrelay` implements the data plane of a dual-channel architecture
(\autoref{fig:architecture}). The control plane — job submission, authentication,
and result retrieval — is the responsibility of the user's existing execution
framework and is not modified.

![Dual-channel streaming architecture. The control plane (existing job scheduler or
Globus Compute) handles job dispatch. The data plane (streamrelay) carries tokens
in real time. Both sides connect outbound to the relay.\label{fig:architecture}](architecture.png)

The relay server maintains a channel registry. A **channel** is a matched pair of
WebSocket connections identified by a randomly generated UUID (122 bits of entropy).
The producer (HPC compute node) connects to `/produce/{channel_id}` and the
consumer (the requesting application) connects to `/consume/{channel_id}`. The
relay forwards JSON messages from producer to consumer without interpretation.

Messages arriving before the consumer connects are buffered in memory (configurable
limit, default 1,000 messages) to handle the case where the producer begins
generating tokens before the consumer has connected. A background reaper task
removes abandoned channels where one side never connected within a configurable
timeout (default 300 seconds).

## Message protocol

All messages are JSON strings:

```
{"type": "token",  "content": "Hello"}    ← one text chunk
{"type": "done",   "usage": {...}}         ← generation complete
{"type": "error",  "message": "..."}       ← something went wrong
```

The relay forwards messages as opaque strings. When end-to-end encryption is
enabled, each message is wrapped in an additional envelope before transmission:

```
{"type": "enc", "d": "<base64(nonce + ciphertext + GCM tag)>"}
```

## End-to-end encryption

`streamrelay` includes optional AES-256-GCM payload encryption
[@nist_gcm]. When a shared key is configured, the producer encrypts each message
with a fresh 12-byte random nonce before sending it to the relay. The relay
forwards the ciphertext unchanged. The consumer decrypts after receiving. The GCM
authentication tag detects any tampering in transit. TLS (`wss://`) protects the
link between each client and the relay; AES-256-GCM additionally protects the
content from the relay operator itself, which is important for users sending
sensitive data such as medical or financial information.

## Security

`streamrelay` enforces three independent security layers, each addressing a
distinct threat:

**Transport security (TLS).** When deployed behind a TLS-terminating reverse proxy
(e.g., Caddy with auto-provisioned Let's Encrypt certificates), all connections use
`wss://`. This encrypts traffic between each client and the relay and prevents
network-level eavesdropping.

**Access control (shared secret).** The relay can be started with a pre-shared
secret (`--secret`). Every producer and consumer must supply the same value at the
WebSocket handshake; connections without the correct secret are rejected before any
channel state is created. This prevents unauthorized parties from connecting to or
eavesdropping on channels. In addition, each channel uses a randomly generated UUID
(122 bits of entropy) — even knowing the relay address, guessing a valid channel ID
is computationally infeasible. The relay holds no persistent state: all channel
information is discarded once both sides disconnect, and no OAuth2 credentials or
user identity information traverse the relay.

The shared secret is delivered to the HPC compute node as a job argument or
environment variable — the same mechanism used to pass other job parameters in
SLURM, PBS, or Globus Compute workflows. No changes to cluster authentication
infrastructure are required.

**End-to-end payload encryption (AES-256-GCM).** TLS protects the link to the
relay, but the relay operator can still see plaintext token payloads. For sensitive
workloads, `streamrelay` provides optional AES-256-GCM payload encryption
[@nist_gcm]: the producer encrypts each message with a fresh 12-byte random nonce
before sending it to the relay, the relay forwards the opaque ciphertext unchanged,
and the consumer decrypts after receiving. The GCM authentication tag detects
tampering in transit. The result is that the relay operator sees only ciphertext
and cannot reconstruct any token payload. Encryption is opt-in and
backward-compatible.

## Globus Compute integration

An optional `StreamingExecutor` class (`pip install streamrelay[globus]`) provides
a high-level API for Globus Compute users. It generates the channel ID, submits the
function with the relay coordinates injected as keyword arguments, and connects as a
consumer — reducing the integration to a standard async-for loop. Globus Compute is
used here as one concrete control plane; the underlying `RelayProducer` and
`RelayConsumer` classes are fully independent of it.

# Performance

`streamrelay` was developed as part of the STREAM system [@nassar2026stream] at
the University of Illinois Chicago, where it has been in production use for LLM
inference on the Lakeshore HPC cluster. Measured end-to-end with a Qwen 2.5 72B
model on Lakeshore (NVIDIA A100 GPUs, SLURM cluster accessed via Globus Compute):

- Median time-to-first-token with `streamrelay`: **0.85 s**
- Median time-to-first-token without streaming (batch mode): **15.68 s**
- Relay overhead (added latency per token vs. direct connection): negligible —
  the relay is a dumb forwarder with no computation

The relay server itself requires approximately 10 MB of RAM and negligible CPU
at single-user load.

# Acknowledgements

`streamrelay` was developed as part of the STREAM project at the Advanced
Cyberinfrastructure for Education and Research (ACER) group at the University of
Illinois Chicago. We thank the UIC ACER team for providing and maintaining the
Lakeshore HPC cluster and the relay server infrastructure used in development and
production.

# References
