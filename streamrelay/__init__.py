"""
streamrelay — Real-time incremental output from batch HPC executors via WebSocket relay.

Solves a fundamental gap: HPC job schedulers (Globus Compute, SLURM, PBS) execute
functions to completion and return a single result. This library adds a lightweight
bidirectional channel so output streams out of the compute node in real time, with
both ends connecting *outbound* to the relay (no inbound ports needed, no VPN).

Works for any incrementally produced data: LLM tokens, simulation checkpoints,
solver convergence metrics, processed records, sensor readings, or any JSON-
serializable payload.

Basic usage:

    # On the HPC compute node (producer)
    from streamrelay import RelayProducer
    with RelayProducer(relay_url, channel_id) as relay:
        for token in your_model_stream(prompt):
            relay.send_token(token)
    # "done" signal sent automatically on exit

    # On your client/middleware (consumer)
    from streamrelay import RelayConsumer
    for token in RelayConsumer(relay_url, channel_id).stream():
        print(token, end="", flush=True)

    # High-level: submit a Globus Compute function and stream its output
    from streamrelay import StreamingExecutor
    async with StreamingExecutor(endpoint_id, relay_url) as executor:
        async for token in executor.stream(my_fn, prompt="Hello"):
            print(token, end="", flush=True)
"""

from streamrelay.consumer import RelayConsumer
from streamrelay.producer import RelayProducer
from streamrelay.server import start_relay
from streamrelay.crypto import encrypt_message, decrypt_message, generate_key
from streamrelay.executor import StreamingExecutor

__version__ = "0.1.1"
__all__ = [
    "RelayProducer",
    "RelayConsumer",
    "StreamingExecutor",
    "start_relay",
    "generate_key",
    "encrypt_message",
    "decrypt_message",
]
