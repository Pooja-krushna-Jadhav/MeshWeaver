"""Unit tests for AsyncUDPEndpoint framing validation, drops, errors, and cleanup."""

import asyncio
import gc
import socket
import struct
import warnings
import pytest

from meshweaver.networking.framing import (
    HEADER_FORMAT,
    MAGIC_BYTES,
    PROTOCOL_VERSION,
    FrameFlags,
    Opcode,
    encode_frame,
)
from meshweaver.networking.protocol import Message
from meshweaver.networking.udp import AsyncUDPEndpoint
from meshweaver.utils.exceptions import FramingError, NetworkError


@pytest.mark.asyncio
async def test_udp_framed_roundtrip():
    received_on_server: list[Message] = []
    received_on_client: list[Message] = []
    reply_event = asyncio.Event()

    async def server_handler(msg: Message, addr: tuple[str, int]) -> Message | None:
        received_on_server.append(msg)
        return Message.pong(sender_id="server-1", receiver_id=msg.sender_id)

    async def client_handler(msg: Message, addr: tuple[str, int]) -> Message | None:
        received_on_client.append(msg)
        reply_event.set()
        return None

    ep1 = AsyncUDPEndpoint(host="127.0.0.1", port=0, handler=server_handler)
    ep2 = AsyncUDPEndpoint(host="127.0.0.1", port=0, handler=client_handler)
    await ep1.start()
    await ep2.start()

    try:
        ep2.send_message(Message.ping(sender_id="client-1"), ("127.0.0.1", ep1.actual_port))
        await asyncio.wait_for(reply_event.wait(), timeout=2.0)

        assert len(received_on_server) == 1
        assert received_on_server[0].msg_type == "ping"
        assert received_on_server[0].sender_id == "client-1"
        assert len(received_on_client) == 1
        assert received_on_client[0].msg_type == "pong"
        assert received_on_client[0].sender_id == "server-1"
    finally:
        await ep1.stop()
        await ep2.stop()


@pytest.mark.asyncio
async def test_udp_rejects_raw_unframed_and_malformed_frames():
    handled: list[Message] = []
    valid_event = asyncio.Event()

    async def handler(msg: Message, addr: tuple[str, int]) -> Message | None:
        handled.append(msg)
        valid_event.set()
        return None

    ep = AsyncUDPEndpoint(host="127.0.0.1", port=0, handler=handler)
    await ep.start()

    raw_sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    try:
        target = ("127.0.0.1", ep.actual_port)

        # 1. Raw unframed JSON
        raw_sock.sendto(b'{"sender_id":"evil","msg_type":"ping","timestamp":1.0,"nonce":"n1"}', target)

        # 2. Bad PROTOCOL_VERSION (version=99)
        valid_frame = bytearray(Message.ping(sender_id="bad-ver").to_frame())
        valid_frame[4] = 99
        raw_sock.sendto(bytes(valid_frame), target)

        # 3. Unknown Opcode (250) with valid CRC for its payload
        payload = Message.ping(sender_id="bad-op").to_bytes()
        bad_op_frame = encode_frame(250, payload)
        raw_sock.sendto(bad_op_frame, target)

        # 4. CRC mismatch
        corrupted = bytearray(Message.ping(sender_id="bad-crc").to_frame())
        corrupted[-1] ^= 0xFF
        raw_sock.sendto(bytes(corrupted), target)

        # 5. Valid framed packet sent after malformed ones
        raw_sock.sendto(Message.ping(sender_id="good-sender").to_frame(), target)

        await asyncio.wait_for(valid_event.wait(), timeout=2.0)
        await asyncio.sleep(0.05)

        assert len(handled) == 1
        assert handled[0].sender_id == "good-sender"
    finally:
        raw_sock.close()
        await ep.stop()


@pytest.mark.asyncio
async def test_udp_queue_full_drops_excess_packets():
    first_entered = asyncio.Event()
    release_handler = asyncio.Event()
    processed: list[str] = []

    async def slow_handler(msg: Message, addr: tuple[str, int]) -> Message | None:
        processed.append(msg.sender_id)
        if len(processed) == 1:
            first_entered.set()
            await release_handler.wait()
        return None

    ep = AsyncUDPEndpoint(
        host="127.0.0.1",
        port=0,
        handler=slow_handler,
        max_queue_size=1,
        worker_count=1,
    )
    await ep.start()

    raw_sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    try:
        target = ("127.0.0.1", ep.actual_port)

        # First packet enters slow_handler and blocks
        raw_sock.sendto(Message.ping(sender_id="p-0").to_frame(), target)
        await asyncio.wait_for(first_entered.wait(), timeout=2.0)

        # Flood 10 more packets while worker is blocked and queue capacity is 1
        for i in range(1, 11):
            raw_sock.sendto(Message.ping(sender_id=f"p-{i}").to_frame(), target)

        await asyncio.sleep(0.05)
        release_handler.set()
        await asyncio.sleep(0.05)

        # At most 1 active + 1 queued should have been processed (packets dropped gracefully)
        assert len(processed) <= 2
        assert "p-0" in processed
    finally:
        release_handler.set()
        raw_sock.close()
        await ep.stop()


@pytest.mark.asyncio
async def test_udp_oversized_datagram_and_inactive_transport_errors():
    ep = AsyncUDPEndpoint(host="127.0.0.1", port=0, max_datagram_size=128)
    await ep.start()
    try:
        with pytest.raises(FramingError, match="exceeds maximum allowed"):
            ep.send_to(b"X" * 129, ("127.0.0.1", 9999))
    finally:
        await ep.stop()

    with pytest.raises(NetworkError, match="transport is not active"):
        ep.send_to(b"hello", ("127.0.0.1", 9999))


@pytest.mark.asyncio
async def test_udp_socket_cleanup_on_bind_failure_and_stop_no_resource_warning():
    blocker = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    blocker.bind(("127.0.0.1", 0))
    occupied_port = blocker.getsockname()[1]

    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always", ResourceWarning)
        try:
            ep_fail = AsyncUDPEndpoint(host="127.0.0.1", port=occupied_port)
            # On Windows without SO_REUSEADDR conflict, or force failure via invalid host
            ep_bad = AsyncUDPEndpoint(host="256.256.256.256", port=0)
            with pytest.raises(OSError):
                await ep_bad.start()
            del ep_bad
            del ep_fail

            ep_ok = AsyncUDPEndpoint(host="127.0.0.1", port=0)
            await ep_ok.start()
            await ep_ok.stop()
            del ep_ok
            gc.collect()
        finally:
            blocker.close()

    resource_warnings = [w for w in caught if issubclass(w.category, ResourceWarning)]
    assert resource_warnings == []
