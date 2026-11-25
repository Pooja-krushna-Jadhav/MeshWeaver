"""Unit tests for binary message framing and stream decoding."""

import asyncio
import struct
import pytest
from meshweaver.networking.framing import (
    HEADER_FORMAT,
    MAGIC_BYTES,
    PROTOCOL_VERSION,
    FrameFlags,
    Opcode,
    decode_frame_bytes,
    encode_frame,
    read_frame,
)
from meshweaver.utils.exceptions import FramingError


def test_frame_encoding_structure():
    payload = b"Hello, MeshWeaver!"
    frame_bytes = encode_frame(Opcode.PING, payload, flags=FrameFlags.JSON)

    assert len(frame_bytes) == 16 + len(payload)
    magic, version, opcode, flags, length, checksum = struct.unpack(HEADER_FORMAT, frame_bytes[:16])

    assert magic == MAGIC_BYTES
    assert version == PROTOCOL_VERSION
    assert opcode == Opcode.PING
    assert flags == FrameFlags.JSON
    assert length == len(payload)
    assert frame_bytes[16:] == payload


@pytest.mark.asyncio
async def test_frame_roundtrip():
    payload = b'{"action": "test", "val": 42}'
    encoded = encode_frame(Opcode.TASK_REQUEST, payload, flags=FrameFlags.JSON)

    reader = asyncio.StreamReader()
    reader.feed_data(encoded)
    reader.feed_eof()

    frame = await read_frame(reader)
    assert frame.opcode == Opcode.TASK_REQUEST
    assert frame.flags == FrameFlags.JSON
    assert frame.payload == payload
    assert frame.version == PROTOCOL_VERSION


@pytest.mark.asyncio
async def test_framing_corrupted_crc32():
    payload = b"Original uncorrupted data"
    encoded = bytearray(encode_frame(Opcode.PING, payload))

    # Corrupt one byte of payload
    encoded[-1] ^= 0xFF

    reader = asyncio.StreamReader()
    reader.feed_data(bytes(encoded))
    reader.feed_eof()

    with pytest.raises(FramingError, match="CRC32 checksum mismatch"):
        await read_frame(reader)


@pytest.mark.asyncio
async def test_framing_invalid_magic():
    payload = b"test"
    encoded = bytearray(encode_frame(Opcode.PING, payload))
    encoded[0:4] = b"BAD!"

    reader = asyncio.StreamReader()
    reader.feed_data(bytes(encoded))
    reader.feed_eof()

    with pytest.raises(FramingError, match="Invalid magic bytes"):
        await read_frame(reader)


@pytest.mark.asyncio
async def test_framing_payload_exceeds_max_size():
    payload = b"A" * 1024
    encoded = encode_frame(Opcode.PING, payload)

    reader = asyncio.StreamReader()
    reader.feed_data(encoded)
    reader.feed_eof()

    with pytest.raises(FramingError, match="exceeds maximum allowed"):
        await read_frame(reader, max_size=512)


@pytest.mark.asyncio
async def test_framing_empty_eof_raises_connection_reset():
    reader = asyncio.StreamReader()
    reader.feed_eof()

    with pytest.raises(ConnectionResetError, match="Connection closed"):
        await read_frame(reader)


@pytest.mark.asyncio
async def test_framing_partial_header_eof_raises_framing_error():
    encoded = encode_frame(Opcode.PING, b"hello")
    reader = asyncio.StreamReader()
    reader.feed_data(encoded[:5])
    reader.feed_eof()

    with pytest.raises(FramingError, match="Incomplete frame header"):
        await read_frame(reader)


@pytest.mark.asyncio
async def test_framing_partial_payload_eof_raises_framing_error():
    encoded = encode_frame(Opcode.PING, b"A" * 20)
    reader = asyncio.StreamReader()
    reader.feed_data(encoded[: 16 + 5])
    reader.feed_eof()

    with pytest.raises(FramingError, match="Incomplete payload"):
        await read_frame(reader)


@pytest.mark.asyncio
async def test_framing_unsupported_version():
    encoded = bytearray(encode_frame(Opcode.PING, b"test"))
    encoded[4] = 99

    reader = asyncio.StreamReader()
    reader.feed_data(bytes(encoded))
    reader.feed_eof()

    with pytest.raises(FramingError, match="Unsupported protocol version"):
        await read_frame(reader)


@pytest.mark.asyncio
async def test_framing_zero_length_payload():
    encoded = encode_frame(Opcode.HEARTBEAT, b"")
    reader = asyncio.StreamReader()
    reader.feed_data(encoded)
    reader.feed_eof()

    frame = await read_frame(reader)
    assert frame.opcode == Opcode.HEARTBEAT
    assert frame.payload == b""


@pytest.mark.asyncio
async def test_framing_unknown_opcode_raises_framing_error():
    encoded = encode_frame(250, b"payload")

    with pytest.raises(FramingError, match="Unknown frame opcode"):
        decode_frame_bytes(encoded)

    reader = asyncio.StreamReader()
    reader.feed_data(encoded)
    reader.feed_eof()

    with pytest.raises(FramingError, match="Unknown frame opcode"):
        await read_frame(reader)


def test_encode_frame_max_size_check():
    with pytest.raises(FramingError, match="exceeds maximum allowed"):
        encode_frame(Opcode.PING, b"X" * 100, max_size=64)


@pytest.mark.asyncio
async def test_framing_byte_by_byte_fragmented_stream_read():
    payload = b'{"fragmented": true, "count": 12345}'
    encoded = encode_frame(Opcode.TASK_REQUEST, payload, flags=FrameFlags.JSON)

    reader = asyncio.StreamReader()

    async def drip_feed():
        for i in range(len(encoded)):
            reader.feed_data(encoded[i : i + 1])
            await asyncio.sleep(0)
        reader.feed_eof()

    feeder = asyncio.create_task(drip_feed())
    try:
        frame = await read_frame(reader)
        assert frame.opcode == Opcode.TASK_REQUEST
        assert frame.flags == FrameFlags.JSON
        assert frame.payload == payload
    finally:
        await feeder
