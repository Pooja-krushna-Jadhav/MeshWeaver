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
