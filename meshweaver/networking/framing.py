"""Binary message framing for MeshWeaver network communication.

Header layout (16 bytes big-endian):
 0                   1                   2                   3
 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1
+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
|                       Magic: "MSHW" (4B)                      |
+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
|  Version (1B) |  Opcode (1B)  |           Flags (2B)          |
+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
|                       Payload Length (4B)                     |
+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
|                      CRC32 Checksum (4B)                      |
+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
"""

import asyncio
from dataclasses import dataclass
from enum import IntEnum
import struct
import zlib
from meshweaver.utils.exceptions import FramingError

MAGIC_BYTES = b"MSHW"
PROTOCOL_VERSION = 1
HEADER_FORMAT = "!4sBBHII"
HEADER_SIZE = struct.calcsize(HEADER_FORMAT)  # 16 bytes


class Opcode(IntEnum):
    PING = 1
    PONG = 2
    TASK_REQUEST = 3
    TASK_RESPONSE = 4
    TASK_CANCEL = 5
    GOSSIP = 6
    KAD_RPC = 7
    HEARTBEAT = 8
    ERROR = 9


class FrameFlags(IntEnum):
    NONE = 0
    JSON = 0
    RAW_PICKLE = 1 << 0
    COMPRESSED = 1 << 1
    SIGNED = 1 << 2
    ENCRYPTED = 1 << 3


@dataclass(frozen=True)
class Frame:
    opcode: Opcode
    flags: int
    payload: bytes
    version: int = PROTOCOL_VERSION


def encode_frame(opcode: Opcode | int, payload: bytes, flags: int = FrameFlags.NONE) -> bytes:
    """Encode an opcode, flags, and payload bytes into a complete wire frame."""
    op_val = int(opcode)
    checksum = zlib.crc32(payload) & 0xFFFFFFFF
    header = struct.pack(
        HEADER_FORMAT,
        MAGIC_BYTES,
        PROTOCOL_VERSION,
        op_val,
        flags,
        len(payload),
        checksum,
    )
    return header + payload


async def read_frame(reader: asyncio.StreamReader, max_size: int = 16 * 1024 * 1024) -> Frame:
    """Asynchronously read exactly one framed message from an asyncio StreamReader.
    
    Raises:
        asyncio.IncompleteReadError: If socket closes before full frame is read.
        FramingError: If magic bytes, version, max length, or CRC32 checksum fail.
    """
    try:
        header_data = await reader.readexactly(HEADER_SIZE)
    except asyncio.IncompleteReadError as e:
        if len(e.partial) == 0:
            raise ConnectionResetError("Connection closed by peer") from None
        raise FramingError(f"Incomplete frame header: received {len(e.partial)}/{HEADER_SIZE} bytes") from e

    magic, version, opcode_val, flags, length, expected_checksum = struct.unpack(HEADER_FORMAT, header_data)

    if magic != MAGIC_BYTES:
        raise FramingError(f"Invalid magic bytes: expected {MAGIC_BYTES!r}, got {magic!r}")

    if version != PROTOCOL_VERSION:
        raise FramingError(f"Unsupported protocol version {version} (expected {PROTOCOL_VERSION})")

    if length > max_size:
        raise FramingError(f"Frame payload length {length} exceeds maximum allowed {max_size} bytes")

    if length > 0:
        try:
            payload = await reader.readexactly(length)
        except asyncio.IncompleteReadError as e:
            raise FramingError(f"Incomplete payload: received {len(e.partial)}/{length} bytes") from e
    else:
        payload = b""

    actual_checksum = zlib.crc32(payload) & 0xFFFFFFFF
    if actual_checksum != expected_checksum:
        raise FramingError(
            f"CRC32 checksum mismatch: expected {expected_checksum:#010x}, got {actual_checksum:#010x}"
        )

    try:
        opcode = Opcode(opcode_val)
    except ValueError:
        opcode = Opcode.ERROR

    return Frame(opcode=opcode, flags=flags, payload=payload, version=version)
