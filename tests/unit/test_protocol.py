"""Unit tests for Message protocol encoding and deserialization."""

import pytest
from meshweaver.networking.framing import Opcode
from meshweaver.networking.protocol import Message
from meshweaver.utils.exceptions import ProtocolError, SerializationError


def test_message_roundtrip():
    msg = Message.ping(sender_id="node_a", receiver_id="node_b", payload={"data": 123})
    raw_bytes = msg.to_bytes()

    restored = Message.from_bytes(raw_bytes)
    assert restored.sender_id == "node_a"
    assert restored.receiver_id == "node_b"
    assert restored.msg_type == "ping"
    assert restored.payload == {"data": 123}
    assert restored.opcode() == Opcode.PING


def test_message_frame_conversion():
    msg = Message.heartbeat(sender_id="node_1", metrics={"cpu": 15.2, "ram": 45.0})
    frame_bytes = msg.to_frame()

    # Frame header is 16 bytes
    assert len(frame_bytes) > 16
    assert frame_bytes[:4] == b"MSHW"


def test_message_invalid_payload():
    with pytest.raises(SerializationError):
        Message.from_bytes(b"not json at all")


def test_message_missing_fields():
    with pytest.raises(ProtocolError):
        Message.from_bytes(b'{"payload": {}}')
