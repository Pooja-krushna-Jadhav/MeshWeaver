"""High-level message definitions and serialization for MeshWeaver."""

from dataclasses import dataclass, field
import json
import logging
from typing import Any
from meshweaver.networking.framing import Frame, FrameFlags, Opcode, encode_frame
from meshweaver.utils.exceptions import ProtocolError, SerializationError
from meshweaver.utils.helpers import generate_nonce, json_dumps, json_loads, utc_now

logger = logging.getLogger("meshweaver.protocol")


@dataclass
class Message:
    sender_id: str
    msg_type: str
    payload: dict[str, Any] = field(default_factory=dict)
    receiver_id: str | None = None
    timestamp: float = field(default_factory=utc_now)
    nonce: str = field(default_factory=generate_nonce)
    signature: str | None = None
    trace_id: str | None = None
    sender_public_key: str | None = None

    def to_dict(self) -> dict[str, Any]:
        return {
            "sender_id": self.sender_id,
            "receiver_id": self.receiver_id,
            "msg_type": self.msg_type,
            "timestamp": self.timestamp,
            "nonce": self.nonce,
            "payload": self.payload,
            "signature": self.signature,
            "trace_id": self.trace_id,
            "sender_public_key": self.sender_public_key,
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "Message":
        try:
            return cls(
                sender_id=data["sender_id"],
                receiver_id=data.get("receiver_id"),
                msg_type=data["msg_type"],
                timestamp=data.get("timestamp", utc_now()),
                nonce=data.get("nonce", generate_nonce()),
                payload=data.get("payload", {}),
                signature=data.get("signature"),
                trace_id=data.get("trace_id"),
                sender_public_key=data.get("sender_public_key"),
            )
        except KeyError as e:
            raise ProtocolError(f"Missing required message field: {e}") from e

    def to_bytes(self) -> bytes:
        """Serialize message to UTF-8 encoded JSON bytes."""
        try:
            return json_dumps(self.to_dict()).encode("utf-8")
        except Exception as e:
            raise SerializationError(f"Failed to serialize message: {e}") from e

    @classmethod
    def from_bytes(cls, data: bytes) -> "Message":
        """Deserialize message from UTF-8 encoded JSON bytes."""
        try:
            parsed = json_loads(data)
            if not isinstance(parsed, dict):
                raise ProtocolError(f"Expected JSON object, got {type(parsed).__name__}")
            return cls.from_dict(parsed)
        except Exception as e:
            if isinstance(e, ProtocolError):
                raise
            raise SerializationError(f"Failed to parse message bytes: {e}") from e

    def opcode(self) -> Opcode:
        """Map message type string to network Opcode."""
        mapping = {
            "ping": Opcode.PING,
            "pong": Opcode.PONG,
            "task_request": Opcode.TASK_REQUEST,
            "task_response": Opcode.TASK_RESPONSE,
            "task_cancel": Opcode.TASK_CANCEL,
            "gossip": Opcode.GOSSIP,
            "kad_rpc": Opcode.KAD_RPC,
            "heartbeat": Opcode.HEARTBEAT,
            "error": Opcode.ERROR,
        }
        return mapping.get(self.msg_type, Opcode.ERROR)

    def to_frame(self, flags: int = FrameFlags.NONE) -> bytes:
        """Serialize to complete binary wire frame."""
        payload_bytes = self.to_bytes()
        return encode_frame(self.opcode(), payload_bytes, flags=flags)

    @classmethod
    def from_frame(cls, frame: Frame) -> "Message":
        """Reconstruct Message from a decoded Frame."""
        return cls.from_bytes(frame.payload)

    # Factory methods
    @classmethod
    def ping(cls, sender_id: str, receiver_id: str | None = None, payload: dict[str, Any] | None = None) -> "Message":
        return cls(sender_id=sender_id, receiver_id=receiver_id, msg_type="ping", payload=payload or {})

    @classmethod
    def pong(cls, sender_id: str, receiver_id: str | None = None, payload: dict[str, Any] | None = None) -> "Message":
        return cls(sender_id=sender_id, receiver_id=receiver_id, msg_type="pong", payload=payload or {})

    @classmethod
    def heartbeat(cls, sender_id: str, receiver_id: str | None = None, metrics: dict[str, Any] | None = None) -> "Message":
        return cls(sender_id=sender_id, receiver_id=receiver_id, msg_type="heartbeat", payload=metrics or {})

    @classmethod
    def gossip(cls, sender_id: str, peer_states: list[dict[str, Any]]) -> "Message":
        return cls(sender_id=sender_id, msg_type="gossip", payload={"peers": peer_states})

    @classmethod
    def error(cls, sender_id: str, code: str, message: str, receiver_id: str | None = None) -> "Message":
        return cls(sender_id=sender_id, receiver_id=receiver_id, msg_type="error", payload={"code": code, "error": message})
