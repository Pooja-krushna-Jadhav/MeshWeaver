"""Utility helper functions for MeshWeaver."""

import hashlib
import json
import secrets
import time
from typing import Any


def utc_now() -> float:
    """Return current UTC timestamp in seconds as float."""
    return time.time()


def generate_nonce(length: int = 16) -> str:
    """Generate a cryptographically secure random hexadecimal nonce string."""
    return secrets.token_hex(length)


def generate_node_id(seed: str | None = None) -> str:
    """Generate a 160-bit Kademlia-compatible node ID as a 40-character hex string.
    
    If seed is provided, hashes it with SHA-1; otherwise generates random bytes.
    """
    if seed:
        return hashlib.sha1(seed.encode("utf-8")).hexdigest()
    random_bytes = secrets.token_bytes(20)
    return hashlib.sha1(random_bytes).hexdigest()


def node_id_to_int(node_id: str) -> int:
    """Convert a 40-character hex node ID string to an integer."""
    return int(node_id, 16)


def int_to_node_id(val: int) -> str:
    """Convert an integer back to a 40-character hex node ID string."""
    return f"{val:040x}"


def xor_distance(id1: str, id2: str) -> int:
    """Calculate the XOR metric distance between two 160-bit node IDs."""
    return node_id_to_int(id1) ^ node_id_to_int(id2)


class SafeJSONEncoder(json.JSONEncoder):
    """Custom JSON encoder handling bytes, sets, and special objects."""
    def default(self, o: Any) -> Any:
        if isinstance(o, bytes):
            return {"__bytes__": True, "hex": o.hex()}
        if isinstance(o, set):
            return list(o)
        if hasattr(o, "to_dict"):
            return o.to_dict()
        return super().default(o)


def json_dumps(data: Any) -> str:
    """Serialize data to a compact JSON string using SafeJSONEncoder."""
    return json.dumps(data, cls=SafeJSONEncoder, separators=(",", ":"), sort_keys=True)


def _json_object_hook(obj: dict[str, Any]) -> Any:
    if obj.get("__bytes__") is True and "hex" in obj:
        return bytes.fromhex(obj["hex"])
    return obj


def json_loads(data: str | bytes) -> Any:
    """Deserialize JSON string or bytes with support for custom byte structures."""
    if isinstance(data, bytes):
        data = data.decode("utf-8")
    return json.loads(data, object_hook=_json_object_hook)
