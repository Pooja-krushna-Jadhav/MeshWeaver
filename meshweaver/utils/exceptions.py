"""MeshWeaver standard exception hierarchy."""

class MeshWeaverError(Exception):
    """Base exception for all MeshWeaver errors."""
    pass


class ConfigurationError(MeshWeaverError):
    """Raised when configuration is invalid or missing."""
    pass


class NetworkError(MeshWeaverError):
    """Base exception for network-related failures."""
    pass


class ConnectionFailedError(NetworkError):
    """Raised when establishing a connection to a peer fails."""
    pass


class ConnectionTimeoutError(NetworkError):
    """Raised when a network operation times out."""
    pass


class ProtocolError(MeshWeaverError):
    """Raised when a malformed, invalid, or corrupted packet is encountered."""
    pass


class FramingError(ProtocolError):
    """Raised when frame headers, length prefixes, or CRC32 checksums fail."""
    pass


class SerializationError(MeshWeaverError):
    """Raised when encoding or decoding a message payload fails."""
    pass


class SecurityError(MeshWeaverError):
    """Base exception for security and authentication errors."""
    pass


class AuthenticationError(SecurityError):
    """Raised when a peer identity, signature, or token fails verification."""
    pass


class UnauthorizedError(SecurityError):
    """Raised when a peer attempts an action it does not have permission for."""
    pass


class ReplayAttackError(SecurityError):
    """Raised when a message with an expired timestamp or duplicate nonce is detected."""
    pass


class SchedulingError(MeshWeaverError):
    """Raised when no suitable peer can be scheduled for a task."""
    pass


class TaskExecutionError(MeshWeaverError):
    """Raised when execution of a task fails on a worker node."""
    pass


class TaskTimeoutError(TaskExecutionError):
    """Raised when a task exceeds its execution time limit."""
    pass


class PeerNotFoundError(MeshWeaverError):
    """Raised when a requested peer cannot be located in the routing table or DHT."""
    pass
