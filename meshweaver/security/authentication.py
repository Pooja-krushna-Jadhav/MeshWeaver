"""Cryptographic identity, message signing, and verification."""

import base64
import hashlib
import hmac
import logging
from typing import Any
from meshweaver.networking.protocol import Message
from meshweaver.utils.exceptions import AuthenticationError, ReplayAttackError, SecurityError
from meshweaver.utils.helpers import json_dumps, utc_now

logger = logging.getLogger("meshweaver.security")

try:
    from cryptography.hazmat.primitives.asymmetric import ed25519
    from cryptography.hazmat.primitives import serialization
    HAS_CRYPTOGRAPHY = True
except ImportError:
    HAS_CRYPTOGRAPHY = False


class KeyManager:
    """Manages asymmetric Ed25519 keypairs with HMAC fallback for zero-dependency operation."""

    def __init__(self, private_key_pem: str | None = None, shared_secret: str | None = None) -> None:
        self.shared_secret = shared_secret.encode("utf-8") if shared_secret else None
        self._ed25519_priv: Any = None
        self._ed25519_pub: Any = None

        if HAS_CRYPTOGRAPHY and not self.shared_secret:
            if private_key_pem:
                self._ed25519_priv = serialization.load_pem_private_key(
                    private_key_pem.encode("utf-8"),
                    password=None,
                )
                self._ed25519_pub = self._ed25519_priv.public_key()
            else:
                self._ed25519_priv = ed25519.Ed25519PrivateKey.generate()
                self._ed25519_pub = self._ed25519_priv.public_key()

    @property
    def has_signing_key(self) -> bool:
        """Return True if a signing key or shared secret is configured."""
        return self.shared_secret is not None or self._ed25519_priv is not None

    @property
    def private_key_pem(self) -> str | None:
        """Export Ed25519 private key as PKCS8 unencrypted PEM string, or None."""
        if self._ed25519_priv is None:
            return None
        pem_bytes = self._ed25519_priv.private_bytes(
            encoding=serialization.Encoding.PEM,
            format=serialization.PrivateFormat.PKCS8,
            encryption_algorithm=serialization.NoEncryption(),
        )
        return pem_bytes.decode("utf-8")

    @property
    def public_key_hex(self) -> str:
        """Export public key as hex string, or 'hmac' in shared-secret mode."""
        if self.shared_secret:
            return "hmac"
        elif self._ed25519_pub is not None:
            raw_bytes = self._ed25519_pub.public_bytes(
                encoding=serialization.Encoding.Raw,
                format=serialization.PublicFormat.Raw,
            )
            return raw_bytes.hex()
        return "none"

    def sign_bytes(self, data: bytes) -> str:
        """Sign arbitrary bytes and return hex signature."""
        if self.shared_secret:
            sig = hmac.new(self.shared_secret, data, hashlib.sha256).digest()
            return sig.hex()
        elif self._ed25519_priv is not None:
            sig = self._ed25519_priv.sign(data)
            return sig.hex()
        raise SecurityError("No private key or shared secret available for signing")

    def verify_bytes(self, data: bytes, signature_hex: str, public_key_hex: str | None = None) -> bool:
        """Verify signature over arbitrary bytes."""
        try:
            sig_bytes = bytes.fromhex(signature_hex)
            if self.shared_secret:
                expected = hmac.new(self.shared_secret, data, hashlib.sha256).digest()
                return hmac.compare_digest(sig_bytes, expected)
            elif HAS_CRYPTOGRAPHY and public_key_hex:
                pub_bytes = bytes.fromhex(public_key_hex)
                peer_pub_key = ed25519.Ed25519PublicKey.from_public_bytes(pub_bytes)
                peer_pub_key.verify(sig_bytes, data)
                return True
            return False
        except Exception as e:
            logger.debug("Signature verification failed: %s", e)
            return False

    def sign_message(self, message: Message) -> None:
        """Sign canonical message content (sender_id + timestamp + nonce + payload + sender_public_key)."""
        if self._ed25519_pub is not None and not message.sender_public_key and self.public_key_hex != "none":
            message.sender_public_key = self.public_key_hex
        canon_bytes = self._canonicalize_message(message)
        message.signature = self.sign_bytes(canon_bytes)

    def verify_message(self, message: Message, public_key_hex: str | None = None) -> bool:
        """Verify message signature against sender's public key or shared secret."""
        if not message.signature:
            return False
        canon_bytes = self._canonicalize_message(message)
        return self.verify_bytes(canon_bytes, message.signature, public_key_hex=public_key_hex)

    def _canonicalize_message(self, msg: Message) -> bytes:
        data = {
            "sender_id": msg.sender_id,
            "receiver_id": msg.receiver_id,
            "msg_type": msg.msg_type,
            "timestamp": msg.timestamp,
            "nonce": msg.nonce,
            "payload": msg.payload,
            "sender_public_key": msg.sender_public_key,
        }
        return json_dumps(data).encode("utf-8")


class ReplayProtector:
    """Detects and rejects replayed messages based on timestamp drift and nonce tracking."""

    def __init__(self, window_sec: float = 30.0, max_entries: int = 10000) -> None:
        self.window_sec = window_sec
        self.max_entries = max_entries
        # nonce -> seen_timestamp
        self._seen_nonces: dict[str, float] = {}

    def _check_timestamp_at(self, message: Message, now: float) -> None:
        drift = abs(now - message.timestamp)
        if drift > self.window_sec:
            raise ReplayAttackError(
                f"Message timestamp drift {drift:.2f}s exceeds allowable window {self.window_sec}s"
            )

    def check_timestamp(self, message: Message) -> None:
        """Validate timestamp drift without recording nonce. Raises ReplayAttackError on violation."""
        self._check_timestamp_at(message, utc_now())

    def validate(self, message: Message) -> None:
        """Validate timestamp window and nonce uniqueness. Raises ReplayAttackError on violation."""
        now = utc_now()
        self._check_timestamp_at(message, now)

        self._cleanup(now)

        if message.nonce in self._seen_nonces:
            raise ReplayAttackError(f"Duplicate nonce detected: {message.nonce}")

        if self.max_entries <= 0:
            return

        while len(self._seen_nonces) >= self.max_entries:
            oldest_nonce = next(iter(self._seen_nonces))
            del self._seen_nonces[oldest_nonce]

        self._seen_nonces[message.nonce] = now

    def _cleanup(self, now: float) -> None:
        cutoff = now - self.window_sec
        expired = [n for n, t in self._seen_nonces.items() if t < cutoff]
        for n in expired:
            del self._seen_nonces[n]
