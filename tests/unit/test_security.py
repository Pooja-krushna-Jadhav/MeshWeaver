"""Unit tests for cryptographic signing, replay protection, and authorization."""

import pytest
from meshweaver.execution.task import Task, TaskType
from meshweaver.networking.protocol import Message
from meshweaver.security.authentication import KeyManager, ReplayProtector
from meshweaver.security.permissions import AuthorizationPolicy
from meshweaver.utils.exceptions import ReplayAttackError, UnauthorizedError
from meshweaver.utils.helpers import utc_now


def test_ed25519_key_manager_signing_and_verification():
    km = KeyManager()
    assert len(km.public_key_hex) > 0

    msg = Message.ping(sender_id="node_a", payload={"data": 123})
    km.sign_message(msg)
    assert msg.signature is not None

    # Verification with correct public key succeeds
    assert km.verify_message(msg, public_key_hex=km.public_key_hex) is True

    # Corrupted payload fails
    msg.payload["data"] = 999
    assert km.verify_message(msg, public_key_hex=km.public_key_hex) is False


def test_hmac_fallback_signing():
    km1 = KeyManager(shared_secret="super_secret_mesh_key")
    km2 = KeyManager(shared_secret="super_secret_mesh_key")
    km_wrong = KeyManager(shared_secret="wrong_secret")

    msg = Message.ping(sender_id="node_1")
    km1.sign_message(msg)

    assert km2.verify_message(msg) is True
    assert km_wrong.verify_message(msg) is False


def test_replay_protector_duplicate_nonce():
    rp = ReplayProtector(window_sec=5.0)
    msg = Message.ping(sender_id="node_a")

    rp.validate(msg)
    # Replay of same message should be rejected
    with pytest.raises(ReplayAttackError, match="Duplicate nonce detected"):
        rp.validate(msg)


def test_replay_protector_expired_timestamp():
    rp = ReplayProtector(window_sec=2.0)
    msg = Message.ping(sender_id="node_a")
    msg.timestamp = utc_now() - 10.0  # 10s in the past

    with pytest.raises(ReplayAttackError, match="exceeds allowable window"):
        rp.validate(msg)


def test_authorization_policy_node_whitelist():
    policy = AuthorizationPolicy(allowed_nodes=["node_allowed"])
    policy.check_node_allowed("node_allowed")

    with pytest.raises(UnauthorizedError, match="not authorized"):
        policy.check_node_allowed("node_intruder")


def test_authorization_policy_cloudpickle_defense():
    policy = AuthorizationPolicy(allow_cloudpickle=False)
    task = Task(name="test", task_type=TaskType.FUNCTION)

    with pytest.raises(UnauthorizedError, match="globally disabled"):
        policy.check_task_permission("any_node", task)


def test_create_ssl_context_none_when_unconfigured():
    from meshweaver.networking.tcp import create_ssl_context
    assert create_ssl_context() is None
    assert create_ssl_context(is_server=True) is None
