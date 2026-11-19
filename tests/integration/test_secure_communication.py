"""Integration tests for secure peer-to-peer communication and attack resistance."""

import pytest
from meshweaver.core.config import NodeConfig
from meshweaver.core.node import Node
from meshweaver.execution.task import Task, TaskState


@pytest.mark.asyncio
async def test_authenticated_peer_communication():
    secret = "production_shared_cluster_secret_42"

    cfg_a = NodeConfig(name="trusted-submitter")
    cfg_a.network.tcp_port = 0
    cfg_a.network.udp_port = 0
    cfg_a.security.enabled = True
    cfg_a.security.require_signatures = True
    cfg_a.security.shared_secret = secret
    node_a = Node(cfg_a)

    cfg_b = NodeConfig(name="trusted-worker")
    cfg_b.network.tcp_port = 0
    cfg_b.network.udp_port = 0
    cfg_b.security.enabled = True
    cfg_b.security.require_signatures = True
    cfg_b.security.shared_secret = secret
    node_b = Node(cfg_b)

    @node_b.registry.register("secure.sum")
    def secure_sum(vals: list[int]) -> int:
        return sum(vals)

    await node_a.start()
    await node_b.start()

    try:
        task = Task(name="secure.sum", payload={"args": [[10, 20, 30]]})
        result = await node_a.submit_task_to_peer(
            host="127.0.0.1",
            port=node_b.tcp_server.actual_port,
            task=task,
        )

        assert result.is_success
        assert result.result == 60
        assert task.state == TaskState.COMPLETED
    finally:
        await node_a.stop()
        await node_b.stop()


@pytest.mark.asyncio
async def test_unauthorized_intruder_rejected():
    valid_secret = "mesh_secret_token"

    cfg_b = NodeConfig(name="secure-worker")
    cfg_b.network.tcp_port = 0
    cfg_b.network.udp_port = 0
    cfg_b.security.enabled = True
    cfg_b.security.require_signatures = True
    cfg_b.security.shared_secret = valid_secret
    node_b = Node(cfg_b)

    # Node Intruder uses wrong secret
    cfg_intruder = NodeConfig(name="intruder")
    cfg_intruder.network.tcp_port = 0
    cfg_intruder.network.udp_port = 0
    cfg_intruder.security.enabled = True
    cfg_intruder.security.require_signatures = True
    cfg_intruder.security.shared_secret = "wrong_or_missing_secret"
    node_intruder = Node(cfg_intruder)

    await node_b.start()
    await node_intruder.start()

    try:
        task = Task(name="math.add", payload={"args": [1, 2]})
        result = await node_intruder.submit_task_to_peer(
            host="127.0.0.1",
            port=node_b.tcp_server.actual_port,
            task=task,
        )

        # Worker rejects intruder with error
        assert not result.is_success
        assert result.state == TaskState.FAILED
        assert "unauthorized" in result.error.lower() or "signature" in result.error.lower()
    finally:
        await node_b.stop()
        await node_intruder.stop()


@pytest.mark.asyncio
async def test_asymmetric_ed25519_first_contact_communication():
    # Asymmetric cryptography with NO shared secret
    cfg_a = NodeConfig(name="asym-submitter")
    cfg_a.network.tcp_port = 0
    cfg_a.network.udp_port = 0
    cfg_a.security.enabled = True
    cfg_a.security.require_signatures = True
    cfg_a.security.shared_secret = None
    node_a = Node(cfg_a)

    cfg_b = NodeConfig(name="asym-worker")
    cfg_b.network.tcp_port = 0
    cfg_b.network.udp_port = 0
    cfg_b.security.enabled = True
    cfg_b.security.require_signatures = True
    cfg_b.security.shared_secret = None
    node_b = Node(cfg_b)

    @node_b.registry.register("math.mult")
    def mult(a: int, b: int) -> int:
        return a * b

    await node_a.start()
    await node_b.start()

    try:
        task = Task(name="math.mult", payload={"args": [8, 9]})
        result = await node_a.submit_task_to_peer(
            host="127.0.0.1",
            port=node_b.tcp_server.actual_port,
            task=task,
        )

        assert result.is_success
        assert result.result == 72
        assert task.state == TaskState.COMPLETED
    finally:
        await node_a.stop()
        await node_b.stop()


@pytest.mark.asyncio
async def test_asymmetric_spoofed_public_key_rejected():
    cfg_b = NodeConfig(name="asym-target")
    cfg_b.network.tcp_port = 0
    cfg_b.network.udp_port = 0
    cfg_b.security.enabled = True
    cfg_b.security.require_signatures = True
    cfg_b.security.shared_secret = None
    node_b = Node(cfg_b)

    cfg_intruder = NodeConfig(name="asym-spoofer")
    cfg_intruder.network.tcp_port = 0
    cfg_intruder.network.udp_port = 0
    cfg_intruder.security.enabled = True
    cfg_intruder.security.require_signatures = True
    cfg_intruder.security.shared_secret = None
    node_intruder = Node(cfg_intruder)

    await node_b.start()
    await node_intruder.start()

    try:
        # Intruder attempts to forge sender_id to claim it is someone else
        from meshweaver.networking.protocol import Message
        req_msg = Message(
            sender_id="f" * 40,  # Faked victim ID
            receiver_id=node_b.node_id,
            msg_type="task_request",
            payload={"name": "math.add", "args": [1, 2]},
            sender_public_key=node_intruder.keys.public_key_hex,
        )
        node_intruder.keys.sign_message(req_msg)

        resp = await node_intruder.tcp_client.send_and_receive(
            "127.0.0.1",
            node_b.tcp_server.actual_port,
            req_msg,
            timeout=3.0,
        )

        assert resp.msg_type == "error"
        assert resp.payload.get("code") == "UNAUTHORIZED"
        assert "does not match" in resp.payload.get("error", "").lower()
    finally:
        await node_b.stop()
        await node_intruder.stop()

