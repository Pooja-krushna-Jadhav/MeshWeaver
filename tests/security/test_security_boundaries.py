"""Security boundary tests: oversized frames, cloudpickle/admin enforcement, IPC isolation, replay, UDP spoofing, and HMAC attacks."""

import asyncio
import os
import struct
import pytest

from meshweaver.core.config import NodeConfig
from meshweaver.core.node import Node
from meshweaver.core.state import NodeInfo, PeerState
from meshweaver.execution.executor import TaskExecutor
from meshweaver.execution.serialization import serialize_callable
from meshweaver.execution.task import Task, TaskState, TaskType
from meshweaver.networking.framing import HEADER_FORMAT, MAGIC_BYTES, PROTOCOL_VERSION, Opcode
from meshweaver.networking.protocol import Message
from meshweaver.security.authentication import KeyManager, ReplayProtector
from meshweaver.security.permissions import AuthorizationPolicy
from meshweaver.utils.exceptions import ReplayAttackError, UnauthorizedError


@pytest.mark.asyncio
async def test_security_rejects_oversized_frame():
    cfg = NodeConfig(name="security-node")
    cfg.network.tcp_port = 0
    cfg.network.udp_port = 0
    cfg.network.max_frame_size = 1024  # Strict 1KB limit
    node = Node(cfg)
    await node.start()

    try:
        reader, writer = await asyncio.open_connection("127.0.0.1", node.tcp_server.actual_port)
        # Forge a 5MB length header with a 0-byte body (must reject before allocating 5MB)
        five_mb = 5 * 1024 * 1024
        forged_header = struct.pack(
            HEADER_FORMAT,
            MAGIC_BYTES,
            PROTOCOL_VERSION,
            int(Opcode.PING),
            0,
            five_mb,
            0,
        )

        writer.write(forged_header)
        await writer.drain()

        # Server should terminate the connection immediately due to frame size violation
        data = await asyncio.wait_for(reader.read(100), timeout=2.0)
        assert len(data) == 0  # Socket closed by server
        writer.close()
        await writer.wait_closed()
    finally:
        await node.stop()


@pytest.mark.asyncio
async def test_security_rejects_arbitrary_cloudpickle_by_default():
    cfg = NodeConfig(name="secure-worker")
    cfg.network.tcp_port = 0
    cfg.network.udp_port = 0
    node = Node(cfg)
    await node.start()

    submitter_cfg = NodeConfig(name="client")
    submitter_cfg.network.tcp_port = 0
    submitter_cfg.network.udp_port = 0
    submitter = Node(submitter_cfg)
    await submitter.start()

    try:
        fn_b64 = serialize_callable(lambda x: x * 2)
        task = Task(
            name="real.cloudpickle",
            task_type=TaskType.FUNCTION,
            payload={"fn_b64": fn_b64, "args": [21]},
        )

        result = await submitter.submit_task_to_peer(
            host="127.0.0.1",
            port=node.tcp_server.actual_port,
            task=task,
        )
        assert not result.is_success
        assert "disabled on this node" in result.error.lower()
    finally:
        await submitter.stop()
        await node.stop()


@pytest.mark.asyncio
async def test_cloudpickle_fails_closed_when_admin_nodes_empty():
    policy = AuthorizationPolicy(allow_cloudpickle=True, admin_nodes=[])
    fn_task = Task(name="fn.task", task_type=TaskType.FUNCTION)
    with pytest.raises(UnauthorizedError, match="ADMIN privilege"):
        policy.check_task_permission("any-node", fn_task)

    worker_cfg = NodeConfig(name="worker-no-admins")
    worker_cfg.network.tcp_port = 0
    worker_cfg.network.udp_port = 0
    worker_cfg.security.allow_cloudpickle = True
    worker_cfg.security.admin_nodes = []
    worker = Node(worker_cfg)

    client_cfg = NodeConfig(name="client")
    client_cfg.network.tcp_port = 0
    client_cfg.network.udp_port = 0
    client = Node(client_cfg)

    await worker.start()
    await client.start()

    try:
        fn_b64 = serialize_callable(lambda x: x + 10)
        task = Task(
            name="fn.empty_admins",
            task_type=TaskType.FUNCTION,
            payload={"fn_b64": fn_b64, "args": [5]},
        )
        result = await client.submit_task_to_peer(
            host="127.0.0.1",
            port=worker.tcp_server.actual_port,
            task=task,
        )
        assert not result.is_success
        assert "admin" in result.error.lower()
    finally:
        await client.stop()
        await worker.stop()


@pytest.mark.asyncio
async def test_cloudpickle_admin_nodes_enforcement():
    admin_cfg = NodeConfig(name="admin-node")
    admin_cfg.network.tcp_port = 0
    admin_cfg.network.udp_port = 0
    admin_node = Node(admin_cfg)

    non_admin_cfg = NodeConfig(name="non-admin-node")
    non_admin_cfg.network.tcp_port = 0
    non_admin_cfg.network.udp_port = 0
    non_admin_node = Node(non_admin_cfg)

    worker_cfg = NodeConfig(name="worker-admin-only")
    worker_cfg.network.tcp_port = 0
    worker_cfg.network.udp_port = 0
    worker_cfg.security.allow_cloudpickle = True
    worker_cfg.security.admin_nodes = [admin_node.node_id]
    worker = Node(worker_cfg)

    await asyncio.gather(admin_node.start(), non_admin_node.start(), worker.start())

    try:
        fn_b64 = serialize_callable(lambda x: x * 3)

        # Non-admin must be rejected
        non_admin_task = Task(
            name="fn.non_admin",
            task_type=TaskType.FUNCTION,
            payload={"fn_b64": fn_b64, "args": [7]},
        )
        res_denied = await non_admin_node.submit_task_to_peer(
            host="127.0.0.1",
            port=worker.tcp_server.actual_port,
            task=non_admin_task,
        )
        assert not res_denied.is_success
        assert "admin" in res_denied.error.lower()

        # Admin must succeed
        admin_task = Task(
            name="fn.admin",
            task_type=TaskType.FUNCTION,
            payload={"fn_b64": fn_b64, "args": [7]},
        )
        res_ok = await admin_node.submit_task_to_peer(
            host="127.0.0.1",
            port=worker.tcp_server.actual_port,
            task=admin_task,
        )
        assert res_ok.is_success
        assert res_ok.result == 21
    finally:
        await asyncio.gather(admin_node.stop(), non_admin_node.stop(), worker.stop())


@pytest.mark.asyncio
async def test_process_isolation_json_pipe_rejects_reduce_and_scrubs_env(tmp_path, monkeypatch):
    marker_file = tmp_path / "pwned.txt"
    monkeypatch.setenv("MESHWEAVER_SECRET", "top_secret_cluster_token")

    def malicious_reduce_return(path_str: str):
        class ReduceExploit:
            def __reduce__(self):
                import pathlib
                return (pathlib.Path(path_str).write_text, ("pwned",))

        return ReduceExploit()

    def read_meshweaver_env():
        import os
        return {k: v for k, v in os.environ.items() if k.startswith("MESHWEAVER_")}

    executor = TaskExecutor(
        node_id="iso-node",
        allow_cloudpickle=True,
        process_isolation=True,
    )

    # 1. Returning an unserializable object with __reduce__ fails JSON encoding in child, never runs __reduce__
    t_exploit = Task(
        name="reduce.escape",
        task_type=TaskType.FUNCTION,
        payload={"fn_b64": serialize_callable(malicious_reduce_return), "args": [str(marker_file)]},
        timeout=5.0,
    )
    res_exploit = await executor.execute_task(t_exploit)
    assert not res_exploit.is_success
    assert res_exploit.state == TaskState.FAILED
    assert "TypeError" in res_exploit.error
    assert not marker_file.exists()

    # 2. Verify MESHWEAVER_* environment variables are scrubbed inside the isolated worker
    t_env = Task(
        name="env.check",
        task_type=TaskType.FUNCTION,
        payload={"fn_b64": serialize_callable(read_meshweaver_env)},
        timeout=5.0,
    )
    res_env = await executor.execute_task(t_env)
    assert res_env.is_success
    assert res_env.result == {}


@pytest.mark.asyncio
async def test_replay_protector_post_auth_no_nonce_poisoning_and_max_entries_cap():
    # 1. Live node: forged bad signature using legitimate sender's nonce must NOT poison nonce cache
    secret = "shared_replay_test_secret"
    cfg_worker = NodeConfig(name="worker")
    cfg_worker.network.tcp_port = 0
    cfg_worker.network.udp_port = 0
    cfg_worker.security.enabled = True
    cfg_worker.security.require_signatures = True
    cfg_worker.security.shared_secret = secret
    worker = Node(cfg_worker)

    cfg_sender = NodeConfig(name="sender")
    cfg_sender.network.tcp_port = 0
    cfg_sender.network.udp_port = 0
    cfg_sender.security.enabled = True
    cfg_sender.security.require_signatures = True
    cfg_sender.security.shared_secret = secret
    sender = Node(cfg_sender)

    await worker.start()
    await sender.start()

    try:
        # Attacker sends forged message with bad signature and chosen nonce
        chosen_nonce = "legit-nonce-abc123"
        forged = Message.ping(sender_id=sender.node_id, receiver_id=worker.node_id)
        forged.nonce = chosen_nonce
        forged.signature = "00" * 32  # Bad signature

        forged_resp = await sender.tcp_client.send_and_receive(
            "127.0.0.1",
            worker.tcp_server.actual_port,
            forged,
            timeout=2.0,
        )
        assert forged_resp.msg_type == "error"
        assert forged_resp.payload["code"] == "UNAUTHORIZED"

        # Now real sender sends valid signed message with that exact same nonce -> must succeed
        legit = Message.ping(sender_id=sender.node_id, receiver_id=worker.node_id)
        legit.nonce = chosen_nonce
        sender.keys.sign_message(legit)

        legit_resp = await sender.tcp_client.send_and_receive(
            "127.0.0.1",
            worker.tcp_server.actual_port,
            legit,
            timeout=2.0,
        )
        assert legit_resp.msg_type == "pong"
    finally:
        await sender.stop()
        await worker.stop()

    # 2. ReplayProtector max_entries FIFO cap prevents unbounded cache growth
    rp = ReplayProtector(window_sec=60.0, max_entries=5)
    for i in range(20):
        m = Message.ping(sender_id="n1")
        m.nonce = f"nonce-{i}"
        rp.validate(m)
    assert len(rp._seen_nonces) == 5
    # Newest 5 retained, oldest evicted
    assert "nonce-19" in rp._seen_nonces
    assert "nonce-0" not in rp._seen_nonces
    # Replaying a retained nonce still triggers ReplayAttackError
    dup = Message.ping(sender_id="n1")
    dup.nonce = "nonce-19"
    with pytest.raises(ReplayAttackError, match="Duplicate nonce"):
        rp.validate(dup)


@pytest.mark.asyncio
async def test_udp_unsigned_and_spoofed_gossip_rejected():
    cfg = NodeConfig(name="udp-sec-node")
    cfg.network.tcp_port = 0
    cfg.network.udp_port = 0
    cfg.security.enabled = True
    cfg.security.require_signatures = True
    cfg.security.shared_secret = "udp_cluster_secret"
    node = Node(cfg)
    await node.start()

    try:
        fake_peer = PeerState(
            info=NodeInfo(
                node_id="a" * 40,
                name="spoofed-peer",
                host="127.0.0.1",
                tcp_port=9999,
                udp_port=9998,
            )
        )

        # 1. Unsigned gossip packet
        unsigned_gossip = Message.gossip(sender_id="b" * 40, peer_states=[fake_peer.to_dict()])
        node.udp_endpoint.send_message(unsigned_gossip, ("127.0.0.1", node.udp_endpoint.actual_port))

        # 2. Spoofed wrong-secret gossip packet
        wrong_keys = KeyManager(shared_secret="wrong_secret")
        spoofed_gossip = Message.gossip(sender_id="c" * 40, peer_states=[fake_peer.to_dict()])
        wrong_keys.sign_message(spoofed_gossip)
        node.udp_endpoint.send_message(spoofed_gossip, ("127.0.0.1", node.udp_endpoint.actual_port))

        await asyncio.sleep(0.1)
        assert "a" * 40 not in node.peer_table._peers
        assert "b" * 40 not in node.peer_table._peers
        assert "c" * 40 not in node.peer_table._peers
    finally:
        await node.stop()


@pytest.mark.asyncio
async def test_hmac_reflection_and_cross_node_replay_rejected():
    secret = "shared_hmac_mesh_secret"

    def make_node(name: str) -> Node:
        cfg = NodeConfig(name=name)
        cfg.network.tcp_port = 0
        cfg.network.udp_port = 0
        cfg.security.enabled = True
        cfg.security.require_signatures = True
        cfg.security.shared_secret = secret
        return Node(cfg)

    node_a = make_node("node-a")
    node_b = make_node("node-b")
    node_c = make_node("node-c")

    await asyncio.gather(node_a.start(), node_b.start(), node_c.start())

    try:
        # 1. HMAC reflection: message claiming sender_id == node_b.node_id signed with shared secret sent to node_b
        reflected = Message.ping(sender_id=node_b.node_id, receiver_id=node_b.node_id)
        node_a.keys.sign_message(reflected)
        refl_resp = await node_a.tcp_client.send_and_receive(
            "127.0.0.1",
            node_b.tcp_server.actual_port,
            reflected,
            timeout=2.0,
        )
        assert refl_resp.msg_type == "error"
        assert refl_resp.payload["code"] == "UNAUTHORIZED"
        assert "reflected" in refl_resp.payload["error"].lower()

        # 2. Cross-node replay: message signed by node_a specifically for receiver_id == node_b replayed to node_c
        targeted = Message.ping(sender_id=node_a.node_id, receiver_id=node_b.node_id)
        node_a.keys.sign_message(targeted)

        # Accepted by intended target node_b
        ok_resp = await node_a.tcp_client.send_and_receive(
            "127.0.0.1",
            node_b.tcp_server.actual_port,
            targeted,
            timeout=2.0,
        )
        assert ok_resp.msg_type == "pong"

        # Replayed exact signed message to node_c -> must be rejected
        replay_resp = await node_a.tcp_client.send_and_receive(
            "127.0.0.1",
            node_c.tcp_server.actual_port,
            targeted,
            timeout=2.0,
        )
        assert replay_resp.msg_type == "error"
        assert replay_resp.payload["code"] == "UNAUTHORIZED"
        assert "receiver_id" in replay_resp.payload["error"].lower()
    finally:
        await asyncio.gather(node_a.stop(), node_b.stop(), node_c.stop())
