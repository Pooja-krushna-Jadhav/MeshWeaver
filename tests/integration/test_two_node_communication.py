"""Integration tests for two independent MeshWeaver nodes communicating."""

import pytest
from meshweaver.core.config import NodeConfig
from meshweaver.core.node import Node
from meshweaver.core.state import NodeStatus, PeerHealth


@pytest.mark.asyncio
async def test_two_node_tcp_ping_pong():
    # Setup Node A on dynamic port 0
    cfg_a = NodeConfig(name="node-alpha")
    cfg_a.network.tcp_port = 0
    cfg_a.network.udp_port = 0
    node_a = Node(cfg_a)

    # Setup Node B on dynamic port 0
    cfg_b = NodeConfig(name="node-beta")
    cfg_b.network.tcp_port = 0
    cfg_b.network.udp_port = 0
    node_b = Node(cfg_b)

    await node_a.start()
    await node_b.start()

    try:
        assert node_a.status == NodeStatus.ONLINE
        assert node_b.status == NodeStatus.ONLINE
        assert node_a.tcp_server.actual_port > 0
        assert node_b.tcp_server.actual_port > 0

        # Node A pings Node B over TCP
        resp_b = await node_a.ping_tcp("127.0.0.1", node_b.tcp_server.actual_port)
        assert resp_b.msg_type == "pong"
        assert resp_b.sender_id == node_b.node_id
        assert resp_b.payload["ack"] is True

        # Verify Node B was added to Node A's peer table and marked healthy
        assert node_b.node_id in node_a.peers
        peer_b_state = node_a.peers[node_b.node_id]
        assert peer_b_state.health == PeerHealth.HEALTHY
        assert peer_b_state.rtt_ms >= 0.0

        # Node B pings Node A over TCP
        resp_a = await node_b.ping_tcp("127.0.0.1", node_a.tcp_server.actual_port)
        assert resp_a.msg_type == "pong"
        assert resp_a.sender_id == node_a.node_id
        assert node_a.node_id in node_b.peers
    finally:
        await node_a.stop()
        await node_b.stop()
        assert node_a.status == NodeStatus.STOPPED
        assert node_b.status == NodeStatus.STOPPED


@pytest.mark.asyncio
async def test_tcp_connection_pool_reuse():
    cfg_a = NodeConfig(name="node-pool-client")
    cfg_a.network.tcp_port = 0
    cfg_a.network.udp_port = 0
    node_a = Node(cfg_a)

    cfg_b = NodeConfig(name="node-pool-server")
    cfg_b.network.tcp_port = 0
    cfg_b.network.udp_port = 0
    node_b = Node(cfg_b)

    await node_a.start()
    await node_b.start()

    try:
        assert node_a.tcp_client.pool is not None

        # Send multiple pings in sequence
        for _ in range(5):
            resp = await node_a.ping_tcp("127.0.0.1", node_b.tcp_server.actual_port)
            assert resp.msg_type == "pong"

        # Check that connection pool retained an open idle connection
        pool_key = ("127.0.0.1", node_b.tcp_server.actual_port)
        assert pool_key in node_a.tcp_client.pool._pool
        conns = node_a.tcp_client.pool._pool[pool_key]
        assert len(conns) == 1
        reader, writer = conns[0]
        assert not writer.is_closing()
    finally:
        await node_a.stop()
        await node_b.stop()

