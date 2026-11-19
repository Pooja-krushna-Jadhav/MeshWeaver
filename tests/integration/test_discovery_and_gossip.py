"""Integration test verifying peer discovery and resource gossip between multiple nodes."""

import asyncio
import pytest
from meshweaver.core.config import NodeConfig
from meshweaver.core.node import Node
from meshweaver.discovery.routing_table import Contact


@pytest.mark.asyncio
async def test_three_node_discovery_and_gossip():
    # Setup 3 nodes with dynamic ports
    cfg1 = NodeConfig(name="node-1")
    cfg1.network.tcp_port = 0
    cfg1.network.udp_port = 0
    cfg1.gossip.interval = 1.0  # Fast interval for test
    node1 = Node(cfg1)

    cfg2 = NodeConfig(name="node-2")
    cfg2.network.tcp_port = 0
    cfg2.network.udp_port = 0
    cfg2.gossip.interval = 1.0
    node2 = Node(cfg2)

    cfg3 = NodeConfig(name="node-3")
    cfg3.network.tcp_port = 0
    cfg3.network.udp_port = 0
    cfg3.gossip.interval = 1.0
    node3 = Node(cfg3)

    await node1.start()
    await node2.start()
    await node3.start()

    try:
        # Add Node 2 and Node 3 to Node 1's routing table
        contact2 = Contact(node2.node_id, "127.0.0.1", node2.tcp_server.actual_port, node2.udp_endpoint.actual_port)
        contact3 = Contact(node3.node_id, "127.0.0.1", node3.tcp_server.actual_port, node3.udp_endpoint.actual_port)

        node1.kademlia.routing_table.add_contact(contact2)
        node1.kademlia.routing_table.add_contact(contact3)

        # Trigger gossip round from Node 1
        await node1.gossip.do_gossip_round()
        await asyncio.sleep(0.5)

        # Check that Node 2 received Node 1's metrics via gossip
        assert node1.node_id in node2.peer_table._peers
        peer1_on_2 = node2.peer_table.get(node1.node_id)
        assert peer1_on_2 is not None
        assert peer1_on_2.metrics.memory_total_mb > 0

        # Check that Node 3 also knows about Node 1
        assert node1.node_id in node3.peer_table._peers

        # Now trigger gossip from Node 2 to propagate to Node 3
        await node2.gossip.do_gossip_round()
        await asyncio.sleep(0.5)

        # Node 3 now knows both Node 1 and Node 2!
        assert len(node3.peer_table.all_peers()) >= 2

    finally:
        await node1.stop()
        await node2.stop()
        await node3.stop()
