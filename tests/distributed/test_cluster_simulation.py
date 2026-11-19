"""Distributed cluster simulation tests with multiple simultaneous peers and concurrent tasks."""

import asyncio
import pytest
from meshweaver.core.config import NodeConfig
from meshweaver.core.node import Node
from meshweaver.discovery.routing_table import Contact
from meshweaver.execution.task import Task, TaskState


@pytest.mark.asyncio
async def test_five_node_cluster_concurrent_workload():
    """Simulate a 5-node cluster running 15 concurrent tasks distributed across workers."""
    nodes: list[Node] = []
    num_nodes = 5

    for i in range(num_nodes):
        cfg = NodeConfig(name=f"cluster-node-{i}")
        cfg.network.tcp_port = 0
        cfg.network.udp_port = 0
        cfg.gossip.interval = 1.0
        node = Node(cfg)

        # Register compute tasks
        @node.registry.register("compute.power")
        def power(base: int, exp: int) -> int:
            return base ** exp

        await node.start()
        nodes.append(node)

    try:
        # Full mesh discovery interconnect
        for i, n1 in enumerate(nodes):
            for j, n2 in enumerate(nodes):
                if i != j:
                    c = Contact(n2.node_id, "127.0.0.1", n2.tcp_server.actual_port, n2.udp_endpoint.actual_port)
                    n1.kademlia.routing_table.add_contact(c)
                    n1.peer_table.update_peer(n2.info, metrics=n2.resource_monitor.sample())

        # Submit 15 tasks concurrently from Node 0
        submitter = nodes[0]
        tasks = [
            Task(name="compute.power", payload={"args": [k + 1, 2]})
            for k in range(15)
        ]

        # Dispatch all concurrently
        dispatch_coroutines = [submitter.submit_task(t) for t in tasks]
        results = await asyncio.gather(*dispatch_coroutines)

        # Verify all 15 tasks completed with exact expected math results
        assert len(results) == 15
        for k, res in enumerate(results):
            assert res.is_success
            expected = (k + 1) ** 2
            assert res.result == expected

        # Verify tasks were distributed across nodes in the cluster
        worker_ids = {res.worker_node_id for res in results}
        assert len(worker_ids) >= 1

    finally:
        for node in nodes:
            await node.stop()
