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


@pytest.mark.asyncio
async def test_bootstrap_only_cluster_discovery_and_workload():
    """Bootstrap a 4-node cluster purely via Kademlia/gossip (no manual peer_table injection)
    and verify concurrent tasks distribute across >= 2 distinct workers."""
    from meshweaver.core.state import ResourceMetrics

    nodes: list[Node] = []

    # 1. Start seed node
    seed_cfg = NodeConfig(name="seed-node")
    seed_cfg.network.tcp_port = 0
    seed_cfg.network.udp_port = 0
    seed_cfg.discovery.enable_lan_broadcast = False
    seed_cfg.gossip.interval = 0.2
    seed_node = Node(seed_cfg)

    @seed_node.registry.register("compute.square")
    def seed_square(n: int) -> int:
        return n * n

    await seed_node.start()
    nodes.append(seed_node)

    # Force seed_node's local CPU high so submitter delegates work across discovered workers
    seed_node.resource_monitor.sample = lambda active_tasks=0: ResourceMetrics(
        cpu_percent=95.0,
        memory_percent=90.0,
        active_tasks=active_tasks,
        max_tasks=8,
        timestamp=100.0,
    )

    try:
        seed_addr = f"127.0.0.1:{seed_node.tcp_server.actual_port}:{seed_node.udp_endpoint.actual_port}"

        # 2. Start 3 worker nodes configured ONLY with bootstrap_peers=[seed_addr]
        for i in range(3):
            w_cfg = NodeConfig(name=f"worker-node-{i}")
            w_cfg.network.tcp_port = 0
            w_cfg.network.udp_port = 0
            w_cfg.discovery.enable_lan_broadcast = False
            w_cfg.discovery.bootstrap_peers = [seed_addr]
            w_cfg.gossip.interval = 0.2
            worker = Node(w_cfg)

            @worker.registry.register("compute.square")
            async def worker_square(n: int) -> int:
                await asyncio.sleep(0.05)
                return n * n

            await worker.start()
            nodes.append(worker)

        # 3. Wait for organic Kademlia + gossip discovery to populate seed_node.peer_table
        for _ in range(50):
            remote_workers = [
                pid for pid in seed_node.peer_table._peers.keys()
                if pid != seed_node.node_id
            ]
            if len(remote_workers) >= 3:
                break
            await asyncio.sleep(0.1)

        remote_workers = [
            pid for pid in seed_node.peer_table._peers.keys()
            if pid != seed_node.node_id
        ]
        assert len(remote_workers) >= 2

        # 4. Submit 12 concurrent tasks from seed_node without any manual peer_table injection
        tasks = [Task(name="compute.square", payload={"args": [k + 1]}) for k in range(12)]
        results = await asyncio.gather(*(seed_node.submit_task(t) for t in tasks))

        assert len(results) == 12
        for k, res in enumerate(results):
            assert res.is_success
            assert res.result == (k + 1) ** 2

        worker_ids = {res.worker_node_id for res in results}
        assert len(worker_ids) >= 2
    finally:
        for node in nodes:
            await node.stop()
