"""Integration tests for distributed load-aware task scheduling across multiple nodes."""

import pytest
from meshweaver.core.config import NodeConfig
from meshweaver.core.node import Node
from meshweaver.core.state import ResourceMetrics
from meshweaver.execution.task import Task


@pytest.mark.asyncio
async def test_load_aware_task_routing():
    # Setup Node A (Submitter)
    cfg_a = NodeConfig(name="node-submitter")
    cfg_a.network.tcp_port = 0
    cfg_a.network.udp_port = 0
    cfg_a.scheduler.strategy = "least_loaded"
    node_a = Node(cfg_a)

    # Setup Node B (Busy Worker, CPU 80%)
    cfg_b = NodeConfig(name="node-worker-busy")
    cfg_b.network.tcp_port = 0
    cfg_b.network.udp_port = 0
    node_b = Node(cfg_b)

    # Setup Node C (Idle Worker, CPU 5%)
    cfg_c = NodeConfig(name="node-worker-idle")
    cfg_c.network.tcp_port = 0
    cfg_c.network.udp_port = 0
    node_c = Node(cfg_c)

    # Register task on both workers
    @node_b.registry.register("math.cube")
    @node_c.registry.register("math.cube")
    def cube(x: int) -> int:
        return x ** 3

    await node_a.start()
    await node_b.start()
    await node_c.start()

    try:
        # Mock Node A's local monitor to report heavy CPU load (90%) so it delegates to mesh
        node_a.resource_monitor.sample = lambda active_tasks=0: ResourceMetrics(
            cpu_percent=90.0,
            memory_percent=85.0,
            active_tasks=active_tasks,
            max_tasks=8,
            timestamp=100.0,
        )

        # Update Node A's peer table with reported metrics for Node B (80%) and Node C (5%)
        metrics_b = ResourceMetrics(cpu_percent=80.0, memory_percent=60.0, active_tasks=3, max_tasks=8, timestamp=100.0)
        metrics_c = ResourceMetrics(cpu_percent=5.0, memory_percent=20.0, active_tasks=0, max_tasks=8, timestamp=100.0)

        node_a.peer_table.update_peer(node_b.info, metrics=metrics_b)
        node_a.peer_table.update_peer(node_c.info, metrics=metrics_c)

        # Submit task on Node A without specifying worker
        task = Task(name="math.cube", payload={"args": [4]})
        result = await node_a.submit_task(task)

        assert result.is_success
        assert result.result == 64  # 4^3 = 64
        # Verify it was scheduled to the least loaded node (Node C!)
        assert result.worker_node_id == node_c.node_id
        assert task.assigned_node_id == node_c.node_id

    finally:
        await node_a.stop()
        await node_b.stop()
        await node_c.stop()
