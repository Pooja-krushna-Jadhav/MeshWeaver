"""Integration tests for remote asynchronous task execution between nodes."""

import pytest
from meshweaver.core.config import NodeConfig
from meshweaver.core.node import Node
from meshweaver.execution.task import Task, TaskState


@pytest.mark.asyncio
async def test_remote_task_dispatch_and_result():
    # Node A is the client submitting tasks
    cfg_a = NodeConfig(name="node-submitter")
    cfg_a.network.tcp_port = 0
    cfg_a.network.udp_port = 0
    node_a = Node(cfg_a)

    # Node B is the worker executing tasks
    cfg_b = NodeConfig(name="node-worker")
    cfg_b.network.tcp_port = 0
    cfg_b.network.udp_port = 0
    node_b = Node(cfg_b)

    # Register custom worker task on Node B
    @node_b.registry.register("worker.compute_factorial")
    def compute_factorial(n: int) -> int:
        res = 1
        for i in range(2, n + 1):
            res *= i
        return res

    await node_a.start()
    await node_b.start()

    try:
        # Submit task from Node A to Node B
        task = Task(
            name="worker.compute_factorial",
            payload={"args": [6]},
            assigned_node_id=node_b.node_id,
        )

        result = await node_a.submit_task_to_peer(
            host="127.0.0.1",
            port=node_b.tcp_server.actual_port,
            task=task,
        )

        assert result.is_success
        assert result.result == 720  # 6! = 720
        assert result.worker_node_id == node_b.node_id
        assert task.state == TaskState.COMPLETED
        assert task.result == 720

    finally:
        await node_a.stop()
        await node_b.stop()


@pytest.mark.asyncio
async def test_remote_task_timeout():
    cfg_a = NodeConfig(name="node-submitter")
    cfg_a.network.tcp_port = 0
    cfg_a.network.udp_port = 0
    node_a = Node(cfg_a)

    cfg_b = NodeConfig(name="node-worker")
    cfg_b.network.tcp_port = 0
    cfg_b.network.udp_port = 0
    node_b = Node(cfg_b)

    @node_b.registry.register("worker.hang")
    def hanging_task():
        import time
        time.sleep(2.0)
        return "done"

    await node_a.start()
    await node_b.start()

    try:
        # Task with 0.1s timeout
        task = Task(name="worker.hang", timeout=0.1)
        result = await node_a.submit_task_to_peer(
            host="127.0.0.1",
            port=node_b.tcp_server.actual_port,
            task=task,
        )

        assert not result.is_success
        assert result.state == TaskState.FAILED
        assert "timeout" in result.error.lower()
        assert task.state == TaskState.FAILED
    finally:
        await node_a.stop()
        await node_b.stop()
