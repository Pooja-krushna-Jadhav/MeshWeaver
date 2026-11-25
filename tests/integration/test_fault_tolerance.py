"""Integration tests verifying fault tolerance, heartbeat loss, and automatic task recovery."""

import asyncio
import pytest
from meshweaver.core.config import NodeConfig
from meshweaver.core.events import EVENT_HEARTBEAT_LOST
from meshweaver.core.node import Node
from meshweaver.core.state import PeerHealth, ResourceMetrics
from meshweaver.execution.task import Task, TaskState


@pytest.mark.asyncio
async def test_automatic_task_recovery_on_node_failure():
    # Setup Node A (Submitter)
    cfg_a = NodeConfig(name="node-submitter")
    cfg_a.network.tcp_port = 0
    cfg_a.network.udp_port = 0
    node_a = Node(cfg_a)
    # Ensure Node A delegates rather than executing locally
    node_a.resource_monitor.sample = lambda active_tasks=0: ResourceMetrics(
        cpu_percent=95.0, memory_percent=90.0, active_tasks=active_tasks, max_tasks=8, timestamp=100.0
    )

    # Setup Node B (Worker 1 that will fail)
    cfg_b = NodeConfig(name="node-worker-1")
    cfg_b.network.tcp_port = 0
    cfg_b.network.udp_port = 0
    node_b = Node(cfg_b)

    # Setup Node C (Worker 2 that acts as failover backup)
    cfg_c = NodeConfig(name="node-worker-2")
    cfg_c.network.tcp_port = 0
    cfg_c.network.udp_port = 0
    node_c = Node(cfg_c)

    @node_c.registry.register("math.square")
    def square(x: int) -> int:
        return x * x

    await node_a.start()
    await node_b.start()
    await node_c.start()

    try:
        # Register workers in Node A's peer table
        metrics_b = ResourceMetrics(cpu_percent=10.0, memory_percent=20.0, timestamp=100.0)
        metrics_c = ResourceMetrics(cpu_percent=20.0, memory_percent=20.0, timestamp=100.0)
        node_a.peer_table.update_peer(node_b.info, metrics=metrics_b)
        node_a.peer_table.update_peer(node_c.info, metrics=metrics_c)

        # Create a task assigned to Node B
        task = Task(
            name="math.square",
            payload={"args": [9]},
            assigned_node_id=node_b.node_id,
        )
        task.transition_to(TaskState.ASSIGNED)
        node_a.tasks[task.task_id] = task

        # Simulate Node B crashing/shutting down abruptly
        await node_b.stop()

        # Update peer table to mark Node B as DEAD so scheduler won't pick it again
        peer_b_state = node_a.peer_table.get(node_b.node_id)
        if peer_b_state:
            peer_b_state.health = PeerHealth.DEAD

        # Fire EVENT_HEARTBEAT_LOST for Node B
        await node_a.events.emit(EVENT_HEARTBEAT_LOST, {"node_id": node_b.node_id})

        # Wait briefly for RecoveryManager to retry and reschedule to Node C
        for _ in range(20):
            if task.state == TaskState.COMPLETED:
                break
            await asyncio.sleep(0.1)

        # Verify task was recovered and completed by Node C!
        assert task.state == TaskState.COMPLETED
        assert task.result == 81  # 9^2 = 81
        assert task.assigned_node_id == node_c.node_id
        assert task.retry_count == 1

    finally:
        await node_a.stop()
        await node_c.stop()


@pytest.mark.asyncio
async def test_caller_receives_recovered_result_on_worker_connection_drop():
    # Setup Node A (Submitter)
    cfg_a = NodeConfig(name="node-submitter-caller")
    cfg_a.network.tcp_port = 0
    cfg_a.network.udp_port = 0
    node_a = Node(cfg_a)
    # Ensure Node A delegates to remote mesh
    node_a.resource_monitor.sample = lambda active_tasks=0: ResourceMetrics(
        cpu_percent=95.0, memory_percent=90.0, active_tasks=active_tasks, max_tasks=8, timestamp=100.0
    )

    # Setup Node B (Worker 1 that will be closed)
    cfg_b = NodeConfig(name="node-worker-crash")
    cfg_b.network.tcp_port = 0
    cfg_b.network.udp_port = 0
    node_b = Node(cfg_b)

    # Setup Node C (Worker 2 backup)
    cfg_c = NodeConfig(name="node-worker-backup")
    cfg_c.network.tcp_port = 0
    cfg_c.network.udp_port = 0
    node_c = Node(cfg_c)

    @node_b.registry.register("math.mult")
    def mult_b(a: int, b: int) -> int:
        return a * b

    @node_c.registry.register("math.mult")
    def mult_c(a: int, b: int) -> int:
        return a * b

    await node_a.start()
    await node_b.start()
    await node_c.start()

    try:
        # Register workers in Node A's peer table
        metrics_b = ResourceMetrics(cpu_percent=5.0, memory_percent=10.0, timestamp=100.0)
        metrics_c = ResourceMetrics(cpu_percent=20.0, memory_percent=20.0, timestamp=100.0)
        node_a.peer_table.update_peer(node_b.info, metrics=metrics_b)
        node_a.peer_table.update_peer(node_c.info, metrics=metrics_c)

        # Stop worker B right before/as task is dispatched
        await node_b.stop()

        # Caller calls submit_task
        task = Task(name="math.mult", payload={"args": [6, 7]})
        result = await node_a.submit_task(task)

        # Caller should successfully receive recovered result from Node C!
        assert result.is_success
        assert result.result == 42
        assert result.worker_node_id == node_c.node_id
        assert task.state == TaskState.COMPLETED
        assert task.retry_count >= 1

    finally:
        await node_a.stop()
        await node_c.stop()


@pytest.mark.asyncio
async def test_organic_heartbeat_recovery_on_stalled_worker_without_duplicate_execution():
    """Organic integration test: HeartbeatMonitor + RecoveryManager detect a stalled worker
    during an in-flight submit_task call, cancel the hung RPC, and return the single
    recovered TaskResult from the healthy backup worker without duplicate execution."""
    cfg_a = NodeConfig(name="organic-submitter")
    cfg_a.network.tcp_port = 0
    cfg_a.network.udp_port = 0
    node_a = Node(cfg_a)
    node_a.heartbeat.interval = 0.05
    node_a.failure_detector.dead_misses = 3
    node_a.recovery.retry_policy.initial_delay = 0.05
    node_a.resource_monitor.sample = lambda active_tasks=0: ResourceMetrics(
        cpu_percent=95.0, memory_percent=90.0, active_tasks=active_tasks, max_tasks=8, timestamp=100.0
    )

    cfg_b = NodeConfig(name="stalled-worker-b")
    cfg_b.network.tcp_port = 0
    cfg_b.network.udp_port = 0
    node_b = Node(cfg_b)

    cfg_c = NodeConfig(name="healthy-worker-c")
    cfg_c.network.tcp_port = 0
    cfg_c.network.udp_port = 0
    node_c = Node(cfg_c)

    b_started = asyncio.Event()
    b_release = asyncio.Event()
    b_exec_count = 0
    c_exec_count = 0

    @node_b.registry.register("work.organic")
    async def work_b(val: int) -> dict:
        nonlocal b_exec_count
        b_exec_count += 1
        b_started.set()
        await b_release.wait()
        return {"worker": "b", "val": val}

    @node_c.registry.register("work.organic")
    async def work_c(val: int) -> dict:
        nonlocal c_exec_count
        c_exec_count += 1
        return {"worker": "c", "val": val * 2}

    # Wrap node_b's TCP handler so once the task stalls on node_b, heartbeats fail with error
    # while the in-flight task_request TCP connection remains stalled open.
    from meshweaver.networking.protocol import Message
    orig_b_handler = node_b._handle_tcp_message

    async def degraded_b_handler(message: Message, writer: asyncio.StreamWriter) -> Message | None:
        if message.msg_type == "heartbeat" and b_started.is_set():
            return Message.error(sender_id=node_b.node_id, receiver_id=message.sender_id, code="UNHEALTHY", message="stalled")
        return await orig_b_handler(message, writer)

    node_b.tcp_server.handler = degraded_b_handler

    await asyncio.gather(node_a.start(), node_b.start(), node_c.start())

    try:
        # Prefer Worker B initially (lowest CPU)
        node_a.peer_table.update_peer(
            node_b.info,
            metrics=ResourceMetrics(cpu_percent=5.0, memory_percent=10.0, timestamp=100.0),
        )
        node_a.peer_table.update_peer(
            node_c.info,
            metrics=ResourceMetrics(cpu_percent=30.0, memory_percent=20.0, timestamp=100.0),
        )

        task = Task(name="work.organic", payload={"args": [21]}, timeout=5.0, max_retries=2)
        result = await asyncio.wait_for(node_a.submit_task(task), timeout=5.0)

        assert result.is_success
        assert result.worker_node_id == node_c.node_id
        assert result.result == {"worker": "c", "val": 42}
        assert task.state == TaskState.COMPLETED
        assert b_exec_count == 1
        assert c_exec_count == 1
    finally:
        b_release.set()
        await asyncio.gather(node_a.stop(), node_b.stop(), node_c.stop())
