import asyncio
import pytest
from meshweaver.core.config import NodeConfig
from meshweaver.core.node import Node
from meshweaver.execution.task import Task
from meshweaver.networking.protocol import Message
from meshweaver.reliability.retry import DeduplicationRegistry, RetryPolicy


def test_retry_policy_exponential_backoff():
    policy = RetryPolicy(max_retries=4, initial_delay=0.5, multiplier=2.0, max_delay=3.0)
    assert policy.get_delay(1) == 0.5
    assert policy.get_delay(2) == 1.0
    assert policy.get_delay(3) == 2.0
    # Capped at max_delay 3.0
    assert policy.get_delay(4) == 3.0


def test_deduplication_registry():
    dedup = DeduplicationRegistry(ttl_sec=60.0)
    task_id = "task-123"

    assert not dedup.is_processed(task_id)

    # First attempt starts
    assert dedup.record_started(task_id) is True
    assert dedup.is_processed(task_id) is True

    # Duplicate attempt is rejected
    assert dedup.record_started(task_id) is False

    # Record completion
    dedup.record_completed(task_id, {"status": "success", "result": 42})
    cached = dedup.get_result(task_id)
    assert cached is not None
    assert cached["result"] == 42


@pytest.mark.asyncio
async def test_node_in_flight_task_deduplication():
    cfg = NodeConfig(name="test-dedup-node")
    cfg.network.tcp_port = 0
    cfg.network.udp_port = 0
    node = Node(cfg)
    try:
        execution_count = 0

        @node.registry.register("count.work")
        async def slow_work():
            nonlocal execution_count
            execution_count += 1
            await asyncio.sleep(0.1)
            return {"count": execution_count}

        task = Task(name="count.work")
        msg1 = Message(sender_id="caller-1", msg_type="task_request", payload=task.to_dict())
        msg2 = Message(sender_id="caller-2", msg_type="task_request", payload=task.to_dict())

        # Send two identical task requests concurrently
        resp1, resp2 = await asyncio.gather(
            node._handle_task_request(msg1),
            node._handle_task_request(msg2),
        )

        assert resp1.msg_type == "task_response"
        assert resp2.msg_type == "task_response"
        assert resp1.payload["result"]["result"] == {"count": 1}
        assert resp2.payload["result"]["result"] == {"count": 1}
        # Underlying execution must occur exactly once
        assert execution_count == 1
    finally:
        await node.stop()

