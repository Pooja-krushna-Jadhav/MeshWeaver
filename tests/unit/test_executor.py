"""Unit tests for TaskExecutor and TaskRegistry."""

import asyncio
import pytest
from meshweaver.execution.executor import TaskExecutor, TaskRegistry
from meshweaver.execution.task import Task, TaskState, TaskType
from meshweaver.utils.exceptions import SecurityError


@pytest.mark.asyncio
async def test_executor_builtins():
    registry = TaskRegistry()
    executor = TaskExecutor(node_id="test-node", registry=registry)

    # Test math.add
    task = Task(name="math.add", payload={"args": [10, 25]})
    result = await executor.execute_task(task)
    assert result.is_success
    assert result.result == 35
    assert result.state == TaskState.COMPLETED

    # Test math.vector_norm
    task2 = Task(name="math.vector_norm", payload={"args": [[3.0, 4.0]]})
    result2 = await executor.execute_task(task2)
    assert result2.is_success
    assert result2.result == 5.0


@pytest.mark.asyncio
async def test_executor_custom_async_handler():
    registry = TaskRegistry()

    @registry.register("custom.async_calc")
    async def custom_calc(multiplier: int):
        await asyncio.sleep(0.01)
        return multiplier * 100

    executor = TaskExecutor(node_id="test-node", registry=registry)
    task = Task(name="custom.async_calc", payload={"kwargs": {"multiplier": 7}})

    result = await executor.execute_task(task)
    assert result.is_success
    assert result.result == 700


@pytest.mark.asyncio
async def test_executor_timeout_enforcement():
    registry = TaskRegistry()

    @registry.register("slow.job")
    async def slow_job():
        await asyncio.sleep(1.0)
        return "finished"

    executor = TaskExecutor(node_id="test-node", registry=registry)
    task = Task(name="slow.job", timeout=0.05)  # 50ms timeout

    result = await executor.execute_task(task)
    assert not result.is_success
    assert result.state == TaskState.FAILED
    assert "timeout limit" in result.error


@pytest.mark.asyncio
async def test_executor_cloudpickle_disabled_by_default():
    executor = TaskExecutor(node_id="test-node", allow_cloudpickle=False)
    task = Task(name="arbitrary.fn", task_type=TaskType.FUNCTION, payload={"fn_b64": "dummy"})

    result = await executor.execute_task(task)
    assert not result.is_success
    assert "disabled on this node" in result.error


@pytest.mark.asyncio
async def test_executor_process_isolation_execution():
    registry = TaskRegistry()
    executor = TaskExecutor(node_id="test-node", registry=registry, process_isolation=True)

    task = Task(name="math.add", payload={"args": [15, 27]})
    result = await executor.execute_task(task)
    assert result.is_success
    assert result.result == 42
    assert executor.active_task_count == 0


@pytest.mark.asyncio
async def test_executor_process_isolation_timeout():
    import base64
    import cloudpickle

    def infinite_loop():
        import time
        while True:
            time.sleep(0.01)

    fn_b64 = base64.b64encode(cloudpickle.dumps(infinite_loop)).decode("utf-8")
    executor = TaskExecutor(
        node_id="test-node",
        allow_cloudpickle=True,
        process_isolation=True,
        max_concurrent_tasks=2,
    )

    task = Task(
        name="infinite_loop",
        task_type=TaskType.FUNCTION,
        payload={"fn_b64": fn_b64},
        timeout=0.3,
    )

    result = await executor.execute_task(task)
    assert not result.is_success
    assert result.state == TaskState.FAILED
    assert "timeout limit" in result.error
    # Ensure capacity is fully freed
    assert executor.active_task_count == 0
