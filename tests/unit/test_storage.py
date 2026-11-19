"""Unit tests for SQLite persistent state store."""

import pytest
from meshweaver.execution.task import Task, TaskState
from meshweaver.storage.state_store import StateStore


@pytest.mark.asyncio
async def test_state_store_task_persistence(tmp_path):
    db_file = str(tmp_path / "test_state.db")
    store = StateStore(db_file)

    task = Task(name="math.add", payload={"args": [10, 20]})
    await store.save_task(task)

    # Retrieve from database
    loaded = await store.get_task(task.task_id)
    assert loaded is not None
    assert loaded.task_id == task.task_id
    assert loaded.name == "math.add"
    assert loaded.payload == {"args": [10, 20]}
    assert loaded.state == TaskState.CREATED

    # Update task state and persist again
    task.transition_to(TaskState.QUEUED)
    task.result = 30
    await store.save_task(task)

    updated = await store.get_task(task.task_id)
    assert updated is not None
    assert updated.state == TaskState.QUEUED
    assert updated.result == 30

    # Test listing tasks
    tasks = await store.list_tasks()
    assert len(tasks) == 1


@pytest.mark.asyncio
async def test_state_store_identity_persistence(tmp_path):
    db_file = str(tmp_path / "test_identity.db")
    store = StateStore(db_file)

    await store.save_identity(
        node_id="node_abc_123",
        name="edge-pi-1",
        public_key="pubkey_hex",
        private_key="privkey_hex",
    )

    # Simulate node reboot by creating fresh store pointing to same database
    store2 = StateStore(db_file)
    identity = await store2.load_identity()
    assert identity is not None
    assert identity["node_id"] == "node_abc_123"
    assert identity["name"] == "edge-pi-1"
    assert identity["public_key"] == "pubkey_hex"


@pytest.mark.asyncio
async def test_node_reboot_identity_and_task_restoration(tmp_path):
    from meshweaver.core.config import NodeConfig
    from meshweaver.core.node import Node

    db_file = str(tmp_path / "persistent_node.db")

    cfg1 = NodeConfig(name="persistent-node")
    cfg1.network.tcp_port = 0
    cfg1.network.udp_port = 0
    cfg1.storage.enabled = True
    cfg1.storage.db_path = db_file
    node1 = Node(cfg1)
    await node1.start()

    original_node_id = node1.node_id
    # Create and save a task that was in-flight (ASSIGNED)
    task = Task(name="math.add", payload={"args": [5, 5]})
    task.transition_to(TaskState.ASSIGNED)
    await node1.store.save_task(task)

    await node1.stop()

    # Re-instantiate node with the same storage database
    cfg2 = NodeConfig(name="persistent-node")
    cfg2.network.tcp_port = 0
    cfg2.network.udp_port = 0
    cfg2.storage.enabled = True
    cfg2.storage.db_path = db_file
    node2 = Node(cfg2)
    await node2.start()

    # Assert node_id was restored
    assert node2.node_id == original_node_id
    # Assert tasks were loaded and restored to RETRYING
    assert task.task_id in node2.tasks
    restored_task = node2.tasks[task.task_id]
    assert restored_task.name == "math.add"
    assert restored_task.state == TaskState.RETRYING

    await node2.stop()

