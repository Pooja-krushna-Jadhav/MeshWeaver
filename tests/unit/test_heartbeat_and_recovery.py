"""Deterministic unit tests for FailureDetector, HeartbeatMonitor, RecoveryManager, and DeduplicationRegistry."""

import asyncio
import pytest

from meshweaver.core.events import EVENT_HEARTBEAT_LOST, Event, EventBus
from meshweaver.core.state import NodeInfo, PeerHealth
from meshweaver.execution.results import TaskResult
from meshweaver.execution.task import Task, TaskState
from meshweaver.gossip.peer_state import PeerTable
from meshweaver.networking.protocol import Message
from meshweaver.reliability.failure_detector import FailureDetector
from meshweaver.reliability.heartbeat import HeartbeatMonitor
from meshweaver.reliability.recovery import RecoveryManager
from meshweaver.reliability.retry import DeduplicationRegistry, RetryPolicy


def test_failure_detector_miss_thresholds_and_monotonic_timestamp():
    fd = FailureDetector(suspect_timeout=10.0, dead_timeout=20.0, dead_misses=3)

    # Unknown peer is DEAD
    assert fd.get_health("unknown-peer") == PeerHealth.DEAD

    # Registered peer starts healthy, 1st miss -> SUSPECT, 3 misses -> DEAD
    fd.register_peer("peer-1")
    assert fd.get_health("peer-1") == PeerHealth.HEALTHY
    initial_ts = fd._last_heartbeat["peer-1"]

    fd.record_failure("peer-1")
    assert fd.get_health("peer-1") == PeerHealth.SUSPECT
    assert fd._last_heartbeat["peer-1"] == initial_ts  # Failures do not corrupt timestamp

    fd.record_failure("peer-1")
    assert fd.get_health("peer-1") == PeerHealth.SUSPECT

    fd.record_failure("peer-1")
    assert fd.get_health("peer-1") == PeerHealth.DEAD

    # Successful heartbeat resets miss count and updates timestamp forward
    fd.record_heartbeat("peer-1")
    assert fd.get_health("peer-1") == PeerHealth.HEALTHY
    assert fd._miss_counts["peer-1"] == 0
    assert fd._last_heartbeat["peer-1"] >= initial_ts


@pytest.mark.asyncio
async def test_heartbeat_monitor_error_response_does_not_mark_alive():
    peer_id = "1" * 40
    peer_table = PeerTable()
    peer_table.update_peer(NodeInfo(node_id=peer_id, name="worker", host="127.0.0.1", tcp_port=9000, udp_port=9001))
    fd = FailureDetector(suspect_timeout=30.0, dead_timeout=60.0, dead_misses=3)
    events = EventBus()
    lost_events: list[Event] = []
    events.subscribe(EVENT_HEARTBEAT_LOST, lambda e: lost_events.append(e))

    async def error_send(host: str, port: int, msg: Message, timeout: float) -> Message:
        return Message.error(sender_id=peer_id, code="UNAUTHORIZED", message="rejected")

    monitor = HeartbeatMonitor(
        node_id="0" * 40,
        peer_table=peer_table,
        failure_detector=fd,
        events=events,
        send_fn=error_send,
        interval=0.5,
    )

    # 1st probe returning error -> SUSPECT (not HEALTHY)
    await monitor.probe_all_peers()
    assert peer_table.get(peer_id).health == PeerHealth.SUSPECT
    assert len(lost_events) == 0

    # 2nd probe returning error -> still SUSPECT
    await monitor.probe_all_peers()
    assert peer_table.get(peer_id).health == PeerHealth.SUSPECT
    assert len(lost_events) == 0

    # 3rd probe returning error -> DEAD and emits EVENT_HEARTBEAT_LOST
    await monitor.probe_all_peers()
    await asyncio.sleep(0)
    assert peer_table.get(peer_id).health == PeerHealth.DEAD
    assert len(lost_events) == 1
    await events.cancel_pending()


@pytest.mark.asyncio
async def test_heartbeat_monitor_emits_lost_on_error_and_recovers_on_pong():
    peer_id = "2" * 40
    peer_table = PeerTable()
    peer_table.update_peer(NodeInfo(node_id=peer_id, name="worker", host="127.0.0.1", tcp_port=9000, udp_port=9001))
    fd = FailureDetector(suspect_timeout=30.0, dead_timeout=60.0, dead_misses=2)
    events = EventBus()
    lost_events: list[Event] = []
    events.subscribe(EVENT_HEARTBEAT_LOST, lambda e: lost_events.append(e))

    respond_with_error = True

    async def fake_send(host: str, port: int, msg: Message, timeout: float) -> Message:
        if respond_with_error:
            return Message.error(sender_id=peer_id, code="ERR", message="fail")
        return Message.pong(sender_id=peer_id, payload={"ack": True})

    monitor = HeartbeatMonitor(
        node_id="0" * 40,
        peer_table=peer_table,
        failure_detector=fd,
        events=events,
        send_fn=fake_send,
        interval=0.5,
    )

    await monitor.probe_all_peers()
    assert peer_table.get(peer_id).health == PeerHealth.SUSPECT

    await monitor.probe_all_peers()
    await asyncio.sleep(0)  # Let emit_nowait task complete
    assert peer_table.get(peer_id).health == PeerHealth.DEAD
    assert len(lost_events) == 1
    assert lost_events[0].data["node_id"] == peer_id

    # Now peer responds with valid pong -> marks alive again
    respond_with_error = False
    await monitor.probe_all_peers()
    assert peer_table.get(peer_id).health == PeerHealth.HEALTHY
    await events.cancel_pending()


@pytest.mark.asyncio
async def test_recovery_manager_stop_and_skips_failed_tasks():
    events = EventBus()
    dead_peer = "d" * 40

    failed_task = Task(name="t.failed", assigned_node_id=dead_peer)
    failed_task.transition_to(TaskState.FAILED, reason="already failed")

    completed_task = Task(name="t.done", assigned_node_id=dead_peer)
    completed_task.transition_to(TaskState.ASSIGNED, reason="assigned")
    completed_task.transition_to(TaskState.COMPLETED, reason="already done")

    active_task = Task(name="t.active", assigned_node_id=dead_peer)
    active_task.transition_to(TaskState.ASSIGNED, reason="assigned")
    active_task.transition_to(TaskState.RUNNING, reason="in-flight")

    tasks_map = {
        failed_task.task_id: failed_task,
        completed_task.task_id: completed_task,
        active_task.task_id: active_task,
    }

    rescheduled: list[str] = []
    gate = asyncio.Event()

    async def slow_reschedule(task: Task) -> TaskResult:
        rescheduled.append(task.task_id)
        await gate.wait()
        return TaskResult(task_id=task.task_id, state=TaskState.COMPLETED, result="ok")

    rm = RecoveryManager(
        node_id="0" * 40,
        tasks_map=tasks_map,
        events=events,
        reschedule_task_fn=slow_reschedule,
        retry_policy=RetryPolicy(max_retries=2, initial_delay=0.01),
    )

    await events.emit(EVENT_HEARTBEAT_LOST, {"node_id": dead_peer})
    await asyncio.sleep(0.05)

    # Only active_task should be recovered; FAILED and COMPLETED must be skipped
    assert rescheduled == [active_task.task_id]
    assert failed_task.state == TaskState.FAILED
    assert completed_task.state == TaskState.COMPLETED

    # stop() cancels active recovery task and unsubscribes
    await rm.stop()
    assert len(rm._recovery_tasks) == 0
    assert len(rm._recovering_task_ids) == 0

    # Subsequent heartbeat lost events do not trigger recovery after stop()
    another_task = Task(name="t.after_stop", assigned_node_id=dead_peer)
    another_task.transition_to(TaskState.ASSIGNED)
    tasks_map[another_task.task_id] = another_task
    await events.emit(EVENT_HEARTBEAT_LOST, {"node_id": dead_peer})
    await asyncio.sleep(0.05)
    assert rescheduled == [active_task.task_id]
    await events.cancel_pending()


def test_dedup_registry_clear_started_and_ttl_cleanup(monkeypatch):
    fake_now = [1000.0]
    monkeypatch.setattr("meshweaver.reliability.retry.utc_now", lambda: fake_now[0])

    dedup = DeduplicationRegistry(ttl_sec=10.0, max_entries=3)

    # record_started -> clear_started allows immediate retry
    assert dedup.record_started("t-fail") is True
    assert dedup.is_processed("t-fail") is True
    dedup.clear_started("t-fail")
    assert dedup.is_processed("t-fail") is False
    assert dedup.record_started("t-fail") is True

    # Completed result is NOT cleared by clear_started
    dedup.record_completed("t-fail", {"result": 123})
    dedup.clear_started("t-fail")
    assert dedup.get_result("t-fail") == {"result": 123}

    # TTL cleanup removes expired entries on get_result/is_processed
    fake_now[0] = 1011.0
    assert dedup.get_result("t-fail") is None
    assert dedup.is_processed("t-fail") is False

    # max_entries cap evicts oldest
    for i in range(5):
        fake_now[0] += 0.1
        dedup.record_completed(f"t-{i}", {"i": i})
    assert len(dedup._cache) == 3
    assert dedup.get_result("t-0") is None
    assert dedup.get_result("t-4") == {"i": 4}


@pytest.mark.asyncio
async def test_node_clears_dedup_started_when_task_fails():
    from meshweaver.core.config import NodeConfig
    from meshweaver.core.node import Node

    cfg = NodeConfig(name="dedup-clear-node")
    cfg.network.tcp_port = 0
    cfg.network.udp_port = 0
    node = Node(cfg)

    attempts = 0

    @node.registry.register("flaky.once")
    def flaky_once() -> int:
        nonlocal attempts
        attempts += 1
        if attempts == 1:
            raise RuntimeError("transient boom")
        return 99

    try:
        task = Task(name="flaky.once")
        msg1 = Message(sender_id="caller-1", msg_type="task_request", payload=task.to_dict())
        resp1 = await node._handle_task_request(msg1)
        assert resp1.msg_type == "task_response"
        assert resp1.payload["result"]["state"] == "FAILED"

        # Failure must clear in-flight dedup entry and NOT cache failed result, so retry executes
        msg2 = Message(sender_id="caller-1", msg_type="task_request", payload=task.to_dict())
        resp2 = await node._handle_task_request(msg2)
        assert resp2.msg_type == "task_response"
        assert resp2.payload["result"]["state"] == "COMPLETED"
        assert resp2.payload["result"]["result"] == 99
        assert attempts == 2
    finally:
        await node.stop()
