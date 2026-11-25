"""Unit tests for PeerDiscoveryService, Kademlia deduplication, GossipProtocol, ResourceMonitor, and TaskScheduler."""

import asyncio
import pytest

from meshweaver.core.config import SchedulerConfig
from meshweaver.core.events import EventBus
from meshweaver.core.state import NodeInfo, PeerHealth, ResourceMetrics
from meshweaver.discovery.kademlia import KademliaNode
from meshweaver.discovery.peer_discovery import PeerDiscoveryService
from meshweaver.discovery.routing_table import Contact
from meshweaver.execution.task import Task
from meshweaver.gossip.gossip_protocol import GossipProtocol
from meshweaver.gossip.peer_state import PeerTable
from meshweaver.gossip.resource_monitor import ResourceMonitor
from meshweaver.networking.protocol import Message
from meshweaver.networking.tcp import AsyncTCPClient, AsyncTCPServer
from meshweaver.scheduler.scheduler import TaskScheduler
from meshweaver.utils.helpers import generate_node_id


@pytest.mark.asyncio
async def test_peer_discovery_bootstrap_retry_when_seed_starts_late(monkeypatch):
    seed_id = generate_node_id("seed-node")
    joiner_id = generate_node_id("joiner-node")

    # Reserve a free port by starting and stopping a temporary server, or bind after 1st failure
    client = AsyncTCPClient(pool_connections=False, connect_timeout=0.3)
    seed_kad = KademliaNode(
        local_node_id=seed_id,
        host="127.0.0.1",
        tcp_port=0,
        udp_port=0,
        send_rpc_fn=lambda h, p, m, t: asyncio.sleep(0),
    )

    async def seed_handler(msg: Message, writer: asyncio.StreamWriter) -> Message | None:
        if msg.msg_type == "kad_rpc":
            return await seed_kad.handle_rpc(msg)
        return None

    seed_server = AsyncTCPServer("127.0.0.1", 0, handler=seed_handler)
    await seed_server.start()
    seed_port = seed_server.actual_port
    seed_kad.tcp_port = seed_port
    # Stop seed server initially so joiner's first bootstrap attempt fails
    await seed_server.stop()

    async def send_rpc(host: str, port: int, msg: Message, timeout: float = 1.0) -> Message | None:
        try:
            return await client.send_and_receive(host, port, msg, timeout=min(timeout, 0.3))
        except Exception:
            return None

    joiner_kad = KademliaNode(
        local_node_id=joiner_id,
        host="127.0.0.1",
        tcp_port=9000,
        udp_port=9001,
        send_rpc_fn=send_rpc,
    )
    discovery = PeerDiscoveryService(
        node_id=joiner_id,
        host="127.0.0.1",
        tcp_port=9000,
        udp_port=9001,
        kademlia=joiner_kad,
        bootstrap_peers=[f"127.0.0.1:{seed_port}"],
        enable_lan_broadcast=False,
        refresh_interval=60.0,
    )

    # Speed up retry backoff sleep in peer_discovery while keeping real Kademlia TCP RPCs
    orig_bootstrap = joiner_kad.bootstrap
    attempts = 0
    first_failed = asyncio.Event()

    async def tracked_bootstrap(contacts: list[Contact]) -> int:
        nonlocal attempts
        attempts += 1
        res = await orig_bootstrap(contacts)
        if res == 0:
            first_failed.set()
        return res

    monkeypatch.setattr(joiner_kad, "bootstrap", tracked_bootstrap)
    real_sleep = asyncio.sleep

    async def fast_sleep(delay: float) -> None:
        await real_sleep(min(delay, 0.05))

    monkeypatch.setattr("meshweaver.discovery.peer_discovery.asyncio.sleep", fast_sleep)

    try:
        await discovery.start()
        await asyncio.wait_for(first_failed.wait(), timeout=5.0)

        # Now bring the seed server online on the same port
        seed_server_late = AsyncTCPServer("127.0.0.1", seed_port, handler=seed_handler)
        await seed_server_late.start()
        try:
            for _ in range(40):
                if joiner_kad.routing_table.get_contact(seed_id) is not None:
                    break
                await real_sleep(0.05)

            assert attempts >= 2
            assert joiner_kad.routing_table.get_contact(seed_id) is not None
        finally:
            await seed_server_late.stop()
    finally:
        await discovery.stop()
        await client.close()


@pytest.mark.asyncio
async def test_kademlia_iterative_find_node_deduplication():
    local_id = generate_node_id("local")
    peer_a = Contact(generate_node_id("peer-a"), "127.0.0.1", 9101, 9102)
    peer_b = Contact(generate_node_id("peer-b"), "127.0.0.1", 9103, 9104)
    shared_c = Contact(generate_node_id("shared-c"), "127.0.0.1", 9105, 9106)
    target_id = generate_node_id("target")

    rpc_calls: list[str] = []

    async def fake_rpc(host: str, port: int, msg: Message, timeout: float) -> Message | None:
        rpc_calls.append(msg.receiver_id or "")
        # Both peer_a and peer_b return overlapping contacts (shared_c + peer_a + peer_b)
        return Message(
            sender_id=msg.receiver_id or "",
            receiver_id=local_id,
            msg_type="kad_rpc",
            payload={
                "rpc": "FIND_NODE_RESP",
                "contacts": [peer_a.to_dict(), peer_b.to_dict(), shared_c.to_dict()],
            },
        )

    kad = KademliaNode(local_id, "127.0.0.1", 9100, 9101, send_rpc_fn=fake_rpc, alpha=3)
    kad.routing_table.add_contact(peer_a)
    kad.routing_table.add_contact(peer_b)

    closest = await kad.iterative_find_node(target_id)
    ids = [c.node_id for c in closest]
    # No duplicate contacts in result, and each discovered peer queried at most once
    assert len(ids) == len(set(ids))
    assert set(ids) == {peer_a.node_id, peer_b.node_id, shared_c.node_id}
    assert len(rpc_calls) == len(set(rpc_calls))


@pytest.mark.asyncio
async def test_gossip_reports_active_tasks_and_enforces_mtu_cap():
    local_id = generate_node_id("gossip-local")
    local_info = NodeInfo(node_id=local_id, name="gossip-local", host="127.0.0.1", tcp_port=8000, udp_port=8001)
    peer_table = PeerTable()
    rm = ResourceMonitor(max_tasks=8)
    events = EventBus()
    kad = KademliaNode(local_id, "127.0.0.1", 8000, 8001, send_rpc_fn=lambda h, p, m, t: asyncio.sleep(0))

    # Populate many peers with long names to exceed 1200B if packed together
    for i in range(12):
        pid = generate_node_id(f"peer-{i}")
        pinfo = NodeInfo(
            node_id=pid,
            name=f"very-long-edge-worker-hostname-number-{i}-with-extra-padding",
            host="127.0.0.1",
            tcp_port=8100 + i,
            udp_port=8200 + i,
        )
        peer_table.update_peer(pinfo, ResourceMetrics(cpu_percent=10.0, memory_percent=20.0))

    sent_messages: list[Message] = []

    def capture_udp(msg: Message, addr: tuple[str, int]) -> None:
        sent_messages.append(msg)

    gossip = GossipProtocol(
        node_id=local_id,
        node_info=local_info,
        peer_table=peer_table,
        resource_monitor=rm,
        kademlia=kad,
        events=events,
        send_udp_fn=capture_udp,
        fanout=3,
        active_tasks_fn=lambda: 5,
    )

    await gossip.do_gossip_round()

    assert len(sent_messages) == 3
    for msg in sent_messages:
        assert len(msg.to_frame()) <= 1200
        self_entry = next(p for p in msg.payload["peers"] if p["info"]["node_id"] == local_id)
        assert self_entry["metrics"]["active_tasks"] == 5

    assert peer_table.get(local_id).metrics.active_tasks == 5
    await events.cancel_pending()


def test_peer_table_tombstone_blocks_zombie_resurrection(monkeypatch):
    fake_now = [1000.0]
    monkeypatch.setattr("meshweaver.gossip.peer_state.utc_now", lambda: fake_now[0])
    monkeypatch.setattr("meshweaver.core.state.utc_now", lambda: fake_now[0])

    pt = PeerTable()
    zombie_id = generate_node_id("zombie")
    zombie_info = NodeInfo(node_id=zombie_id, name="zombie", host="127.0.0.1", tcp_port=8800, udp_port=8801)

    pt.update_peer(zombie_info, metrics=ResourceMetrics(cpu_percent=10.0, timestamp=1000.0), direct_contact=True)
    assert pt.get(zombie_id) is not None

    # Advance past purge_threshold -> peer purged & tombstone recorded at t=1100.0
    fake_now[0] = 1100.0
    _, _, purged = pt.check_stale(stale_threshold=15.0, dead_threshold=30.0, purge_threshold=90.0)
    assert zombie_id in purged
    assert pt.get(zombie_id) is None

    # Indirect gossip carrying stale timestamp <= tombstone_ts must be dropped
    res = pt.update_peer(
        zombie_info,
        metrics=ResourceMetrics(cpu_percent=12.0, timestamp=1050.0),
        direct_contact=False,
    )
    assert res is None
    assert pt.get(zombie_id) is None

    # Direct contact clears tombstone and restores peer
    restored = pt.update_peer(
        zombie_info,
        metrics=ResourceMetrics(cpu_percent=15.0, timestamp=1105.0),
        direct_contact=True,
    )
    assert restored is not None
    assert restored.health == PeerHealth.HEALTHY


def test_resource_monitor_rapid_sample_caching(monkeypatch):
    calls = 0
    rm = ResourceMonitor(max_tasks=8, min_interval=5.0)
    orig_cpu = rm._get_cpu_percent

    def counted_cpu(now: float) -> float:
        nonlocal calls
        calls += 1
        return orig_cpu(now)

    monkeypatch.setattr(rm, "_get_cpu_percent", counted_cpu)

    m1 = rm.sample(active_tasks=1)
    m2 = rm.sample(active_tasks=4)

    assert calls == 1
    assert m1.cpu_percent == m2.cpu_percent
    assert m1.memory_total_mb == m2.memory_total_mb
    assert m1.active_tasks == 1
    assert m2.active_tasks == 4


@pytest.mark.asyncio
async def test_scheduler_concurrent_burst_reservation():
    local_id = generate_node_id("sched-local")
    local_info = NodeInfo(node_id=local_id, name="sched-local", host="127.0.0.1", tcp_port=8000, udp_port=8001)
    peer_table = PeerTable()
    rm = ResourceMonitor(max_tasks=8)
    rm.sample = lambda active_tasks=0: ResourceMetrics(
        cpu_percent=95.0, memory_percent=95.0, active_tasks=active_tasks, max_tasks=8
    )
    events = EventBus()

    p1_id = generate_node_id("worker-1")
    p2_id = generate_node_id("worker-2")
    peer_table.update_peer(
        NodeInfo(node_id=p1_id, name="w1", host="127.0.0.1", tcp_port=8010, udp_port=8011),
        metrics=ResourceMetrics(cpu_percent=10.0, memory_percent=10.0, active_tasks=0, max_tasks=8),
    )
    peer_table.update_peer(
        NodeInfo(node_id=p2_id, name="w2", host="127.0.0.1", tcp_port=8020, udp_port=8021),
        metrics=ResourceMetrics(cpu_percent=10.0, memory_percent=10.0, active_tasks=0, max_tasks=8),
    )

    scheduler = TaskScheduler(
        node_id=local_id,
        node_info=local_info,
        peer_table=peer_table,
        resource_monitor=rm,
        events=events,
        config=SchedulerConfig(strategy="least_loaded"),
    )

    # Burst selection without gossip updates between calls
    sel1, is_local1 = scheduler.select_peer_for_task(Task(name="math.add"))
    assert sel1 is not None and not is_local1
    scheduler.reserve_peer(sel1.info.node_id)

    sel2, is_local2 = scheduler.select_peer_for_task(Task(name="math.add"))
    assert sel2 is not None and not is_local2
    assert sel2.info.node_id != sel1.info.node_id

    # Shared peer_table active_tasks must remain untouched by optimistic reservation
    assert peer_table.get(sel1.info.node_id).metrics.active_tasks == 0

    scheduler.release_peer(sel1.info.node_id)
    assert scheduler._in_flight_reservations == {}
    await events.cancel_pending()
