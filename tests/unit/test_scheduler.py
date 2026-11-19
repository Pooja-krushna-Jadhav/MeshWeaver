"""Unit tests for scheduler load balancing and peer selection policies."""

from meshweaver.core.state import NodeInfo, PeerHealth, PeerState, ResourceMetrics
from meshweaver.execution.task import Task
from meshweaver.scheduler.load_balancer import LeastLoadedStrategy, MultiMetricStrategy, RoundRobinStrategy


def make_peer(node_id: str, cpu: float, ram: float, active_tasks: int = 0, max_tasks: int = 8) -> PeerState:
    info = NodeInfo(node_id=node_id, host="127.0.0.1", tcp_port=8000, udp_port=8001)
    metrics = ResourceMetrics(
        cpu_percent=cpu,
        memory_percent=ram,
        memory_total_mb=1000.0,
        memory_available_mb=1000.0 * (1.0 - ram / 100.0),
        active_tasks=active_tasks,
        max_tasks=max_tasks,
    )
    return PeerState(info=info, metrics=metrics, health=PeerHealth.HEALTHY)


def test_least_loaded_strategy():
    strategy = LeastLoadedStrategy()
    p1 = make_peer("p1", cpu=85.0, ram=50.0)
    p2 = make_peer("p2", cpu=15.0, ram=60.0)  # lowest CPU
    p3 = make_peer("p3", cpu=45.0, ram=20.0)

    task = Task(name="test")
    selected = strategy.select_peer(task, [p1, p2, p3])
    assert selected is not None
    assert selected.info.node_id == "p2"


def test_least_loaded_skips_overloaded():
    strategy = LeastLoadedStrategy()
    # p1 is overloaded (cpu 95%)
    p1 = make_peer("p1", cpu=95.0, ram=50.0)
    p2 = make_peer("p2", cpu=70.0, ram=50.0)

    task = Task(name="test")
    selected = strategy.select_peer(task, [p1, p2])
    assert selected is not None
    assert selected.info.node_id == "p2"


def test_multi_metric_strategy():
    strategy = MultiMetricStrategy(cpu_weight=0.5, ram_weight=0.3, queue_weight=0.2)
    # p1: low cpu (20%), high ram (80%), 0 tasks
    # score = 0.5*0.2 + 0.3*0.8 + 0.2*0 = 0.1 + 0.24 = 0.34
    p1 = make_peer("p1", cpu=20.0, ram=80.0, active_tasks=0)

    # p2: moderate cpu (30%), low ram (20%), 0 tasks
    # score = 0.5*0.3 + 0.3*0.2 + 0.2*0 = 0.15 + 0.06 = 0.21 (better)
    p2 = make_peer("p2", cpu=30.0, ram=20.0, active_tasks=0)

    task = Task(name="test")
    selected = strategy.select_peer(task, [p1, p2])
    assert selected is not None
    assert selected.info.node_id == "p2"


def test_round_robin_strategy():
    strategy = RoundRobinStrategy()
    p1 = make_peer("p1", cpu=50.0, ram=50.0)
    p2 = make_peer("p2", cpu=50.0, ram=50.0)

    task = Task(name="test")
    s1 = strategy.select_peer(task, [p1, p2])
    s2 = strategy.select_peer(task, [p1, p2])
    s3 = strategy.select_peer(task, [p1, p2])

    assert s1.info.node_id == "p1"
    assert s2.info.node_id == "p2"
    assert s3.info.node_id == "p1"
