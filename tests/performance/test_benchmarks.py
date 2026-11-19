"""Performance benchmark tests measuring latency, throughput, and framing overhead."""

import asyncio
import time
import pytest
from meshweaver.core.config import NodeConfig
from meshweaver.core.node import Node
from meshweaver.execution.task import Task
from meshweaver.networking.framing import Opcode, encode_frame, read_frame
from meshweaver.networking.protocol import Message


def test_benchmark_framing_throughput():
    """Benchmark framing encode/decode throughput in memory."""
    payload = b'{"action": "compute", "args": [1, 2, 3, 4, 5], "data": "edge_telemetry_payload"}'
    iterations = 5000

    start_t = time.perf_counter()
    for _ in range(iterations):
        encoded = encode_frame(Opcode.TASK_REQUEST, payload)
    encode_elapsed = time.perf_counter() - start_t

    ops_per_sec = iterations / encode_elapsed
    print(f"\n[BENCHMARK] Framing Encode: {ops_per_sec:.0f} frames/sec ({encode_elapsed*1000/iterations:.3f} ms/op)")
    assert ops_per_sec > 10000  # Must exceed 10k frames/sec


@pytest.mark.asyncio
async def test_benchmark_task_dispatch_latency():
    """Benchmark end-to-end task dispatch, TCP wire transit, execution, and result return latency."""
    cfg_a = NodeConfig(name="bench-submitter")
    cfg_a.network.tcp_port = 0
    cfg_a.network.udp_port = 0
    node_a = Node(cfg_a)

    cfg_b = NodeConfig(name="bench-worker")
    cfg_b.network.tcp_port = 0
    cfg_b.network.udp_port = 0
    node_b = Node(cfg_b)

    @node_b.registry.register("bench.add")
    def fast_add(a: int, b: int) -> int:
        return a + b

    await node_a.start()
    await node_b.start()

    try:
        # Warmup
        task = Task(name="bench.add", payload={"args": [1, 1]})
        await node_a.submit_task_to_peer("127.0.0.1", node_b.tcp_server.actual_port, task)

        # Timed iterations
        iterations = 50
        latencies: list[float] = []

        for i in range(iterations):
            t = Task(name="bench.add", payload={"args": [i, i]})
            t0 = time.perf_counter()
            res = await node_a.submit_task_to_peer("127.0.0.1", node_b.tcp_server.actual_port, t)
            dt = (time.perf_counter() - t0) * 1000.0  # ms
            latencies.append(dt)
            assert res.is_success
            assert res.result == i * 2

        avg_lat = sum(latencies) / len(latencies)
        min_lat = min(latencies)
        max_lat = max(latencies)
        p95_lat = sorted(latencies)[int(len(latencies) * 0.95)]

        print(
            f"\n[BENCHMARK] Remote Task Execution (50 runs): Avg: {avg_lat:.2f}ms | Min: {min_lat:.2f}ms | P95: {p95_lat:.2f}ms"
        )
        assert avg_lat < 100.0  # Typical local loopback is ~1-5ms
    finally:
        await node_a.stop()
        await node_b.stop()
