"""Interactive Live Terminal Dashboard demonstration with local worker nodes."""

import asyncio
import random
import sys
from pathlib import Path

# Ensure package root is in python path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from meshweaver.core.config import NodeConfig
from meshweaver.core.node import Node
from meshweaver.core.state import ResourceMetrics
from meshweaver.execution.task import Task
from meshweaver.monitoring.dashboard import TerminalDashboard


async def main() -> None:
    # 1. Initialize main dashboard node
    cfg_main = NodeConfig(name="edge-controller")
    cfg_main.network.tcp_port = 8765
    cfg_main.network.udp_port = 8766
    node_main = Node(cfg_main)
    await node_main.start()

    # 2. Start 2 local worker nodes to demonstrate genuine distributed execution
    cfg_w1 = NodeConfig(name="rpi4-worker-01")
    cfg_w1.network.tcp_port = 8767
    cfg_w1.network.udp_port = 8768
    worker1 = Node(cfg_w1)

    cfg_w2 = NodeConfig(name="jetson-edge-02")
    cfg_w2.network.tcp_port = 8769
    cfg_w2.network.udp_port = 8770
    worker2 = Node(cfg_w2)

    await worker1.start()
    await worker2.start()

    # Register worker tasks
    for w in (node_main, worker1, worker2):
        @w.registry.register("math.square")
        def square(x: int) -> int:
            return x * x

        @w.registry.register("math.fibonacci")
        def fib(n: int) -> int:
            a, b = 0, 1
            for _ in range(n):
                a, b = b, a + b
            return a

    # Register in peer table with live metrics
    node_main.peer_table.update_peer(worker1.info, metrics=worker1.resource_monitor.sample(), rtt_ms=1.2)
    node_main.peer_table.update_peer(worker2.info, metrics=worker2.resource_monitor.sample(), rtt_ms=1.8)

    # 3. Start live Rich terminal dashboard
    dashboard = TerminalDashboard(node_main, refresh_interval=0.5)
    await dashboard.start()

    # 4. Continuous task workload generator
    async def workload_generator() -> None:
        task_names = ["math.square", "math.fibonacci", "math.add", "math.vector_norm"]
        counter = 1
        while True:
            await asyncio.sleep(random.uniform(1.2, 2.5))
            chosen = random.choice(task_names)
            if chosen == "math.square":
                task = Task(name="math.square", payload={"args": [random.randint(10, 99)]})
            elif chosen == "math.fibonacci":
                task = Task(name="math.fibonacci", payload={"args": [random.randint(15, 30)]})
            elif chosen == "math.add":
                task = Task(name="math.add", payload={"args": [random.randint(10, 50), random.randint(10, 50)]})
            else:
                task = Task(name="math.vector_norm", payload={"args": [[random.random() * 10, random.random() * 10]]})

            try:
                await node_main.submit_task(task)
            except Exception:
                pass
            counter += 1

    gen_task = asyncio.create_task(workload_generator())

    try:
        while True:
            await asyncio.sleep(0.5)
    except (KeyboardInterrupt, asyncio.CancelledError):
        pass
    finally:
        gen_task.cancel()
        await dashboard.stop()
        await node_main.stop()
        await worker1.stop()
        await worker2.stop()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
