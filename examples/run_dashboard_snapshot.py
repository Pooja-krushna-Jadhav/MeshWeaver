"""Run a live snapshot of the MeshWeaver Terminal Dashboard."""

import asyncio
import os
import random
import sys
from pathlib import Path

# Ensure package root is in python path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from rich.console import Console
from meshweaver.core.config import NodeConfig
from meshweaver.core.node import Node
from meshweaver.core.state import NodeInfo, PeerHealth, ResourceMetrics
from meshweaver.execution.task import Task
from meshweaver.monitoring.dashboard import TerminalDashboard


async def main() -> None:
    console = Console(width=100, force_terminal=True, color_system="standard")

    # Start main node
    cfg = NodeConfig(name="edge-gateway-01")
    cfg.network.tcp_port = 0
    cfg.network.udp_port = 0
    node = Node(cfg)
    await node.start()

    # Start 2 real local worker nodes
    cfg_w1 = NodeConfig(name="rpi4-worker-a")
    cfg_w1.network.tcp_port = 0
    cfg_w1.network.udp_port = 0
    w1 = Node(cfg_w1)

    cfg_w2 = NodeConfig(name="jetson-nano-01")
    cfg_w2.network.tcp_port = 0
    cfg_w2.network.udp_port = 0
    w2 = Node(cfg_w2)

    await w1.start()
    await w2.start()

    try:
        # Register workers in main node's peer table
        m1 = ResourceMetrics(cpu_percent=24.0, memory_percent=48.0, active_tasks=1, max_tasks=8, timestamp=100.0)
        m2 = ResourceMetrics(cpu_percent=8.5, memory_percent=32.0, active_tasks=0, max_tasks=8, timestamp=100.0)
        node.peer_table.update_peer(w1.info, metrics=m1, rtt_ms=1.4)
        node.peer_table.update_peer(w2.info, metrics=m2, rtt_ms=2.1)

        # Execute tasks across the cluster
        for i in range(1, 4):
            t = Task(name="math.add", payload={"args": [i * 10, i * 25]})
            await node.submit_task(t)

        dashboard = TerminalDashboard(node)
        layout = dashboard.render_layout()
        console.print(layout)

    finally:
        await node.stop()
        await w1.stop()
        await w2.stop()


if __name__ == "__main__":
    asyncio.run(main())
