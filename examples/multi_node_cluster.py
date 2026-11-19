"""Demonstration script spinning up a 3-node MeshWeaver cluster on localhost."""

import asyncio
import logging
import os
import sys
from pathlib import Path

# Ensure package root is in python path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from meshweaver.core.config import NodeConfig
from meshweaver.core.node import Node
from meshweaver.discovery.routing_table import Contact
from meshweaver.execution.task import Task


async def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s: %(message)s")
    print("=" * 65)
    print("       MESHWEAVER: 3-NODE CLUSTER DEMONSTRATION")
    print("=" * 65)

    # 1. Initialize 3 nodes with ephemeral ports
    cfg1 = NodeConfig(name="controller-1")
    cfg1.network.tcp_port = 0
    cfg1.network.udp_port = 0
    node1 = Node(cfg1)

    cfg2 = NodeConfig(name="worker-alpha")
    cfg2.network.tcp_port = 0
    cfg2.network.udp_port = 0
    node2 = Node(cfg2)

    cfg3 = NodeConfig(name="worker-beta")
    cfg3.network.tcp_port = 0
    cfg3.network.udp_port = 0
    node3 = Node(cfg3)

    # Register edge tasks on nodes
    @node1.registry.register("math.fibonacci")
    @node2.registry.register("math.fibonacci")
    @node3.registry.register("math.fibonacci")
    def fib(n: int) -> int:
        a, b = 0, 1
        for _ in range(n):
            a, b = b, a + b
        return a

    # Start all nodes
    await node1.start()
    await node2.start()
    await node3.start()

    print(f"\n[+] Controller Node: {node1.node_id[:8]} on TCP {node1.info.tcp_port}")
    print(f"[+] Worker Alpha:    {node2.node_id[:8]} on TCP {node2.info.tcp_port}")
    print(f"[+] Worker Beta:     {node3.node_id[:8]} on TCP {node3.info.tcp_port}")

    try:
        # 2. Peer interconnect & discovery
        c2 = Contact(node2.node_id, "127.0.0.1", node2.tcp_server.actual_port, node2.udp_endpoint.actual_port)
        c3 = Contact(node3.node_id, "127.0.0.1", node3.tcp_server.actual_port, node3.udp_endpoint.actual_port)

        node1.kademlia.routing_table.add_contact(c2)
        node1.kademlia.routing_table.add_contact(c3)

        print("\n[*] Exchanging resource gossip across cluster...")
        await node1.gossip.do_gossip_round()
        await node2.gossip.do_gossip_round()
        await node3.gossip.do_gossip_round()
        await asyncio.sleep(0.5)

        print(f"[✓] Controller discovered {len(node1.peer_table.all_peers())} peers.")

        # 3. Submit computational tasks from Controller
        print("\n[*] Submitting tasks from Controller to the Mesh...")
        for i in range(1, 6):
            n_val = i * 5
            task = Task(name="math.fibonacci", payload={"args": [n_val]})
            res = await node1.submit_task(task)
            print(f"    Task #{i}: fib({n_val:02d}) = {res.result:<8} (Executed by: {res.worker_node_id[:8]} in {res.execution_time_sec*1000:.2f}ms)")

        print("\n[✓] Cluster workload demonstration completed successfully!")

    finally:
        print("\n[*] Shutting down nodes cleanly...")
        await node1.stop()
        await node2.stop()
        await node3.stop()
        print("[✓] All nodes stopped.")


if __name__ == "__main__":
    asyncio.run(main())
