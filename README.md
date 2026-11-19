# ⬡ MeshWeaver

**Zero-Dependency Peer-to-Peer Asynchronous Task Broker for Edge Computing**

[![Python Version](https://img.shields.io/badge/python-3.10%20%7C%203.11%20%7C%203.12%20%7C%203.13-blue.svg)](https://python.org)
[![License: MIT](https://img.shields.io/badge/License-MIT-green.svg)](LICENSE)
[![Tests](https://img.shields.io/badge/tests-54%20passed-brightgreen.svg)]()
[![Zero External Dependencies](https://img.shields.io/badge/dependencies-0%20mandatory-success.svg)]()

MeshWeaver is a decentralized, peer-to-peer (P2P) asynchronous task broker engineered for resource-constrained edge computing environments (such as clusters of Raspberry Pis, industrial edge gateways, and IoT nodes).

Unlike conventional distributed task queues (like Celery) that rely on heavy centralized infrastructure (Redis, RabbitMQ, central coordinators), MeshWeaver forms a self-organizing mesh network where nodes autonomously discover one another, gossip transient resource metrics, intelligently schedule compute jobs to the least-loaded peer, and automatically recover from peer crashes.

---

## 🌟 Key Highlights

- **100% Pure Python Standard Library**: Runs with zero mandatory third-party packages using native `asyncio`, `socket`, `struct`, `sqlite3`, `hashlib`, `hmac`, `secrets`, and OS-level `/proc` & `ctypes` resource readers.
- **Progressive Acceleration**: Seamlessly activates `rich` (for live terminal monitoring), `cryptography` (for Ed25519 PKI signatures), and `cloudpickle` (for authorized function serialization) when installed.
- **Kademlia DHT Peer Discovery**: 160-bit SHA-1 keyspace, XOR distance metric, $k$-buckets ($k=20$), and $\alpha$-concurrent iterative lookups.
- **Zero-Config LAN Auto-Discovery**: Automatic local subnet UDP broadcast beacon for instant edge discovery without manual IP entry.
- **Epidemic Resource Gossip**: Lightweight periodic load gossip (CPU%, RAM%, queue depth) decoupled from DHT routing to avoid churn.
- **Intelligent Multi-Metric Scheduler**: Pluggable scheduling policies (`LeastLoaded`, `MultiMetricScore`, `RoundRobin`) routing tasks to the optimal node.
- **Crash Durability & Fault Tolerance**: Heartbeat failure detector, automatic task reassignment on peer death, and SQLite state persistence across reboots.
- **Idempotency & Replay Defense**: Deduplication cache preventing duplicate task execution; timestamp window + cryptographically verified nonce tracking.
- **Hardened Remote Execution**: Defense-in-depth architecture rejecting arbitrary bytecode deserialization in favor of strictly typed registered handlers.
- **Terminal Dashboard (Operate Mode)**: Real-time interactive TUI built with Rich displaying cluster topology, live resource meters, task pipelines, and event feeds.

---

## 🏗️ System Architecture

```text
                               +----------------------------------------+
                               |           MeshWeaver Node              |
                               |  Node ID (160-bit SHA-1), IP, Port     |
                               +-------------------+--------------------+
                                                   |
         +--------------------+--------------------+--------------------+--------------------+
         |                    |                    |                    |                    |
         v                    v                    v                    v                    v
+------------------+ +------------------+ +------------------+ +------------------+ +------------------+
|   Networking     | |  Peer Discovery  | | Resource Gossip  | | Task Scheduler   | | Task Execution   |
| - TCP Transport  | | - Kademlia DHT   | | - CPU/RAM Engine | | - Load Balancer  | | - State Machine  |
| - UDP Datagram   | | - XOR Metric     | | - Epistemic State| | - Scoring Policy | | - Sandbox Runner |
| - Binary Framing | | - k-Buckets      | | - Periodic Dissem| | - Capacity Guard | | - Task Registry  |
| - Keepalive Pool | | - LAN Broadcast  | | - Stale Pruning  | | - Re-routing     | | - Result Channel |
+------------------+ +------------------+ +------------------+ +------------------+ +------------------+
         |                    |                    |                    |                    |
         +--------------------+--------------------+--------------------+--------------------+
                                                   |
         +--------------------+--------------------+--------------------+
         |                                         |                    |
         v                                         v                    v
+------------------+                     +------------------+ +------------------+
| Reliability      |                     | Security         | | Storage & Mon.   |
| - Heartbeat Ping |                     | - Ed25519 / HMAC | | - SQLite Store   |
| - Failure Detect |                     | - Nonce Replay   | | - Rich Dashboard |
| - Deduplication  |                     | - TLS Context    | | - Metrics Engine |
| - Retry Policy   |                     | - RCE Sandbox    | | - JSON Logging   |
+------------------+                     +------------------+ +------------------+
```

---

## 🚀 Quick Start

### 1. Installation

Install MeshWeaver from source:

```bash
git clone https://github.com/Infotact-solution/MeshWeaver.git
cd MeshWeaver

# Zero-dependency install (pure standard library)
pip install -e .

# Or install with full TUI and cryptographic accelerators
pip install -e ".[full]"
```

### 2. Start a Node

Start a MeshWeaver node using the CLI:

```bash
# Start worker node on port 8765
meshweaver start --name worker-1 --port 8765

# Start node with live interactive terminal dashboard
meshweaver start --name worker-1 --port 8765 --dashboard

# Start node with cluster security secret
meshweaver start --name worker-1 --port 8765 --secret my_cluster_secret_key
```

### 3. Join a Cluster

Start a second node pointing to the first node as bootstrap peer:

```bash
meshweaver start --name worker-2 --port 8767 --udp-port 8768 --bootstrap 127.0.0.1:8765
```

### 4. Submit Tasks

Submit a distributed task from the command line:

```bash
meshweaver submit math.add --args "[100, 250]" --target 127.0.0.1:8765
```

Output:
```text
Submitting task 'math.add' to 127.0.0.1:8765...

[SUCCESS] Task execution completed!
Worker Node: a3f37006...
Duration:    0.002s
Result:      350
```

---

## 💻 Python API Usage

```python
import asyncio
from meshweaver import Node, NodeConfig
from meshweaver.execution.task import Task

async def main():
    # Configure node
    cfg = NodeConfig(name="edge-node-1")
    node = Node(cfg)

    # Register custom task handler
    @node.registry.register("image.process")
    def process_image(width: int, height: int) -> dict:
        return {"pixels": width * height, "status": "processed"}

    await node.start()

    # Submit task (automatically scheduled to optimal node)
    task = Task(name="image.process", payload={"args": [1920, 1080]})
    result = await node.submit_task(task)

    print(f"Task executed by {result.worker_node_id}: {result.result}")
    await node.stop()

if __name__ == "__main__":
    asyncio.run(main())
```

---

## 📊 Live Terminal Dashboard

Running `meshweaver start --dashboard` launches an interactive terminal dashboard built with Rich, designed in accordance with **Impeccable Operate Mode** standards:

```text
╭─────────────────────────────────────────────────────────────────────────────────────────────╮
│ ⬡ MESHWEAVER | Edge Task Broker      Node: 7ff201d1… [controller-1] ● ONLINE    TCP: 8765   │
╰─────────────────────────────────────────────────────────────────────────────────────────────╯
╭─ Cluster Peers (2 discovered) ──────────╮╭─ Local Telemetry ────────────────────────────────╮
│ Peer ID    Endpoint       Health   CPU  ││ CPU Load: [██████░░░░░░] 52.0%                   │
│ a3f37006…  127.0.0.1:8767 ● HEALTHY 12% ││ RAM Load: [████████░░░░] 68.0% (4120MB free)    │
│ 4f4f3a21…  127.0.0.1:8769 ● HEALTHY 34% ││ Capacity: [██░░░░░░░░░░] 2/8 slots               │
╰─────────────────────────────────────────╯│ Completed: 124  Failed: 0  Retries: 1            │
╭─ Task Pipeline (Recent Activity) ───────╮╰──────────────────────────────────────────────────╯
│ Task ID    Handler      State     Dur   │╭─ Live Event Stream ──────────────────────────────╮
│ e8143b0a…  math.add     COMPLETED 0.00s ││ 01:15:43 Peer connected: a3f37006…               │
│ cadd5433…  image.filter COMPLETED 0.04s ││ 01:15:44 Task e8143b0a completed                │
╰─────────────────────────────────────────╯╰──────────────────────────────────────────────────╯
```

---

## 🧪 Comprehensive Verification & Test Suite

MeshWeaver includes 54 automated tests across 5 test suites:

```bash
# Run all tests
python -m pytest tests/ -v
```

```text
tests/unit/                     # 23 tests (Framing, Protocol, Kademlia, Table, Config, Monitor, Store, Security)
tests/integration/              # 16 tests (Two-node TCP, Remote Task Dispatch, Gossip, Scheduling, Failover)
tests/distributed/              # 5-node concurrent cluster simulation
tests/security/                 # Oversized frame rejection, signature tampering, replay attacks, RCE boundary
tests/performance/              # Wire throughput (2.6M frames/sec) & end-to-end task latency (1.25ms avg)
```

---

## 📚 Documentation

- [Step-by-Step Running Guide](INSTRUCTIONS.md)
- [Subsystem Architecture](docs/ARCHITECTURE.md)
- [Networking & Binary Protocol](docs/NETWORKING_PROTOCOL.md)
- [Security & Threat Model](docs/SECURITY.md)

---

## 📄 License

MIT License. Designed and engineered for high-reliability edge computing.
