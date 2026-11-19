# MeshWeaver — How to Run Guide

Complete instructions for running MeshWeaver in both interactive demonstration mode and as a real multi-node distributed cluster.

---

## 📌 Prerequisites & Setup

Ensure you are in the project root directory (`MeshWeaver`):

```powershell
cd /path/to/MeshWeaver
```

Install the package in editable mode (if not already done):

```powershell
pip install -e .
```

To enable live rich terminal dashboard and cryptographic signatures:

```powershell
pip install -e ".[full]"
```

---

## 🚀 Option 1: Quick 1-Command Live Interactive Demo

If you want to watch the real-time dashboard with automated background tasks and simulated edge peers immediately in a single window:

```powershell
python examples/dashboard_demo.py
```

### What this does:
1. Spawns an edge controller node with the **Rich live terminal dashboard**.
2. Boots 2 local worker nodes (`rpi4-worker-01` and `jetson-edge-02`) connected via real TCP loopback sockets.
3. Continuously generates background compute tasks (`math.square`, `math.fibonacci`, `math.add`, `math.vector_norm`).
4. Updates CPU/RAM load meters and the Task Pipeline live in real time.
5. Press `Ctrl+C` to cleanly shut down all nodes.

---

## 🌐 Option 2: Running the Real Multi-Node Cluster

To experience real distributed task execution across multiple processes, open **2 or 3 separate PowerShell terminals**.

### Step 1: In Terminal 1 — Start the Main Node with Live Dashboard

```powershell
cd /path/to/MeshWeaver
python -m meshweaver start --name node-1 --port 8765 --dashboard
```

- This starts a real background P2P node daemon.
- Binds **TCP port `8765`** for task transfers and **UDP port `8766`** for discovery and gossip.
- Actively samples your computer's actual CPU and RAM utilization.
- The interactive terminal dashboard takes over Terminal 1.

---

### Step 2: In Terminal 2 — Connect a Second Worker Node

Open a **second PowerShell window**:

```powershell
cd /path/to/MeshWeaver
python -m meshweaver start --name node-2 --port 8767 --udp-port 8768 --bootstrap 127.0.0.1:8765
```

- `node-2` starts on port `8767` and contacts `node-1` via the bootstrap address `127.0.0.1:8765`.
- Both nodes discover each other using the **Kademlia DHT**.
- Every 5 seconds, they exchange CPU and RAM metrics over UDP gossip.

👉 **Look at Terminal 1:** `node-2` will automatically appear in the **Cluster Peers** table showing its live latency (RTT) and resource metrics!

---

### Step 3: In Terminal 3 — Submit Computational Tasks

Open a **third PowerShell window** (or use Terminal 2 if starting in background) to submit tasks to the cluster:

#### 1. Basic Addition:
```powershell
cd /path/to/MeshWeaver
python -m meshweaver submit math.add --args "[125, 375]" --target 127.0.0.1:8765
```

#### 2. Multiplication:
```powershell
python -m meshweaver submit math.multiply --args "[18, 25]" --target 127.0.0.1:8765
```

#### 3. Vector Euclidean Norm:
```powershell
python -m meshweaver submit math.vector_norm --args "[[3.0, 4.0]]" --target 127.0.0.1:8765
```

#### 4. Text Word Counter:
```powershell
python -m meshweaver submit text.word_count --args "[\"edge computing decentralized p2p broker\"]" --target 127.0.0.1:8765
```

👉 **Look at Terminal 1:**
- You will watch the task pop up in the **Task Pipeline** table.
- It is assigned to the least-loaded worker (`node-1` or `node-2`).
- The status changes from `RUNNING` $\rightarrow$ `COMPLETED`.
- The completed task counter in **Local Telemetry** increments.

---

## ⚡ Step 4: Test Fault Tolerance & Automatic Recovery Live

1. With both `node-1` (dashboard) and `node-2` running, submit a longer computation or continuous tasks.
2. In Terminal 2 (where `node-2` is running), press `Ctrl+C` to simulate an abrupt worker crash.
3. **Look at Terminal 1:**
   - Within seconds, `node-2`'s health badge transitions from `● HEALTHY` $\rightarrow$ `● SUSPECT` $\rightarrow$ `● DEAD`.
   - Any task that was assigned to `node-2` is automatically rescued by the `RecoveryManager`, marked `RETRYING`, and rescheduled to a surviving node without data loss!

---

## 🛠️ CLI Reference

### `meshweaver start`
Starts a MeshWeaver node daemon.

| Option | Default | Description |
|---|---|---|
| `--name` | `mesh-node` | Friendly name for this node |
| `--host` | `127.0.0.1` | Network interface to bind (`0.0.0.0` for all interfaces) |
| `--port` | `8765` | TCP port for task transfer (0 for dynamic port) |
| `--udp-port` | `8766` | UDP port for discovery and gossip |
| `--bootstrap` | None | Comma-separated list of seed peers (`host:port`) |
| `--secret` | None | Shared cluster security secret for HMAC signing |
| `--dashboard` | False | Launch the full-screen interactive Rich terminal UI |
| `--log-level` | `INFO` | Logging level (`DEBUG`, `INFO`, `WARNING`, `ERROR`) |

### `meshweaver submit`
Submits a task to a target node.

| Option | Default | Description |
|---|---|---|
| `task_name` | *(Required)* | Name of the task handler (e.g., `math.add`) |
| `--target` | `127.0.0.1:8765` | Target node `host:port` |
| `--args` | `[]` | JSON array of positional arguments (e.g., `'[10, 20]'`) |
| `--kwargs` | `{}` | JSON object of keyword arguments |
| `--timeout` | `60.0` | Maximum execution timeout in seconds |

---

## ❓ Troubleshooting

### Error: `[Errno 2] No such file or directory: '...dashboard_demo.py'`
You are likely inside the inner `meshweaver/` directory instead of the project root.
Run:
```powershell
cd ..
python examples/dashboard_demo.py
```

### Port Already in Use (`[WinError 10048]`)
If port 8765 is occupied by a previous run:
- Either specify a different port: `--port 8775 --udp-port 8776`
- Or pass `--port 0 --udp-port 0` to let the OS automatically select available ephemeral ports.

---

## 🧪 Running the Automated Test Suite

To verify all 54 unit, integration, distributed, security, and performance benchmark tests:

```powershell
python -m pytest tests/ -v
```
