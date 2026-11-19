# MeshWeaver Subsystem Architecture

## 1. Architectural Philosophy

MeshWeaver is built to eliminate single points of failure (SPOFs) in distributed task computation at the network edge. In edge deployments (e.g. dozens of micro-computers or IoT gateways in remote facilities), centralized message brokers such as RabbitMQ or Redis clusters represent vulnerable bottlenecks requiring significant system administration and cloud connectivity.

MeshWeaver implements a **symmetric decentralized topology**: every node is an autonomous peer capable of both initiating tasks, gossiping resource availability, discovering neighbors, and executing compute workloads.

---

## 2. Component Structure

The system is organized into modular layers adhering to clean architectural boundaries:

```text
meshweaver/
├── core/
│   ├── config.py             # Strongly typed configurations & environment overrides
│   ├── events.py             # Intra-node non-blocking async EventBus
│   ├── node.py               # Node lifecycle & coordination runtime
│   └── state.py              # Domain state models (NodeInfo, PeerState, ResourceMetrics)
├── networking/
│   ├── framing.py            # 16-byte binary wire framing with CRC32 integrity checks
│   ├── protocol.py           # Canonical Message envelopes & JSON wire encoding
│   ├── tcp.py                # Asynchronous framed TCP server, client & connection pool
│   └── udp.py                # Asynchronous UDP datagram transport with SO_BROADCAST
├── discovery/
│   ├── kademlia.py           # Kademlia DHT RPC protocol & alpha-parallel lookups
│   ├── peer_discovery.py     # LAN broadcast beacon & bootstrap orchestrator
│   └── routing_table.py      # 160-bit XOR metric routing table & k-buckets
├── gossip/
│   ├── gossip_protocol.py    # Epidemic randomized gossip engine
│   ├── peer_state.py         # Thread-safe peer state table & stale peer pruning
│   └── resource_monitor.py   # Pure-Python zero-dependency CPU and RAM monitor (/proc & ctypes)
├── scheduler/
│   ├── load_balancer.py      # Pluggable scheduling strategies (LeastLoaded, MultiMetric, RoundRobin)
│   └── scheduler.py          # Intelligent peer selector & placement engine
├── execution/
│   ├── executor.py           # TaskExecutor with concurrency semaphore & thread offloading
│   ├── results.py            # TaskResult data models
│   ├── serialization.py      # Safe schema serialization & restricted cloudpickle gate
│   └── task.py               # Task lifecycle finite state machine
├── reliability/
│   ├── failure_detector.py   # Adaptive heartbeat arrival interval failure detector
│   ├── heartbeat.py          # Active peer heartbeat probing service
│   ├── recovery.py           # Automated task failover & re-routing coordinator
│   └── retry.py              # Exponential backoff retry policy & deduplication registry
├── security/
│   ├── authentication.py     # Ed25519 asymmetric PKI & HMAC-SHA256 signing
│   ├── certificates.py       # Self-signed TLS context generator
│   └── permissions.py        # Whitelist authorization & capability checks
├── storage/
│   └── state_store.py        # Durable SQLite storage for tasks & node identity
└── monitoring/
    ├── dashboard.py          # Live interactive Rich terminal dashboard (Operate mode)
    └── metrics.py            # Circular event logs & throughput metrics collector
```

---

## 3. Subsystem Deep Dive

### 3.1 Kademlia DHT vs. Dynamic Gossip

A fundamental design decision in MeshWeaver is the **strict separation between DHT routing and resource gossip**:

- **Kademlia DHT**: Designed for structured $O(\log N)$ routing and semi-static contact discovery across 160-bit node IDs. Storing transient metrics (such as CPU% updated every second) directly in a DHT produces high routing churn and storage thrashing.
- **Epidemic Gossip Protocol**: Runs periodically every 5 seconds. Each node samples its local CPU and RAM, bundles it with a subset of known peers, and sends it to $\beta=3$ random neighbors. Within $O(\log N)$ gossip cycles, network-wide load vectors converge with minimal network overhead.

### 3.2 Scheduling Algorithms

The scheduler supports modular strategies:

1. **LeastLoadedStrategy**:
   Finds all candidates where $Health \neq DEAD$ and $Capacity > 0$, sorting primarily by the lowest reported CPU% and secondarily by free execution slots.
2. **MultiMetricStrategy**:
   Calculates a normalized composite cost score:
   $$\text{Score} = w_{cpu} \left(\frac{CPU\%}{100}\right) + w_{ram} \left(\frac{RAM\%}{100}\right) + w_{queue} \left(\frac{\text{ActiveTasks}}{\text{MaxTasks}}\right) + w_{latency} \left(\frac{\text{RTT}}{1000}\right)$$
   The node with the lowest score is selected.
3. **RoundRobinStrategy**:
   Distributes tasks evenly in cyclic order across available worker nodes.

### 3.3 Reliability & Failure Detection

- Heartbeat probes run actively every 2 seconds.
- The `FailureDetector` maintains a rolling window of inter-arrival delays.
- A peer transitions through three distinct states:
  - `HEALTHY`: Received heartbeat within `suspect_timeout` (4.0s).
  - `SUSPECT`: Missing heartbeat between 4.0s and 8.0s. Node is deprioritized from new task assignments.
  - `DEAD`: Unresponsive for over 8.0s (`dead_timeout`).
- When a node is confirmed `DEAD`, `RecoveryManager` immediately finds all tasks assigned to that node in `ASSIGNED` or `RUNNING` state, transitions them to `RETRYING`, and resubmits them through the scheduler to healthy surviving nodes.
