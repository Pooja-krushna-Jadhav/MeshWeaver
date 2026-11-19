# MeshWeaver Networking & Binary Wire Protocol

## 1. Binary Wire Framing

MeshWeaver implements a high-throughput binary framing protocol to encapsulate all TCP and UDP messages. Every frame begins with a fixed 16-byte big-endian header followed by the variable payload.

### Frame Layout (16 Bytes Header)

```text
 0                   1                   2                   3
 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1
+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
|                       Magic: "MSHW" (4B)                      |
+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
|  Version (1B) |  Opcode (1B)  |           Flags (2B)          |
+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
|                       Payload Length (4B)                     |
+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
|                      CRC32 Checksum (4B)                      |
+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
|                       Payload Data (Variable)                 |
+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
```

### Field Definitions

| Field | Size | Type | Description |
|---|---|---|---|
| **Magic** | 4 Bytes | Bytes | ASCII bytes `b"MSHW"` (0x4D534857). Packets without matching magic are immediately rejected. |
| **Version** | 1 Byte | `uint8` | Current protocol version (Value: `1`). |
| **Opcode** | 1 Byte | `uint8` | Message operation code (see table below). |
| **Flags** | 2 Bytes | `uint16` | Bitmask: Bit 0 = Raw Pickle, Bit 1 = Compressed (zlib), Bit 2 = Signed, Bit 3 = Encrypted. |
| **Length** | 4 Bytes | `uint32` | Length of payload in bytes (Max: 16 MB). |
| **CRC32** | 4 Bytes | `uint32` | CRC-32 checksum of payload for wire corruption detection. |

---

## 2. Operation Codes (Opcodes)

| Value | Name | Purpose | Transport |
|---|---|---|---|
| `1` | `PING` | Liveness ping requesting PONG | TCP / UDP |
| `2` | `PONG` | Liveness acknowledgment | TCP / UDP |
| `3` | `TASK_REQUEST` | Request remote execution of a Task | TCP |
| `4` | `TASK_RESPONSE` | Return execution result of a Task | TCP |
| `5` | `TASK_CANCEL` | Instruct worker to cancel in-flight task | TCP |
| `6` | `GOSSIP` | Transmit peer resource load vectors | UDP / TCP |
| `7` | `KAD_RPC` | Kademlia DHT RPC (`PING`, `FIND_NODE`, `STORE`, `FIND_VALUE`) | TCP / UDP |
| `8` | `HEARTBEAT` | Regular node liveness pulse | TCP |
| `9` | `ERROR` | Remote execution or protocol error report | TCP |

---

## 3. LAN Broadcast Beacon

For zero-configuration edge networks (e.g. edge devices connected to the same WiFi or ethernet switch), MeshWeaver broadcasts a periodic UDP beacon to `255.255.255.255:52520` containing node identity and listening ports:

```json
{
  "type": "MSHW_BEACON",
  "node_id": "a3f37006...",
  "host": "192.168.1.42",
  "tcp_port": 8765,
  "udp_port": 8766
}
```

Neighbor nodes capture this beacon and automatically register the new node into their Kademlia routing table.
