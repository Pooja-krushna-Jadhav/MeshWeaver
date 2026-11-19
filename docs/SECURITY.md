# MeshWeaver Security & Threat Model

## 1. Threat Model & Security Boundaries

In edge computing environments, devices often operate on physically exposed or semi-trusted local networks. MeshWeaver implements a multi-layered defense strategy:

```text
┌────────────────────────────────────────────────────────┐
│                   Untrusted Network                    │
└──────────────────────────┬─────────────────────────────┘
                           │ 1. Frame bounds & CRC32 validation
                           ▼
┌────────────────────────────────────────────────────────┐
│                   Replay Protection                    │
│      (Timestamp window ≤ 30s + Unique Nonce cache)     │
└──────────────────────────┬─────────────────────────────┘
                           │ 2. Timestamp check & Nonce verification
                           ▼
┌────────────────────────────────────────────────────────┐
│             Cryptographic Authentication               │
│       (Ed25519 Asymmetric PKI or HMAC-SHA256 Secret)   │
└──────────────────────────┬─────────────────────────────┘
                           │ 3. Signature verification
                           ▼
┌────────────────────────────────────────────────────────┐
│                 Authorization Policy                   │
│      (Node Whitelist & Capability Access Matrix)       │
└──────────────────────────┬─────────────────────────────┘
                           │ 4. Whitelist check
                           ▼
┌────────────────────────────────────────────────────────┐
│               Task Deserialization Gate                │
│    (Default: Safe Typed Handlers; RCE Cloudpickle OFF) │
└──────────────────────────┬─────────────────────────────┘
                           │ 5. Execution Sandbox
                           ▼
┌────────────────────────────────────────────────────────┐
│                   Task Execution                       │
│    (Thread pool, Concurrency Semaphore, Timeouts)      │
└────────────────────────────────────────────────────────┘
```

---

## 2. Remote Code Execution (RCE) Defense

The original specification proposed `cloudpickle` for task serialization. Deserializing arbitrary Python bytecode allows remote arbitrary code execution (RCE) by design.

MeshWeaver solves this security risk through a **Dual-Mode Execution Architecture**:

1. **Default Mode: Registered Task Handlers (Zero RCE Risk)**
   - Only pre-registered named tasks (e.g. `math.add`, `image.process`, `analytics.aggregate`) can be called.
   - Arguments and keyword arguments are serialized as strict, typed JSON data structures.
   - Arbitrary code deserialization is strictly rejected.
2. **Optional Mode: Signed Cloudpickle Execution**
   - Disabled by default (`allow_cloudpickle = False`).
   - Can only be enabled explicitly via configuration (`--allow-cloudpickle` or `MESHWEAVER_ALLOW_CLOUDPICKLE=1`).
   - Requires valid Ed25519 digital signature matching an administrator public key whitelist.
   - Monitored by execution timeouts to prevent resource starvation.

---

## 3. Cryptographic Authentication & Replay Defense

Every message envelope contains:
- `timestamp`: UTC epoch timestamp in seconds.
- `nonce`: Cryptographically secure random 16-byte hex string (`secrets.token_hex(16)`).
- `signature`: Ed25519 or HMAC-SHA256 signature calculated over the canonical payload.

### Replay Attack Prevention
The `ReplayProtector` enforces:
1. **Timestamp Freshness**: Rejects any message where $|t_{now} - t_{msg}| > \text{replay\_window\_sec}$ (default 30 seconds).
2. **Nonce De-duplication**: Tracks all observed nonces within the valid time window. Re-submission of any previously seen nonce raises `ReplayAttackError`.
