---
id: shared-memory
title: Production POSIX Shared Memory (SHM) IPC Engine
sidebar_label: Shared Memory IPC
---

# Production POSIX Shared Memory (SHM) IPC Engine

For institutional high-frequency trading (HFT) and statistical arbitrage strategies colocated on the same bare-metal server chassis, traditional TCP and WebSocket loopbacks introduce unacceptable OS kernel network stack overhead (5 to 25 µs).

`Chain-Market-Price` provides a Tier-1 zero-copy POSIX Shared Memory circular ring buffer delivering tick consumption in **under 75 nanoseconds**.

---

## 1. Shared Memory Architecture

```
                  +-------------------------------------------------------+
                  |  PRODUCER: Chain-Market-Price Engine Core (Core 5)     |
                  +-------------------------------------------------------+
                                              |
                          Memory-Mapped Zero-Copy Write (< 15ns)
                                              v
+-------------------------------------------------------------------------------------------+
|               POSIX SHARED MEMORY SEGMENT (/dev/shm/cmp_market_data.shm)                  |
|                                                                                           |
|  [ ShmHeader: Magic (CMP1) | Version | Capacity (16384) | Head Sequence (AtomicU64) ]     |
|                                                                                           |
|  Slot 0: [ 64-Byte Cache-Aligned ShmMessageSlot (Seq: 16384, Price, Qty, Telemetry) ]    |
|  Slot 1: [ 64-Byte Cache-Aligned ShmMessageSlot (Seq: 16385, Price, Qty, Telemetry) ]    |
|  Slot 2: [ 64-Byte Cache-Aligned ShmMessageSlot (Seq: 16386, Price, Qty, Telemetry) ]    |
|  ...                                                                                      |
|  Slot 16383: [ 64-Byte Cache-Aligned ShmMessageSlot (Seq: 16383, Price, Qty, Telemetry) ] |
+-------------------------------------------------------------------------------------------+
                                              |
                +-----------------------------+-----------------------------+
                |                                                           |
  Optimistic Read (< 35ns)                                    Optimistic Read (< 35ns)
                v                                                           v
+------------------------------------+                     +------------------------------------+
|  CONSUMER 1: C++23 Strategy Engine |                     |  CONSUMER 2: Python Alpha Feed     |
|  (include/cmp/shm_client.hpp)      |                     |  (examples/python/hft_alpha_feed)  |
+------------------------------------+                     +------------------------------------+
```

---

## 2. Invariants & Synchronization

1. **Exact 64-Byte Slot Size:**
   Every `ShmMessageSlot` is exactly 64 bytes (`alignas(64)` / `#[repr(align(64))]`), matching modern CPU L1/L2 cache lines 1:1.
2. **Single-Writer Multi-Reader (SWMR):**
   A single high-priority writer updates the ring sequentially. Any number of reader processes can consume ticks concurrently without acquiring locks.
3. **Monotonic Sequence Barriers:**
   The writer writes payload fields first, then issues a `Release` fence to update `head_sequence`. Readers execute an optimistic `memcpy`, followed by a sequence parity check to detect concurrent overwrites.
4. **Hardware Pause Instruction:**
   When the queue is empty, reader polling loops issue a CPU pause intrinsic (`_mm_pause()` on x86-64, `isb` on ARM64) to save power and prevent pipeline stalls upon new tick arrival.

---

## 3. Multi-Tier Elastic Shared Memory & Cascading Fallback

To support heterogeneous hardware environments, `ElasticShmRing` automatically negotiates the storage backend and capacity:

| Operational Tier | Storage Backend | Ring Capacity | Memory Footprint | Privilege Level |
| :--- | :--- | :--- | :--- | :--- |
| **Tier 3: Enterprise Bare Metal** | `HugeTLB` (2MB/1GB pages) | 65,536 slots | 4 MB contiguous pinned RAM | `CAP_IPC_LOCK` / HugePages mount |
| **Tier 2: Cloud Virtualized** | `PosixShm` (`/dev/shm`) | 16,384 slots | 1 MB standard 4KB pages | Standard container unprivileged |
| **Tier 1: Mid-Range Workstation** | `AnonymousOrTmpfs` (`/tmp`) | 4,096 slots | 256 KB temporary file | Completely unprivileged (macOS/Linux) |
| **Safe Failover** | `InMemory` | Dynamic | RAM heap ring | Zero filesystem access required |

### Automated Cascading Fallback State Machine

```text
Boot: Request SHM Ring
         |
         v
[Stage 1: HugeTLB /dev/hugepages] -- (Failed: ENOMEM/EPERM/macOS) --> [Stage 2: POSIX SHM /dev/shm]
                                                                               |
                                                                               +-- (Failed: Container limit/ReadOnly) --> [Stage 3: tmpfs /tmp]
                                                                                                                                 |
                                                                                                                                 +-- (Failed) --> [Stage 4: In-Memory Safe Fallback]
```

### Usage Example

```rust
use distribution::shm::{ElasticShmConfig, ElasticShmRing};
use core_engine::topology::HardwareTier;

// Initialize optimal ring configuration based on detected hardware tier
let config = ElasticShmConfig::for_tier(HardwareTier::Tier2CloudVirtualized);
let mut ring = ElasticShmRing::create_or_fallback(config, std::process::id());

// Publish normalized BBO tick
ring.publish_bbo(&bbo, 1_000_000);
```

