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
