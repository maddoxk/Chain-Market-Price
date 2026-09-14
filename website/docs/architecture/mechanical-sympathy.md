---
id: mechanical-sympathy
title: Mechanical Sympathy & Memory Invariants
sidebar_label: Mechanical Sympathy
---

# Mechanical Sympathy & Memory Invariants

To achieve deterministic sub-microsecond latency, software must cooperate with modern microprocessor architecture—specifically memory caches (L1, L2, L3), instruction pipelining, CPU interconnect buses, and branch predictors.

---

## 1. 64-Byte Cache Line Alignment & False Sharing Elimination

In modern x86-64 (Intel Xeon / AMD EPYC) and ARM64 (Apple Silicon / Graviton) architectures, CPU memory controllers fetch RAM in **64-byte chunks (cache lines)**.

### The False Sharing Trap
When two independent CPU cores modify distinct variables located within the same 64-byte cache line, the hardware cache coherency protocol (MESI/MOESI) invalidates the cache line across all cores. This forces costly L3/RAM cache snooping cycles (~50 to 150 nanoseconds per write).

### The Solution: Explicit Cache Padding
All shared memory structs and atomic cursors in `Chain-Market-Price` are explicitly aligned to 64 bytes using `#[repr(align(64))]`:

```rust
#[repr(align(64))]
pub struct CachePadded<T>(pub T);

/// 64-Byte Cache-Line Aligned Shared Memory Message Slot
#[repr(C, align(64))]
#[derive(Clone, Copy, Default, Debug, PartialEq, Eq)]
pub struct ShmMessageSlot {
    pub sequence: u64,                  // 8 bytes
    pub kind: u8,                       // 1 byte
    pub flags: u8,                      // 1 byte
    pub venue_id: u16,                  // 2 bytes
    pub market_id: u32,                 // 4 bytes
    pub telemetry: TelemetryTimestamps, // 32 bytes
    pub price: i64,                     // 8 bytes
    pub qty: u64,                       // 8 bytes
} // Total: Exactly 64 bytes (1 cache line)
```

```
+----------------------------------------------------------------------------------------------------+
|                                    64-BYTE CPU CACHE LINE                                          |
| [Seq: 8B] [Kind: 1B] [Flags: 1B] [Venue: 2B] [Mkt: 4B] [Telemetry: 32B] [Price: 8B] [Qty: 8B]     |
+----------------------------------------------------------------------------------------------------+
|<--------------------------------------- 64 Bytes ------------------------------------------------->|
```

---

## 2. Lock-Free Single-Producer Single-Consumer (SPSC) Ring Buffers

Inter-thread messaging uses a lock-free circular queue modeled after the **LMAX Disruptor pattern**.

### Memory Synchronization Architecture
- **Producer:** Reads consumer cursor via relaxed atomic loads, only executing an `Acquire` barrier when the queue appears full. Writes payload, then issues a `Release` fence when incrementing the `producer_cursor`.
- **Consumer:** Reads producer cursor via relaxed atomic loads, only executing an `Acquire` barrier when the queue appears empty. Reads payload, then issues a `Release` fence when advancing `consumer_cursor`.

```rust
pub struct SpscRingBuffer<T, const N: usize> {
    producer_cursor: CachePadded<AtomicU64>,
    cached_consumer_cursor: CachePadded<AtomicU64>,

    consumer_cursor: CachePadded<AtomicU64>,
    cached_producer_cursor: CachePadded<AtomicU64>,

    buffer: Box<[UnsafeCell<T>]>,
}
```

### Measured Cost
- Queue Push: **~ 12 nanoseconds**
- Queue Pop: **~ 14 nanoseconds**
- Zero OS system calls, zero kernel locks (`futex`/`mutex`), zero thread context switches.

---

## 3. Branchless Topic Routing

Traditional pub/sub message brokers evaluate nested `if/else` checks or hash maps to match client subscriptions, causing CPU instruction pipeline stalls from branch mispredictions.

`Chain-Market-Price` eliminates branches using an **Inverted Roaring Bitmap**:
- Topics map to dense 64-bit bitmasks (`ClientBitmap`).
- Client subscriptions are registered by setting bits.
- Delivering a tick requires only bitwise operations (`&`, `|`, `^`) and hardware `BLSR` (`w &= w - 1`) / `CTZ` (`trailing_zeros`) instructions to enumerate subscriber indices in under **8 nanoseconds**.
