---
id: shm-layout
title: Shared Memory Binary Memory Layout
sidebar_label: SHM Memory Layout
---

# Shared Memory Binary Memory Layout

The POSIX shared memory ring buffer at `/dev/shm/cmp_market_data.shm` enforces binary compatibility across Rust, Modern C++23, and Python 3.11+.

---

## 1. File Layout Overview

```text
+--------------------------------------------------------------------------------+
|  ShmHeader (Offset 0 .. 64 bytes)                                              |
|  - magic: u32 (0x434D5031 "CMP1")                                              |
|  - version: u32 (1)                                                            |
|  - slot_capacity: u32 (16384)                                                  |
|  - slot_size_bytes: u32 (64)                                                   |
|  - writer_pid: u32                                                             |
|  - _pad0: u32                                                                  |
|  - writer_heartbeat_ns: AtomicU64                                              |
|  - head_sequence: AtomicU64 (Cache-Aligned to offset 64)                       |
+--------------------------------------------------------------------------------+
|  Slot 0 (Offset 64 .. 128 bytes): ShmMessageSlot (64 Bytes)                    |
+--------------------------------------------------------------------------------+
|  Slot 1 (Offset 128 .. 192 bytes): ShmMessageSlot (64 Bytes)                  |
+--------------------------------------------------------------------------------+
|  ...                                                                           |
+--------------------------------------------------------------------------------+
|  Slot N-1: ShmMessageSlot (64 Bytes)                                           |
+--------------------------------------------------------------------------------+
```

---

## 2. `ShmMessageSlot` Field Offsets (Exactly 64 Bytes)

| Byte Offset | Field | Type | Description |
| :--- | :--- | :--- | :--- |
| `0..8` | `sequence` | `uint64_t` / `u64` | Monotonic sequence number |
| `8..9` | `kind` | `uint8_t` / `u8` | Message type (1=BBO, 2=Trade, 3=Heartbeat) |
| `9..10` | `flags` | `uint8_t` / `u8` | Flags (0x01=Synthetic, 0x02=Mempool) |
| `10..12` | `venue_id` | `uint16_t` / `u16` | Venue identifier (1=Binance, 102=UniV3) |
| `12..16` | `market_id` | `uint32_t` / `u32` | Global token pair ID |
| `16..24` | `t0_exchange_ns` | `uint64_t` / `u64` | Venue matching engine timestamp |
| `24..32` | `t1_ingest_nic_ns`| `uint64_t` / `u64` | Hardware PTP ingress timestamp |
| `32..40` | `t2_engine_proc_ns`| `uint64_t` / `u64`| Engine processing complete timestamp |
| `40..48` | `t3_egress_ns` | `uint64_t` / `u64` | Ring buffer commit timestamp |
| `48..56` | `price` | `int64_t` / `i64` | Scaled price ($P \times 10^8$) |
| `56..64` | `qty` | `uint64_t` / `u64` | Scaled quantity ($Q \times 10^8$) |
