---
id: intro
title: Overview & Architecture
sidebar_label: Overview
slug: /intro
---

# Chain-Market-Price

## Institutional Ultra-Low Latency CeFi & DeFi Market Data Engine

**Chain-Market-Price** is an ultra-high performance cryptocurrency market data aggregation, normalization, and distribution platform built in pure Rust. Engineered for high-frequency trading (HFT) desks, quantitative hedge funds, and statistical arbitrageurs, the engine unifies top-tier centralized exchanges (Binance, OKX, Bybit) and decentralized automated market makers (Uniswap v2, v3, v4, Curve, Raydium) into a single, nanosecond-benchmarked order book feed.

---

## Key Performance Characteristics

| Metric | Measured Specification | Architectural Mechanism |
| :--- | :--- | :--- |
| **Colocated IPC Latency** | **< 75 nanoseconds** | Zero-copy POSIX Shared Memory (`/dev/shm`) circular ring |
| **Median Tick-to-Egress ($T_3 - T_1$)** | **~2.16 microseconds** | Lock-free LMAX Disruptor SPSC queues + SIMD parsing |
| **99th Percentile Jitter (P99)** | **< 4.80 microseconds** | Strict CPU core isolation (`isolcpus`), tickless kernel |
| **Dynamic Memory Allocations** | **0 bytes** | Pre-allocated static cache-aligned flat arrays |
| **Cache Line False Sharing** | **0%** | Strict 64-byte alignment (`#[repr(align(64))]`) |
| **Throughput Capacity** | **> 500,000 msgs/sec** | Branchless bitmask filtering & vectorized unmasking |

---

## System Architecture

The engine executes in a pipelined, multi-stage lock-free architecture where each critical execution stage is pinned to dedicated, isolated physical CPU cores on NUMA Node 0:

```
+----------------------------------------------------------------------------------------------------+
|                                    HARDWARE INGRESS & PACKET ARRIVAL                               |
|   Solarflare ef_vi NIC / DPDK C FFI Kernel Bypass  ==>  IEEE 1588 PTP Hardware Timestamp (T1)       |
+----------------------------------------------------------------------------------------------------+
                                                  |
                                                  v  [Lock-Free SPSC Ring Buffer #1]
+----------------------------------------------------------------------------------------------------+
|                                  NORMALIZATION & DESERIALIZATION                                   |
|   - CeFi Normalizer: Zero-Alloc ASCII Parser (Binance, OKX, Bybit)                                 |
|   - On-Chain Ingest: EVM Reth IPC & Solana Yellowstone Geyser Parser                               |
+----------------------------------------------------------------------------------------------------+
                                                  |
                                                  v  [Lock-Free SPSC Ring Buffer #2]
+----------------------------------------------------------------------------------------------------+
|                                   ORDER BOOK & AMM DISCRETIZATION                                  |
|   - Central Limit Order Book: 2048-slot Contiguous Flat Array (PriceLevel)                         |
|   - Uniswap v2: Constant Product Discretizer (Exact Square Root Ratio Scaling)                    |
|   - Uniswap v3/v4: Concentrated Liquidity Dynamic Hook Discretizer                                 |
|   - Curve Stableswap: Newton-Raphson Integer Solver (256-Bit Precision)                            |
|   ==> Engine Processing Complete Timestamp (T2)                                                    |
+----------------------------------------------------------------------------------------------------+
                                                  |
                        +-------------------------+-------------------------+
                        |                                                   |
                        v                                                   v
+-----------------------------------------------+   +-----------------------------------------------+
|       TIER 1: POSIX SHARED MEMORY (IPC)       |   |       TIER 2 & 3: WEBSOCKET DUAL-WIRE         |
|   - Sub-75ns Circular Ring (/dev/shm)         |   |   - SBE Binary Framing (Template 101, 104)    |
|   - 64-Byte Cache-Aligned ShmMessageSlot      |   |   - Vectorized RFC 6455 Frame Unmasking       |
|   - C++23 (_mm_pause) & Python 3.11 MMap      |   |   - Inverted Roaring Bitmap Topic Router      |
|   ==> Wire Egress Timestamp (T3)              |   |   ==> Wire Egress Timestamp (T3)              |
+-----------------------------------------------+   +-----------------------------------------------+
```

---

## Core Principles: Mechanical Sympathy

1. **Zero Runtime Allocation:**
   No heap allocation (`malloc`, `free`, `Vec::push`, `String`) occurs on the steady-state ingestion, normalization, or distribution hot paths. All buffers, queues, and order books are pre-allocated during engine bootstrap.
2. **Cache-Conscious Memory Layout:**
   All critical data structures (`ShmMessageSlot`, `PriceLevel`, `SpscRingBuffer` cursors) are aligned to 64-byte boundaries (`alignas(64)` / `#[repr(align(64))]`) matching CPU L1/L2 cache lines, completely eliminating false sharing across cores.
3. **Pure Integer Fixed-Point Arithmetic:**
   Floating-point instructions (`f32`, `f64`) introduce non-deterministic rounding and CPU flag register state stalls. All prices and quantities are handled as $10^8$ fixed-point integers (`i64`/`u64`), with 256-bit wide integer math for Curve Stableswap invariants.
4. **4-Point Nanosecond Latency Accounting:**
   Every market event tracks four synchronized timestamps ($T_0, T_1, T_2, T_3$), giving algorithms instant visibility into venue matching engine latency, network transit time, internal processing latency, and client egress delays.
