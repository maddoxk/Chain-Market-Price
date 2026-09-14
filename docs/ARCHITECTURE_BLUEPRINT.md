# Institutional Systems Architecture Blueprint
## High-Performance CeFi & DeFi Market Data Engine (HRT / Citadel Caliber)

**Document Version:** 1.0.0  
**Classification:** Enterprise Trading Infrastructure Specification  

---

## 1. System Overview & Core Invariants

High-Frequency Trading (HFT), statistical arbitrage, and cross-venue latency arbitrage (e.g., Binance vs. Uniswap v3 vs. Raydium) require capturing price discovery and pending transaction intent (mempool) before they are reflected in block states. A delay of 50 microseconds during a volatility spike or liquidation cascade can result in toxic fills or lost arbitrage opportunities.

### Fundamental Divergence: TradFi vs. Modern Crypto/DeFi

```
+----------------------------------------------------------------------------------------------------+
| TRADITIONAL FINANCE (TradFi: CME, Nasdaq, Eurex)                                                   |
| - Transport: UDP Multicast over dedicated cross-connects (Equinix NY4, LD4, FR2).                  |
| - Framing: Simple Binary Encoding (SBE) / ITCH / OUCH.                                             |
| - Determinism: Strict hardware timestamps, single-order sequencer, zero encryption on internal LAN.|
+----------------------------------------------------------------------------------------------------+
                                                VS
+----------------------------------------------------------------------------------------------------+
| CRYPTO CEFI & DEFI REALITY                                                                         |
| - CeFi (Binance, OKX, Bybit, Coinbase): TLS-encrypted WebSockets streaming dynamic JSON payloads.  |
| - EVM (Ethereum, Arbitrum, Base): P2P mempool gossip, builder bid streams (MEV-Boost), state diffs.|
| - Solana: Leader schedule TPU shreds, Yellowstone Geyser gRPC/Protobuf, Jito Shredstream.         |
| - DEX Models: AMM pools (x*y=k, concentrated liquidity tick math, stableswap curves) rather than  |
|   traditional Limit Order Books (LOB).                                                             |
+----------------------------------------------------------------------------------------------------+
```

### Core Engineering Invariants ("Mechanical Sympathy")
1. **Single-Writer Principle:** Data structures are modified exclusively by a single CPU core. No multi-threaded locking or synchronization primitives (`pthread_mutex`, `std::mutex`) in the hot path.
2. **Zero Dynamic Allocation in Steady State:** Zero `malloc`, zero heap fragmentation, zero runtime garbage collection. All message buffers, ring slots, and order book depth structures are statically pre-allocated in continuous HugePage memory.
3. **Decoupled Asynchronous Egress:** Slow WAN WebSocket clients are strictly isolated from the core processing pipeline. Backpressure never bleeds upstream.
4. **Cache-Line & NUMA Pinning:** All critical data structures are padded and aligned to 64-byte boundaries. Core execution, memory buffers, and NIC DMA rings are pinned to a single NUMA socket.

---

## 2. Language Selection: Modern C++23 vs. Rust

| Dimension | Modern C++23 (`clang++-18` / `g++-14`) | Modern Rust (`rustc 1.82+`, 2024 Edition) | Institutional Verdict |
| :--- | :--- | :--- | :--- |
| **Hot-Path Memory Layout** | Complete raw pointer flexibility, `std::assume_aligned`, manual union punning. | `#[repr(C)]`, `#[repr(align(64))]`, raw pointers (`*const T`, `*mut T`). Bounds checking elided via unsafe blocks. | **Tie.** Both compile to identical x86-64/AVX-512 assembly. |
| **Concurrency & Memory Safety** | `std::atomic` with memory orderings. No compiler defense against data races, memory leaks, or use-after-free under concurrency. | Statically enforced `Send`/`Sync` traits. Data races are impossible in safe Rust; lock-free models verified with `Miri` and `Loom`. | **Rust Wins.** Prevents critical memory corruption during high-volatility flash crashes. |
| **Crypto & Blockchain Ecosystem** | Weak native blockchain crates. Solarflare C headers and legacy FIX engines. | Native Solana validator stack (`solana-program`, Yellowstone Geyser), Ethereum Reth (`alloy`, `revm`), CosmWasm. | **Rust Wins Decisively.** Direct zero-copy integration with node crates without C FFI boundaries. |
| **SIMD JSON Deserialization** | `simdjson` (Lemire et al.): Fast, but DOM-mode creates dynamic trees unless manually mapping to tape. | `sonic-rs` / `simd-json`: Vectorized direct deserialization into typed structs without intermediate DOM allocation. | **Rust Wins.** Direct schema deserialization is faster and avoids intermediate allocation. |
| **Kernel Bypass & Hardware Interop** | Native C ABI headers for Solarflare `ef_vi` and DPDK. | Trivial C FFI bindings (`bindgen`). Zero runtime cost via `#[inline(always)]`. | **Tie.** Thin Rust wrappers expose zero-cost C bindings. |

### Architectural Decision: **Pure Rust Core Engine with C FFI Ingestion Bridge**
The engine core, ring buffers, state simulation, and fanout distribution are built natively in **Rust**, with thin C FFI wrappers for Solarflare `ef_vi` / OpenOnload.

---

## 3. Core Engine Architecture

```
                                  +-------------------------------------------------------------+
                                  |                     HARDWARE LAYER                          |
                                  |  Dual-Socket AMD EPYC 9654 / Intel Xeon Platinum 8480+      |
                                  |  NIC: Solarflare XtremeScale X2522 / Intel E810 (PCIe Gen4) |
                                  +------------------------------+------------------------------+
                                                                 | DMA
                                                                 v
+-------------------------------------------------------------------------------------------------------------------------------+
|                                                NUMA NODE 0 (STRICT PINNING)                                                   |
|                                                                                                                               |
|   +--------------------------+       +-----------------------------+       +----------------------------------------------+   |
|   |   CORE 1 (ISOLCPUS)      |       |     CORE 2 (ISOLCPUS)       |       |            CORE 3 (ISOLCPUS)                 |   |
|   |  Ingestion / Demuxer     |       |  SIMD Protocol Parser       |       |         OrderBook Sequencer                  |   |
|   |                          |       |                             |       |                                              |   |
|   |  +--------------------+  |       |  +-----------------------+  |       |  +----------------------------------------+  |   |
|   |  | Solarflare EF_VI   |  | SPSC  |  | sonic-rs / simd-json  |  | SPSC  |  | Dense Contiguous L2/L3 Book Array     |  |   |
|   |  | Packet RX Ring     |  |=====> |  | Zero-copy parser      |  |=====> |  | (Price-indexed ladder, L1d resident)  |  |   |
|   |  +--------------------+  | Queue |  +-----------------------+  | Queue |  +----------------------------------------+  |   |
|   |  | Userspace TLS      |  | #1    |  | Normalizer:           |  | #2    |  | Aggregated BBO & Depth Generator       |  |   |
|   |  | Record Assembler   |  |       |  | Fixed-Point Converter |  |       |  +----------------------------------------+  |   |
|   |  +--------------------+  |       |  +-----------------------+  |       |                      |                       |   |
|   +--------------------------+       +-----------------------------+       +----------------------|-----------------------+   |
|                                                                                                   | Lock-Free Broadcast       |
|                                                                                                   v SPSC Ring / SHM           |
|                                      +--------------------------------------------------------------------+                   |
|                                      |                     CORES 4-8 (ISOLCPUS)                           |                   |
|                                      |      Distribution Egress: Shared Memory, Multicast & io_uring WS   |                   |
|                                      +--------------------------------------------------------------------+                   |
+-------------------------------------------------------------------------------------------------------------------------------+
```

---

## 4. Multi-Chain Ingestion & Speculative Mempool Execution

### 4.1 Multi-Chain Ingestion Topology
- **Ethereum / EVM L2s:** Direct Reth Engine IPC + Devp2p gossip wire sniffer + Bloxroute BDN + Fiber stream.
- **Solana:** Yellowstone Geyser gRPC direct validator streaming + Jito ShredStream TPU listener + zero-copy Borsh deserialization for Raydium/Orca CLMM accounts.
- **CeFi Ingestion:** Solarflare `ef_vi` hardware rings + `aws-lc-rs` TLS 1.3 decryption + `sonic-rs` AVX-512 JSON parser.

### 4.2 Mathematical AMM to Virtual Order Book Conversion
- **Uniswap v2 Constant Product ($x \cdot y = k$):** Discretized into $N$ price bins around spot:
  $$\Delta x = \sqrt{\frac{R_x R_y}{P_{\text{target}} \cdot \gamma}} - R_x$$
- **Uniswap v3 / v4 Concentrated Liquidity:** Active tick bitmasks scanned using SIMD bit-shifts; discrete depth synthesized in nanoseconds:
  $$\Delta x = L \cdot \left( \frac{1}{\sqrt{P_{\text{lower}}}} - \frac{1}{\sqrt{P_{\text{upper}}}} \right), \quad \Delta y = L \cdot \left( \sqrt{P_{\text{upper}}} - \sqrt{P_{\text{lower}}} \right)$$
- **Curve Stableswap Invariant:** Discretized using Newton-Raphson approximation.

### 4.3 Speculative Mempool Sandbox (`revm`)
- Embedded in-process `revm` copy-on-write state cache.
- Simulates pending transactions before block inclusion.
- Emits predictive order book shift events to give client bots a 200–400ms time advantage.

---

## 5. Multi-Tier Distribution Architecture

```
                                  +-------------------------------------------------------+
                                  |     Core Normalization & Arbitrage Calculation        |
                                  |     (Zero-Allocation Pipeline, Core-Pinned, NUMA 0)   |
                                  +---------------------------+---------------------------+
                                                              |
                                           [Lock-Free SPSC Broadcast Bus]
                                                              |
          +---------------------------------------------------+---------------------------------------------------+
          |                                                   |                                                   |
          v                                                   v                                                   v
+-----------------------+                           +-------------------+                               +--------------------+
|  TIER 1: Local IPC    |                           | TIER 2: DC LAN    |                               | TIER 3: Remote WS  |
|  Shared Memory (SHM)  |                           | Multicast / TCP   |                               | TLS Engine         |
+-----------+-----------+                           +---------+---------+                               +---------+----------+
            |                                                 |                                                   |
            | POSIX SHM Ring                                  | MoldUDP64 / TCPDirect                             | io_uring / kTLS
            | < 100 ns median                                 | < 5 µs median                                     | < 250 µs edge
            v                                                 v                                                   v
+-----------------------+                           +-------------------+                               +--------------------+
| Colocated HFT Bots    |                           | Cross-Connect LAN |                               | Institutional &    |
| (Same Rack / Server)  |                           | (Equinix LD4/NY4) |                               | Public WS Clients  |
+-----------------------+                           +-------------------+                               +--------------------+
```

### 5.1 Tier 1: Same-Box Shared Memory IPC (< 100ns)
- Mounted on `/dev/shm` backed by 1GB HugePages.
- Single-Writer Multi-Reader (SWMR) circular queue with acquire/release memory fences.
- Zero copy, sub-L3 cache bouncing latency (~35 - 75ns).

### 5.2 Tier 2: LAN Multicast (< 5µs)
- Implements the **NASDAQ MoldUDP64** protocol.
- Switch ASIC replication via Arista 7130/7050X3 hardware.
- Unicast TCP sequence replay engine for packet drop recovery.

### 5.3 Tier 3: High-Density Thread-per-Core WebSocket Engine (< 250µs)
- Built on Linux `io_uring` with registered buffers and zero-copy send (`IORING_OP_SEND_ZC`).
- Kernel TLS (`kTLS`) hardware offload via Mellanox ConnectX-6 Dx.
- Scalable to 50,000+ connections per server without head-of-line blocking.

---

## 6. Dynamic Subscription Filtering & Inverted Bitmap Routing

### 6.1 Compact 64-Bit Topic Key
```
+-------------------+--------------------+--------------------+--------------------+
| Venue ID (8 bits) | Asset ID (24 bits) | Stream Type (8 b)  | Sub-Flags (24 b)   |
+-------------------+--------------------+--------------------+--------------------+
```

### 6.2 Inverted Roaring Bitmap Fan-Out
- Subscriptions are indexed via **Roaring Bitmaps**.
- Incoming market updates index into the bitmap in $O(1)$ time.
- AVX-512 SIMD instructions (`_mm512_popcnt_epi64`) scan active client IDs in single-digit clock cycles.

### 6.3 SIMD-Accelerated Dynamic Predicates
- Branchless 32-byte predicate structs (min notional USD, gas priority fee threshold, slippage bps).
- Evaluated across 8 clients concurrently using AVX-2/AVX-512 comparison instructions.

---

## 7. Performance Benchmarks & SLAs

| Pipeline Processing Stage | Target Latency |
| :--- | :--- |
| 1. NIC DMA transfer + EF_VI Rx Ring poll | **0.42 µs** |
| 2. TLS Record Decryption (AES-NI / AVX-512) | **0.85 µs** |
| 3. WebSocket Frame Validation & Demux | **0.12 µs** |
| 4. SPSC Queue Transfer #1 (Ingestion -> Parser) | **0.08 µs** |
| 5. SIMD JSON Deserialization (`sonic-rs`) | **0.74 µs** |
| 6. Fixed-Point Price Normalization | **0.14 µs** |
| 7. SPSC Queue Transfer #2 (Parser -> Book Engine)| **0.08 µs** |
| 8. Contiguous OrderBook L2 Ladder Update | **0.32 µs** |
| 9. Shared Memory Egress (IPC write to SHM) | **0.15 µs** |
| **TOTAL IN-MEMORY TICK-TO-EGRESS (MEDIAN):** | **2.90 µs** |
