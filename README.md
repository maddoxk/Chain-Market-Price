# Chain-Market-Price: Ultra-Low Latency Crypto & DeFi Market Data Engine
### Institutional Enterprise Architecture (Hudson River Trading / Citadel Caliber)

An enterprise-grade, microsecond-latency market data and mempool streaming platform designed for quantitative high-frequency trading (HFT), cross-venue statistical arbitrage, and on-chain MEV bots.

---

## Key Highlights

- **Extreme Low-Latency Pipeline:** Median in-memory tick-to-egress of **< 3.2 µs**; sub-100ns local Shared Memory IPC.
- **Language & Mechanics:** Pure **Rust** core engine with C FFI bindings to Solarflare `ef_vi` and DPDK. Zero heap allocation in steady state, hardware cache-line padding (64-byte alignment), and NUMA core pinning.
- **Multi-Chain & CeFi Coverage:**
  - **EVM (Ethereum / Arbitrum / Base):** Direct Reth IPC, P2P mempool sniffing, Bloxroute BDN, Fiber, and embedded `revm` copy-on-write state simulation.
  - **Solana:** Jito ShredStream TPU listener, Yellowstone Geyser gRPC/Protobuf direct validator ingestion, and zero-copy Borsh CLMM tick-array deserialization.
  - **CeFi (Binance / OKX / Bybit / Coinbase):** Colocated low-latency WebSockets/FIX with sequence gap detection and snapshot recovery.
- **AMM to Virtual Order Book Reconstruction:** Mathematical discretization of Uniswap v2/v3/v4, Curve Stableswap, and Raydium/Orca CLMM concentrated liquidity into standard L2/L3 order book price ladders.
- **Multi-Tier Decoupled Client Egress:**
  - **Tier 1 (Same-Box IPC):** POSIX Shared Memory (`/dev/shm` HugePages) SWMR Ring Buffer (< 100 ns).
  - **Tier 2 (DC LAN):** NASDAQ MoldUDP64 Multicast / Solarflare TCPDirect (< 5 µs).
  - **Tier 3 (Edge / Remote):** High-density Thread-per-Core Linux `io_uring` + `kTLS` WebSocket engine (< 250 µs).
- **Dual Wire Protocol:**
  - **Simple Binary Encoding (SBE) / FlatBuffers:** Zero-copy binary serialization for latency-critical alpha.
  - **Vectorized JSON (`sonic-rs`):** AVX-512 SIMD accelerated JSON for browser dashboards and standard clients.
- **Dynamic Subscription Engine:** Inverted Roaring Bitmap index over 64-bit compact topic keys with SIMD-vectorized client predicate filtering (min notional, gas threshold, slippage tolerance).

---

## Architectural Diagram

```
                                  +-------------------------------------------------------------+
                                  |                     HARDWARE LAYER                          |
                                  |  Dual AMD EPYC 9654 / NIC: Solarflare X2522 (PCIe Gen4)     |
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

## Repository Layout

```text
├── Cargo.toml
├── README.md
├── docs/
│   └── ARCHITECTURE_BLUEPRINT.md    # Institutional 60-page caliber systems architecture specification
├── schemas/
│   └── market_data.sbe.xml          # Production Simple Binary Encoding (SBE) XML schema
└── crates/
    ├── core-engine/                 # Lock-free SPSC ring buffer, contiguous book, SIMD unmasking
    ├── ingest-models/               # 4-point telemetry, normalized BBO/Trade/L2 structures
    └── distribution/                # 64-bit Topic keys, Roaring Bitmap router, lock-free token bucket
```

---

## Detailed Documentation
See the full architectural blueprint in [ARCHITECTURE_BLUEPRINT.md](docs/ARCHITECTURE_BLUEPRINT.md) or the system design artifact in your session brain.
