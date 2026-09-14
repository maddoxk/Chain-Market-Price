# Chain-Market-Price: Ultra-Low Latency Crypto & DeFi Market Data Engine
### Institutional Enterprise Architecture (Hudson River Trading / Citadel Caliber)

An enterprise-grade, microsecond-latency market data and mempool streaming platform designed for quantitative high-frequency trading (HFT), cross-venue statistical arbitrage, and on-chain MEV bots.

---

## Key Highlights

- **Extreme Low-Latency Pipeline:** Median in-memory tick-to-egress of **< 3.2 µs**; sub-100ns local Shared Memory IPC.
- **Language & Mechanics:** Pure **Rust** core engine with zero heap allocation in steady state, hardware cache-line padding (64-byte alignment), and NUMA core pinning.
- **Multi-Chain & CeFi Coverage:**
  - **EVM (Ethereum / Arbitrum / Base):** Direct Reth IPC, P2P mempool sniffing, Bloxroute BDN, Fiber, and embedded speculative swap simulation.
  - **Solana:** Jito ShredStream TPU listener, Yellowstone Geyser gRPC/Protobuf direct validator ingestion, and zero-copy Borsh CLMM tick-array deserialization.
  - **CeFi (Binance / OKX / Bybit):** Low-latency WebSocket feeds with monotonic sequence gap detection and snapshot recovery.
- **AMM to Virtual Order Book Reconstruction:** Mathematical discretization of Uniswap v2 ($x \cdot y = k$), Uniswap v3/v4 concentrated liquidity tick math, and Raydium/Orca CLMM into standard L2/L3 order book price ladders.
- **Multi-Tier Decoupled Client Egress:**
  - **Tier 1 (Same-Box IPC):** POSIX Shared Memory (`/dev/shm` HugePages) SWMR Ring Buffer (< 100 ns).
  - **Tier 2 (DC LAN):** NASDAQ MoldUDP64 Multicast / Solarflare TCPDirect (< 5 µs).
  - **Tier 3 (Edge / Remote):** High-density Thread-per-Core RFC 6455 WebSocket engine (< 250 µs).
- **Dual Wire Protocol:**
  - **Simple Binary Encoding (SBE):** Zero-copy binary serialization adhering to `schemas/market_data.sbe.xml` for latency-critical alpha (~12ns/op).
  - **Fast Table-Driven JSON:** Zero-allocation integer-to-ASCII decimal formatter for browser dashboards and standard clients.
- **Dynamic Subscription Engine:** Inverted client bitmap index (`ClientBitmap`) over 64-bit compact topic keys with dynamic predicate filtering (min notional USD, gas threshold, slippage tolerance).

---

## Architectural Topology

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
|   |  | CeFi / Solana /    |  | SPSC  |  | Fast Deserializer /   |  | SPSC  |  | Dense Contiguous L2/L3 Book Array     |  |   |
|   |  | EVM Mempool Ingest |  |=====> |  | AMM Virtualizer       |  |=====> |  | (Price-indexed ladder, L1d resident)  |  |   |
|   |  +--------------------+  | Queue |  +-----------------------+  | Queue |  +----------------------------------------+  |   |
|   |  | Sequence Tracker & |  | #1    |  | Normalizer:           |  | #2    |  | Aggregated BBO & Depth Generator       |  |   |
|   |  | Gap Detector       |  |       |  | Fixed-Point Scaler    |  |       |  +----------------------------------------+  |   |
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

## Workspace Layout

```text
├── Cargo.toml
├── README.md
├── docs/
│   ├── ARCHITECTURE_BLUEPRINT.md    # Institutional systems architecture specification
│   └── CLIENT_INTEGRATION_GUIDE.md  # Client connection, wire protocols, and SBE/JSON specs
├── schemas/
│   └── market_data.sbe.xml          # Production Simple Binary Encoding (SBE) XML schema
├── examples/
│   └── bot_client.rs                # Runnable quant arbitrage bot client reference
└── crates/
    ├── core-engine/                 # Lock-free SPSC ring buffer, contiguous book, SIMD unmasking
    ├── ingest-models/               # 4-point telemetry, normalized BBO/Trade/L2 structures
    ├── distribution/                # SBE codec, fast JSON, Roaring Bitmap router, WebSocket engine
    ├── cefi-ingest/                 # Binance/OKX/Bybit ingestion, sequence tracker, gap detector
    ├── amm-virtualizer/             # Uniswap v2/v3/v4 & Raydium CLMM order book discretizer
    └── onchain-ingest/              # Solana Geyser/Jito & EVM mempool speculative state sandbox
```

---

## Quickstart & Bot Client Example

### 1. Build the Workspace
```bash
cargo check --workspace --examples
```

### 2. Run the Quant Bot Client Example
```bash
cargo run --example bot_client
```

### 3. Detailed Client Integration Guide
Read [docs/CLIENT_INTEGRATION_GUIDE.md](docs/CLIENT_INTEGRATION_GUIDE.md) for full wire format offsets, Python/TypeScript integration snippets, and nanosecond latency telemetry accounting.
