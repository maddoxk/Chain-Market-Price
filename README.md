# Chain-Market-Price

A Rust workspace for experimenting with a low-latency crypto market-data pipeline:
**parse an exchange message, normalize it, update a price ladder, encode it (SBE / JSON), fan it out to subscribers.**
The core crates have no third-party dependencies. A separate crate connects to the real Coinbase Exchange public WebSocket feed.

This is a learning and portfolio project. It is not deployed anywhere, it has not been run on colocated or
kernel-bypass hardware, and it does not trade. The table below says exactly which parts are real, which are simulated,
and which are only written down as a design.

## Status

| Area | Status | Where | Notes |
| :--- | :--- | :--- | :--- |
| Live market data from a real venue | **Real** (opt-in crate) | `crates/live-feed`, [docs/LIVE_FEED.md](docs/LIVE_FEED.md) | Coinbase Exchange public `ticker` + `heartbeat` over TLS WebSocket, no API key. L1 quotes and trades only (no order book). Reconnect with backoff, heartbeat watchdog, sequence and `trade_id` gap detection, resubscribe. |
| Coinbase ticker parser | **Real** | `crates/cefi-ingest` | Zero-allocation field extraction; tested on a recorded real capture and run live in CI. |
| Binance / OKX parsers | **Code + static samples only** | `crates/cefi-ingest` | Nothing connects to Binance, OKX or Bybit. (Binance.com blocks US IPs.) There is no Bybit parser. |
| Sequence-gap state machine | **Real, logic only** | `cefi-ingest::SequenceTracker`, `live-feed::Session` | `SequenceTracker` does not fetch REST snapshots; `Session` (Coinbase) does resubscribe. |
| Fixed-point parse, SBE encode/decode, fast JSON | **Real** | `core-engine`, `distribution` | Unit-tested round trips. The SBE codec is hand-written; no test checks it against `schemas/market_data.sbe.xml`. |
| SPSC ring buffer, contiguous price ladder | **Real, in-process** | `core-engine` | Unit-tested. Cross-thread use is exercised by the feed-simulator example. |
| Topic router (bitmap index, token buckets, filters) | **Real, in-process** | `distribution` | Benchmarked (below). |
| WebSocket server | **Frame codec only** | `distribution::websocket` | RFC 6455 frame encode/decode, unmasking, command parsing, in-process broadcast. There is no TCP listener or HTTP upgrade handshake, so no client can connect. `examples/bot_client.rs` prints canned data. |
| Shared-memory IPC | **Protocol only, not cross-process** | `distribution::shm` | Slot layout and SWMR cursor logic are implemented and tested in-process. The file in `/dev/shm` is created and sized, but the ring itself is a heap `Box`; it is never `mmap`ed. Another process cannot read it. The C++ and Python clients in `examples/` and `include/` do `mmap` that file, but nothing writes ring data into it, so they have never read real data from this engine. |
| Kernel bypass (Solarflare ef_vi, DPDK) | **Simulated** | `core-engine::kernel_bypass`, `cefi-ingest::transport` | `KernelBypassBridge` is an in-memory ring filled by `inject_packet`. There is no ef_vi/DPDK FFI. `SolarflareEfViTransport` wraps that simulation. |
| io_uring transport | **Not io_uring** | `cefi-ingest::transport::io_uring` | Uses `std::net::UdpSocket` plus an in-memory completion queue; no io_uring syscalls. |
| Standard UDP socket transport | **Real** | `cefi-ingest::transport::socket` | Non-blocking UDP; not used by the live feed (which is TCP/TLS). |
| NIC / PTP hardware timestamps | **Not implemented** | | `t1_ingest_nic_ns` is a field name. In the live feed it holds wall-clock time at user-space receive. |
| CPU pinning, `SCHED_FIFO` | **Implemented, unverified** | `core-engine::scheduler` | Linux `sched_setaffinity` / `sched_setscheduler` via `extern "C"`; failures are ignored. Not validated on isolated cores. |
| AMM virtualizer (Uniswap v2/v3/v4 math, Curve, CLMM) | **Real math, offline** | `crates/amm-virtualizer` | A handful of unit tests (about 5). Not validated against on-chain state; no chain connection. |
| EVM mempool / Solana (Reth IPC, Geyser, Jito, Bloxroute) | **Not implemented** | `crates/onchain-ingest` | Contains an in-process "speculative swap" calculation on a constant-product pool. No network code. |
| Feed simulator | **Synthetic** | `core-engine::simulator`, `examples/feed_simulator.rs` | Generates fake ticks for stress tests. |
| Docker / Helm / systemd / sysctl files | **Written, not exercised** | `deploy/` | Not run in CI. |
| Docs site | **Partly aspirational** | `website/`, `docs/ARCHITECTURE_BLUEPRINT.md` | Describe the target design. Where they disagree with this README, this README is right. |

## Measured performance

In-process pipeline (one Coinbase `ticker` JSON message already in memory, to router dispatch for 50 subscribed clients),
single thread, GitHub Actions `ubuntu-latest` (4 vCPU shared VM, AMD EPYC 9V45, rustc 1.99.1, release build), three runs:

| | p50 | p99 |
| :--- | :--- | :--- |
| parse | ~0.48 us | 0.52 to 1.04 us |
| router dispatch to 50 clients (incl. SBE + JSON encode) | ~1.37 us | 1.41 to 1.57 us |
| **whole pipeline per message** | **~1.83 us** | 1.90 to 2.26 us |

This is not a wire-to-wire latency: it excludes NIC, kernel, TLS, WebSocket receive and cross-core transfer,
and tails on a shared VM mostly measure the hypervisor. Details, the full table, and how an earlier version of this
benchmark overstated results are in [website/docs/benchmarks/tick-to-egress.md](website/docs/benchmarks/tick-to-egress.md).
Raw CI output is committed in [`results/`](results/). An earlier README and docs quoted numbers (2.16 us, 48 ns SHM reads,
12 ns SBE encode) that were not backed by recorded output; they have been removed.

Live feed, 20 s on a GitHub runner: 174 messages from BTC-USD and ETH-USD, 0 sequence or trade-id gaps; local
processing per BBO (SPSC hand-off + SBE encode) p50 130 ns. See [docs/LIVE_FEED.md](docs/LIVE_FEED.md).

## Try it

```bash
# All tests (no network needed; live feed tests replay a recorded capture)
cargo test --workspace

# Live Coinbase feed for 30 seconds (needs network)
cargo run --release -p live-feed --bin live_feed -- --products BTC-USD,ETH-USD --seconds 30

# Benchmarks
cargo bench -p distribution --bench tick_to_egress
cargo bench -p distribution --bench bench_dispatch
```

Requires a recent stable Rust toolchain (CI uses `dtolnay/rust-toolchain@stable`).

## Layout

```text
crates/
  ingest-models/    Normalized BBO / trade structs, venue ids, 4-point timestamp struct
  core-engine/      SPSC ring, contiguous price ladder, fixed-point parser, topology probe,
                    scheduler, kernel-bypass SIMULATION, feed simulator, cmp-calibrate tool
  cefi-ingest/      JSON parsers (Coinbase, Binance, OKX), sequence tracker, transport adapters
  amm-virtualizer/  AMM math -> price ladders
  onchain-ingest/   In-process speculative swap calculation (no chain connectivity)
  distribution/     SBE codec, JSON writer, topic router, WebSocket frame codec, SHM ring (heap-backed)
  live-feed/        Opt-in: tokio + tokio-tungstenite client for Coinbase; the only crate with dependencies
docs/               DESIGN.md (start here), LIVE_FEED.md, plus design-target documents
results/            Raw benchmark output from CI
```

[docs/DESIGN.md](docs/DESIGN.md) walks through the architecture and trade-offs.

## Limitations

- One venue is connected, with L1 data only. No depth, no multi-venue aggregation, no persistence or replay.
- Benchmarks are single-threaded and in-process on shared cloud VMs. No cross-core, NIC, kernel or network latency is measured.
- "Zero allocation on the hot path" is a design goal. It is not verified with an allocator counter, and the live
  feed binary allocates (WebSocket frames, `Vec`s for events and latency samples).
- The JSON "parser" is a substring scan for known keys, not a validating JSON parser. It assumes keys are unique in the message.
- Shared memory, kernel bypass, io_uring, WebSocket serving and on-chain ingestion are not functional as described in the older design documents.
- The price ladder keeps a fixed 2048-level window around a base price; its best-price tracking is simple
  (it only moves outward unless a level is zeroed).
- No license file is present in the repository.
