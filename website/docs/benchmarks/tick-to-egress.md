---
id: tick-to-egress
title: In-Process Pipeline Benchmark
sidebar_label: Latency Benchmarks
---

# In-process pipeline benchmark

`crates/distribution/benches/tick_to_egress.rs` measures how long this codebase takes to turn one
exchange JSON message that is **already in memory** into encoded bytes handed to a no-op sink.
It is a single-threaded, single-process benchmark. It does **not** measure NIC arrival, kernel or TLS receive,
WebSocket framing, cross-core cache transfer, or any real socket send, so it is not a wire-to-wire latency.

An earlier version of this page listed a 2.16 µs median, "hardware PTP" timestamps and a NIC DMA stage. None of that was
backed by recorded output (the old benchmark printed hard-coded stage numbers and derived its T2/T3 timestamps from
constants), so it has been removed.

## What runs in each iteration

| Stage | Code |
| :--- | :--- |
| parse | `CefiParser::parse_coinbase_ticker_bbo` on a ~406-byte Coinbase `ticker` JSON message |
| ring #1, ring #2 | `SpscRingBuffer` push + pop. Same thread, so this is the cost of the operations, not a cross-core hand-off |
| book | `ContiguousOrderBook` bid and ask level update + best-price read |
| dispatch | `InvertedTopicRouter::dispatch_bbo` to 50 subscribed clients (25 SBE, 25 JSON), including lazy SBE and JSON encoding and 50 sink callbacks |

Inputs are a pool of 4096 distinct payloads (different prices, sizes, sequence numbers) cycled, so the code cannot be
specialised to one message. The pool is small and cache-resident, so caches and branch predictors are warmer than on live traffic.
The benchmark asserts that all 50 clients are served on every iteration (a drained rate limiter would otherwise make
dispatch skip work and look faster).

## Measured results

Environment: GitHub Actions `ubuntu-latest` runner (image ubuntu26 20261026.180.1), 4 vCPU shared VM (Hyper-V), AMD EPYC 9V45,
rustc 1.99.1, `cargo bench` (release profile with `lto = "fat"`, `codegen-units = 1`). Commit `587ebf6`.
Raw output: [`results/latest/`](https://github.com/maddoxk/Chain-Market-Price/tree/main/results/latest).
Three consecutive runs in the same job; the range below is across those runs. Times in nanoseconds.

| Measurement | p50 | p99 | p99.9 |
| :--- | :--- | :--- | :--- |
| empty `Instant::now()` pair (timer overhead) | 20 | 21 | 21 to 30 |
| stage: parse | 481 to 491 | 521 to 1042 | 1051 to 1192 |
| stage: ring #1 push + pop | 30 | 31 | 31 to 40 |
| stage: book update | 20 | 31 to 40 | 40 |
| stage: ring #2 push + pop | 30 | 31 to 40 | 40 to 41 |
| stage: dispatch to 50 clients | 1352 to 1392 | 1412 to 1572 | 6690 to 6951 |
| **end-to-end, one timer pair per message** | **1823 to 1853** | **1903 to 2264** | **7220 to 24607** |
| batch mean (2,000,000 messages, one timer around the loop) | 1824 to 1837 ns per message | | |

Reading the table:

- The stage timings include one timer pair each (about 20 ns), so the five stages overstate their true cost by up to
  about 100 ns in total. The end-to-end and batch numbers agree (about 1.83 µs per message), which is the best single figure.
- Roughly 75 percent of the time is `dispatch_bbo`: two `RwLock` reads, hash-map lookups, and per-client
  session checks, token-bucket consumption and callback, for 50 clients. The parse is about 26 percent.
- The p99.9 and max are dominated by scheduler and hypervisor noise on a shared VM. A maximum of several hundred
  microseconds appeared in one run. These numbers say nothing about tail latency on dedicated, isolated cores.
- Different runner VMs differ. An earlier run on a different host CPU (AMD EPYC 7763) gave the same dispatch cost within
  about 30 percent. Treat results as indicative of the code, not as hardware-independent constants.

### A finding the first run exposed

The first CI run (EPYC 7763, raw output in `results/before-find-subsequence-fix/`) measured parse at **4.9 µs** per
message, about 10 times slower than the 0.28 µs the old table claimed. The cause was `find_subsequence` doing a full slice
comparison at every byte offset for each of the ~7 JSON keys. After checking the needle's first and last byte before comparing,
parse dropped to about 0.48 µs (end-to-end 5.9 µs to 1.8 µs).

## Micro-benchmarks (`bench_dispatch`)

Constant inputs, hot caches, perfectly predicted branches. These are best-case per-call costs. Same environment.

| Operation | ns per op |
| :--- | :--- |
| SBE BBO encode (92 bytes) | 1.59 |
| SBE trade encode (95 bytes) | 2.96 |
| JSON BBO serialize | 50.5 |
| JSON trade serialize | 29.3 |
| Router dispatch, 100 clients, no-op sink | 2509 per tick (25.1 per client) |

## Benchmark code changes

- The hand-rolled `black_box` (`read_volatile` of the argument plus `mem::forget`) only makes the pointer value
  opaque, not what it points to, and the old benchmark fed it a string literal that the compiler could see through.
  Replaced with `std::hint::black_box`, and inputs now come from a heap-allocated pool. I did not check whether
  the old code was actually being constant-folded; this change removes the possibility.
- T2 and T3 were `t1 + parse + book` and `t2 + 450` (a constant), and the final banner printed "ALL PRODUCTION CRITERIA MET"
  regardless of the SLA check results. All removed. Run under `cargo test` (debug build) the old SLA checks printed FAILED
  at a 25 µs median, which is how this was noticed.
- `bench_dispatch.rs` had `assert!` SLA thresholds that could fail on a noisy runner. Removed.

## Reproduce

```bash
cargo bench -p distribution --bench tick_to_egress
cargo bench -p distribution --bench bench_dispatch
```

CI job `bench` in `.github/workflows/ci.yml` runs both (tick_to_egress three times) on `ubuntu-latest` and uploads the
raw output as the `bench-output` artifact.
