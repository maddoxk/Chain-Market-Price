# Design walk-through

Read this before an interview: it explains what each piece does, why it is shaped that way, and where the
shortcuts are. File paths are relative to the repo root.

## 1. The pipeline

```
 exchange JSON  ->  parse  ->  NormalizedBbo  ->  SPSC ring  ->  price ladder  ->  SPSC ring  ->  router  ->  SBE / JSON bytes
 (live-feed)       (cefi-ingest)  (ingest-models)  (core-engine)  (core-engine)                    (distribution)
```

The benchmark (`crates/distribution/benches/tick_to_egress.rs`) runs exactly this chain on one thread.
The live binary (`crates/live-feed/src/bin/live_feed.rs`) runs `Session -> ring -> SBE encode`.

## 2. Data model (`crates/ingest-models`)

`NormalizedBbo` and `UnifiedTrade` are `#[repr(C)]`, `Copy`, fixed-size structs. Prices and sizes are `i64`/`u64` scaled
by 10^8 (fixed point), never floats: no rounding drift, cheap comparison, easy to hash and to put on the wire.
`TelemetryTimestamps` carries four times: `t0` exchange, `t1` local ingest, `t2` engine done, `t3` egress.
Only `t0` (from the exchange) and `t1` (wall clock at receive) are filled by the live feed. Despite the field name
`t1_ingest_nic_ns`, it is not a NIC timestamp.

## 3. Parsing (`crates/cefi-ingest/src/lib.rs`)

`CefiParser::parse_coinbase_ticker_bbo` finds known keys (`"best_bid":` and so on) with a byte scan and converts the
quoted decimal string straight to a scaled integer with `core_engine::parse_fixed_point_8`. No `String`, no `Vec`, no
serde.

Trade-off: this is fast to write and to run, but it is not a JSON parser. It would misbehave if a key text appeared
inside a string value, and it does not validate the document. For a feed whose shape we know, that is acceptable; for
untrusted input it is not. The benchmark showed why the scan itself matters: comparing a slice at every offset made
parsing a 400-byte message cost 4.9 us; checking the first and last byte first brought it to 0.48 us.

ISO-8601 timestamps are converted to epoch nanoseconds with a days-from-civil formula (Hinnant), so no date crate is needed.

## 4. Live connection and failure handling (`crates/live-feed`, `docs/LIVE_FEED.md`)

Split on purpose:

- `session.rs`: a pure state machine. Input: message bytes and a `now_ms` argument. Output: normalized events plus an optional
  `Action` (resubscribe / reconnect). No sockets, no clock, no async, so every failure path is a plain unit test
  that replays a recorded capture with lines deleted or duplicated.
- `bin/live_feed.rs`: owns the socket, the timer and the backoff, and does what `Session` says.

Why `trade_id` and not only `sequence`: the ticker channel emits only matches, but `sequence` also advances for events
that are not on the channel, so it has holes. `trade_id` is contiguous per product, so a jump means a lost message.

## 5. SPSC ring (`crates/core-engine/src/lib.rs`)

A bounded single-producer single-consumer queue: power-of-two capacity, head and tail indices in separate 64-byte-aligned
`CachePadded` cells to avoid false sharing, producer publishes with `Release`, consumer observes with `Acquire`.
Single producer and single consumer means no CAS loops: each index has exactly one writer.
In this repo's benchmark and live binary it is used from one thread, so the measured cost (about 30 ns per push+pop pair
including timer overhead) is the cost of the operations, not of moving a cache line between cores.

## 6. Price ladder (`ContiguousOrderBook`)

Two flat arrays of 2048 `PriceLevel`s indexed by `|price - base| / tick`. Update is O(1) arithmetic, no tree or hash map,
and the whole thing is contiguous. Trade-offs: a fixed window around a base price (prices outside are ignored), a
simple best-price rule (moves outward, recalculates by scanning only when the best level is zeroed), and the same index for
prices above and below the base. It is a sketch of a ladder, not a complete matching-engine book.

## 7. Fan-out (`crates/distribution`)

- `TopicKey` packs (venue, market, stream type, flags) into one `u64`.
- `InvertedTopicRouter` maps topic to a roaring-style bitmap of client ids, so a tick finds its subscribers with a hash lookup
  and a bitmap iteration rather than scanning all clients.
- Per client, in order: session active, spread filter, token-bucket rate limit, then the message is encoded lazily
  once per wire protocol (SBE or JSON) and handed to a callback.
- Locks: two `RwLock` reads per dispatch. That, plus per-client work, is about 75 percent of the pipeline time in the benchmark.
  A lock-free design would be the next step if this mattered.
- `sbe.rs` is a hand-written fixed-layout binary codec (8-byte header + fixed block). It is round-trip tested; it is
  not generated from, or checked against, the XML schema.

## 8. Things that look like systems code but are simulated

- **Shared memory** (`distribution/src/shm.rs`): the slot layout and single-writer multi-reader sequence protocol are real
  and tested, but the ring is a heap allocation. The `/dev/shm` file is created and sized and then ignored. Making it real
  requires `mmap` (so a libc dependency or hand-written FFI) and a writer that places the header and slots in the mapping.
- **Kernel bypass** (`core-engine/src/kernel_bypass.rs`, `cefi-ingest/src/transport/`): an in-memory descriptor ring that
  tests fill with `inject_packet`. No ef_vi, no DPDK. The "io_uring" transport uses a normal UDP socket.
- **On-chain** (`crates/onchain-ingest`): arithmetic on a constant-product pool for a made-up pending transaction.

These are interfaces and data structures worth discussing, but the README labels them as simulated for a reason.

## 9. Benchmark methodology

See `website/docs/benchmarks/tick-to-egress.md`. In short: vary inputs, use `std::hint::black_box`, check that the
benchmark did the work (all 50 clients served), run three passes (per-stage timers, per-message timer, one timer for a batch)
and measure the timer cost. Numbers come from GitHub-hosted runners and are archived under `results/`.

## 10. If this continued

1. `mmap`-backed SHM with a real cross-process reader test.
2. Coinbase `level2` (needs auth) or a venue with public depth, feeding the ladder with real deltas.
3. Cross-core benchmark: producer and consumer pinned to different cores, measured with a shared monotonic clock.
4. A validating or at least fuzzed JSON path, plus a proptest for the fixed-point parser.
