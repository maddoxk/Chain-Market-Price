//! In-process pipeline benchmark: payload in memory -> encoded bytes handed to a no-op sink.
//!
//! WHAT IS MEASURED (single thread, one process, no network, no NIC, no syscalls):
//!
//!   1. parse     `CefiParser::parse_coinbase_ticker_bbo` on a JSON payload already in memory
//!   2. ring #1   `SpscRingBuffer` push + pop (same thread, so this is the cost of the
//!                operations themselves, not of a cross-core hand-off)
//!   3. book      `ContiguousOrderBook` bid + ask update and BBO read
//!   4. ring #2   push + pop again
//!   5. dispatch  `InvertedTopicRouter::dispatch_bbo` to 50 subscribed clients (25 SBE, 25 JSON);
//!                this includes the lazy SBE and JSON encoding and the 50 sink callbacks
//!
//! WHAT IS NOT MEASURED: NIC arrival, kernel/TLS/WebSocket receive, cross-core cache-line
//! transfer, any real socket send. Nothing here is a "wire-to-wire" latency.
//!
//! HOW: three separate passes over the same input stream so that timer cost does not leak
//! into the numbers unnoticed:
//!   A. per-stage pass: an `Instant` pair around each stage
//!   B. end-to-end pass: one `Instant` pair around the whole iteration
//!   C. batch pass: one timer around the whole loop (ns/op, no per-iteration timer cost)
//! The cost of an empty `Instant::now()` pair is measured and printed so it can be read off.
//!
//! INPUTS vary (a pool of 4096 distinct payloads with different prices, sizes, sequence and
//! trade ids, cycled), so the parse/book code cannot be specialised to one message. The pool
//! is still small and cache-resident, so branch prediction and caches are warmer than they
//! would be on live traffic.
//!
//! Output ends with machine-readable `RESULT` lines that CI archives.

#![allow(clippy::all)]

use std::hint::black_box;
use std::time::Instant;

use cefi_ingest::CefiParser;
use core_engine::{ContiguousOrderBook, SpscRingBuffer};
use distribution::{ClientSession, InvertedTopicRouter, TopicKey, WireProtocol, STREAM_TYPE_BBO};
use ingest_models::{NormalizedBbo, VenueId};

const POOL: usize = 4096;
const WARMUP: usize = 20_000;
const SAMPLES: usize = 200_000;
const CLIENTS: u32 = 50;
const MARKET_ID: u32 = 101;
/// 77320.00 in 1e-8 fixed point. Prices in the pool stay within +1.50 of this, which is inside
/// the order book's 2048-level window (2048 * 0.001 = 2.048).
const BASE_PRICE: i64 = 77_320_00_000_000;

/// Coinbase `ticker` messages with the same shape as the recorded live feed.
fn make_payloads() -> Vec<Vec<u8>> {
    (0..POOL)
        .map(|i| {
            let cents = 7_732_000 + (i * 37 % 150) as u64;
            let bid = format!("{}.{:02}", cents / 100, cents % 100);
            let ask = format!("{}.{:02}", (cents + 1) / 100, (cents + 1) % 100);
            format!(
                "{{\"type\":\"ticker\",\"sequence\":{},\"product_id\":\"BTC-USD\",\"price\":\"{}\",\
\"open_24h\":\"77747.9\",\"volume_24h\":\"7190.97519819\",\"low_24h\":\"76238.54\",\
\"high_24h\":\"77980.33\",\"volume_30d\":\"159706.40974752\",\"best_bid\":\"{}\",\
\"best_bid_size\":\"0.{:08}\",\"best_ask\":\"{}\",\"best_ask_size\":\"0.{:08}\",\
\"side\":\"buy\",\"time\":\"2026-11-03T14:35:39.041089Z\",\"trade_id\":{},\
\"last_size\":\"0.00040846\"}}",
                138_715_980_000u64 + i as u64 * 3,
                ask,
                bid,
                1 + (i * 7919) % 99_999_999,
                ask,
                1 + (i * 104_729) % 99_999_999,
                1_114_414_000u64 + i as u64
            )
            .into_bytes()
        })
        .collect()
}

struct Pipeline {
    ingress: SpscRingBuffer<NormalizedBbo, 1024>,
    sequencer: SpscRingBuffer<NormalizedBbo, 1024>,
    book: ContiguousOrderBook,
    router: InvertedTopicRouter,
    delivered: u64,
    bytes: u64,
}

impl Pipeline {
    fn new() -> Self {
        let router = InvertedTopicRouter::new();
        // Coinbase venue id, so the BBOs produced by the parser hit this topic.
        let topic = TopicKey::new(VenueId::Coinbase as u8, MARKET_ID, STREAM_TYPE_BBO, 0);
        for id in 1..=CLIENTS {
            let proto = if id % 2 == 0 {
                WireProtocol::Sbe
            } else {
                WireProtocol::Json
            };
            // Huge token bucket: with now_ms fixed at 0 nothing refills, and a drained bucket
            // would make dispatch skip work and look faster than it is.
            let s = ClientSession::new(id, proto, 2_000_000_000, 2_000_000_000, 0);
            router.register_client(s).unwrap();
            router.subscribe(id, topic).unwrap();
        }
        Pipeline {
            ingress: SpscRingBuffer::new(),
            sequencer: SpscRingBuffer::new(),
            book: ContiguousOrderBook::new(BASE_PRICE),
            router,
            delivered: 0,
            bytes: 0,
        }
    }

    #[inline(always)]
    fn dispatch(&mut self, bbo: &NormalizedBbo) {
        let mut bytes = 0u64;
        let m = self.router.dispatch_bbo(bbo, 0, |_id, _proto, payload| {
            bytes += payload.len() as u64;
        });
        self.delivered += m.delivered_clients as u64;
        self.bytes += bytes;
    }

    /// Whole pipeline, no timers.
    #[inline(always)]
    fn run_once(&mut self, payload: &[u8], t1: u64) {
        let bbo = CefiParser::parse_coinbase_ticker_bbo(MARKET_ID, black_box(payload), t1).unwrap();
        self.ingress.try_push(bbo).unwrap();
        let a = self.ingress.try_pop().unwrap();
        self.book.update_bid(a.bid_price, a.bid_qty, 1, t1);
        self.book.update_ask(a.ask_price, a.ask_qty, 1, t1);
        let mut b = a;
        b.bid_price = self.book.bbo_bid().unwrap().price;
        b.ask_price = self.book.bbo_ask().unwrap().price;
        self.sequencer.try_push(b).unwrap();
        let c = self.sequencer.try_pop().unwrap();
        self.dispatch(&c);
    }
}

fn pct(sorted: &[u64], num: usize, den: usize) -> u64 {
    sorted[(sorted.len() * num / den).min(sorted.len() - 1)]
}

fn report(name: &str, mut v: Vec<u64>) {
    v.sort_unstable();
    println!(
        "{:<34} p50 {:>6}  p90 {:>6}  p99 {:>6}  p99.9 {:>7}  max {:>8}  (ns, n={})",
        name,
        pct(&v, 50, 100),
        pct(&v, 90, 100),
        pct(&v, 99, 100),
        pct(&v, 999, 1000),
        v[v.len() - 1],
        v.len()
    );
    println!(
        "RESULT\t{}\t{}\t{}\t{}\t{}\t{}",
        name.replace(' ', "_"),
        pct(&v, 50, 100),
        pct(&v, 90, 100),
        pct(&v, 99, 100),
        pct(&v, 999, 1000),
        v[v.len() - 1]
    );
}

fn main() {
    println!(
        "tick_to_egress: in-process pipeline benchmark, build profile = {}",
        if cfg!(debug_assertions) {
            "DEBUG (numbers are not meaningful; run with --release / cargo bench)"
        } else {
            "release"
        }
    );
    let payloads = make_payloads();
    println!(
        "payload pool: {} messages, ~{} bytes each\n",
        POOL,
        payloads[0].len()
    );

    let mut p = Pipeline::new();

    // Correctness check before timing anything: every client must actually be served.
    p.run_once(&payloads[0], 1);
    assert_eq!(
        p.delivered, CLIENTS as u64,
        "router did not deliver to all clients; benchmark would not measure full fan-out"
    );
    p.delivered = 0;
    p.bytes = 0;

    for i in 0..WARMUP {
        p.run_once(&payloads[i % POOL], i as u64);
    }

    // --- timer overhead: an empty Instant pair ---
    let mut timer = Vec::with_capacity(SAMPLES);
    for _ in 0..SAMPLES {
        let t = Instant::now();
        timer.push(t.elapsed().as_nanos() as u64);
    }

    // --- Pass A: per-stage timers ---
    let mut s_parse = Vec::with_capacity(SAMPLES);
    let mut s_ring1 = Vec::with_capacity(SAMPLES);
    let mut s_book = Vec::with_capacity(SAMPLES);
    let mut s_ring2 = Vec::with_capacity(SAMPLES);
    let mut s_disp = Vec::with_capacity(SAMPLES);
    let mut s_sum = Vec::with_capacity(SAMPLES);
    p.delivered = 0;
    for i in 0..SAMPLES {
        let payload = &payloads[i % POOL];
        let t1 = i as u64;

        let t = Instant::now();
        let bbo = CefiParser::parse_coinbase_ticker_bbo(MARKET_ID, black_box(payload), t1).unwrap();
        let d_parse = t.elapsed().as_nanos() as u64;
        black_box(&bbo);

        let t = Instant::now();
        p.ingress.try_push(bbo).unwrap();
        let a = p.ingress.try_pop().unwrap();
        let d_ring1 = t.elapsed().as_nanos() as u64;

        let t = Instant::now();
        p.book.update_bid(a.bid_price, a.bid_qty, 1, t1);
        p.book.update_ask(a.ask_price, a.ask_qty, 1, t1);
        let mut b = a;
        b.bid_price = p.book.bbo_bid().unwrap().price;
        b.ask_price = p.book.bbo_ask().unwrap().price;
        let d_book = t.elapsed().as_nanos() as u64;

        let t = Instant::now();
        p.sequencer.try_push(b).unwrap();
        let c = p.sequencer.try_pop().unwrap();
        let d_ring2 = t.elapsed().as_nanos() as u64;

        let t = Instant::now();
        p.dispatch(black_box(&c));
        let d_disp = t.elapsed().as_nanos() as u64;

        s_parse.push(d_parse);
        s_ring1.push(d_ring1);
        s_book.push(d_book);
        s_ring2.push(d_ring2);
        s_disp.push(d_disp);
        s_sum.push(d_parse + d_ring1 + d_book + d_ring2 + d_disp);
    }
    assert_eq!(p.delivered, SAMPLES as u64 * CLIENTS as u64);

    // --- Pass B: one timer pair per iteration ---
    let mut e2e = Vec::with_capacity(SAMPLES);
    p.delivered = 0;
    for i in 0..SAMPLES {
        let payload = &payloads[i % POOL];
        let t = Instant::now();
        p.run_once(payload, i as u64);
        e2e.push(t.elapsed().as_nanos() as u64);
    }
    assert_eq!(p.delivered, SAMPLES as u64 * CLIENTS as u64);

    // --- Pass C: batch, no per-iteration timer ---
    const BATCH: usize = 2_000_000;
    p.delivered = 0;
    let t = Instant::now();
    for i in 0..BATCH {
        p.run_once(&payloads[i % POOL], i as u64);
    }
    let batch_ns = t.elapsed().as_nanos() as f64;
    assert_eq!(p.delivered, BATCH as u64 * CLIENTS as u64);
    black_box(p.bytes);

    println!(
        "--- A. per-stage (Instant pair around each stage; includes timer overhead below) ---"
    );
    report("timer overhead (empty pair)", timer);
    report("stage parse", s_parse);
    report("stage ring#1 push+pop", s_ring1);
    report("stage book update", s_book);
    report("stage ring#2 push+pop", s_ring2);
    report("stage dispatch (50 clients)", s_disp);
    report("sum of stages", s_sum);
    println!("\n--- B. end-to-end (one Instant pair per iteration) ---");
    report("end-to-end per message", e2e);
    println!(
        "\n--- C. batch ({} iterations, one timer around the loop) ---",
        BATCH
    );
    println!("batch mean: {:.1} ns/message", batch_ns / BATCH as f64);
    println!(
        "RESULT\tbatch_mean_ns_per_message\t{:.1}",
        batch_ns / BATCH as f64
    );
    println!(
        "\nScope: in-process, single thread, payload already in memory, no-op sink. Not a wire-to-wire latency."
    );
}
