---
id: rust-integration
title: Rust Integration & Embedded Engine Usage
sidebar_label: Rust Integration
---

# Rust Integration & Embedded Engine Usage

`Chain-Market-Price` is built as a set of modular, independent Rust crates that can be embedded directly into custom Rust trading engines, MEV searchers, or market-making bots.

---

## 1. Crate Architecture

```toml
[dependencies]
core-engine = { path = "crates/core-engine" }
amm-virtualizer = { path = "crates/amm-virtualizer" }
distribution = { path = "crates/distribution" }
ingest-models = { path = "crates/ingest-models" }
```

---

## 2. Embedded SBE Decoding Example

```rust
use distribution::{decode_bbo_sbe, encode_bbo_sbe, TOTAL_SBE_BBO_LEN};
use ingest_models::NormalizedBbo;

fn main() {
    let mut sbe_buffer = [0u8; TOTAL_SBE_BBO_LEN];
    
    // Assume `raw_bytes` arrived from network or shared memory
    let bbo = decode_bbo_sbe(&sbe_buffer).expect("SBE decode failed");
    
    println!("Market: {}", bbo.market_id);
    println!("Bid: ${:.2}", bbo.bid_price as f64 / 1e8);
    println!("Ask: ${:.2}", bbo.ask_price as f64 / 1e8);
    println!("Spread: {} bps", bbo.spread_bps);
    println!("Internal Latency: {} ns", bbo.telemetry.internal_latency_ns());
}
```

---

## 3. High-Speed SPSC Ring Buffer Usage

```rust
use core_engine::SpscRingBuffer;

// Pre-allocate lock-free ring of 1024 elements (64-byte aligned)
let queue: SpscRingBuffer<u64, 1024> = SpscRingBuffer::new();

// Push item (zero alloc, sub-15ns)
queue.try_push(42).unwrap();

// Pop item (zero alloc, sub-15ns)
if let Some(val) = queue.try_pop() {
    assert_eq!(val, 42);
}
```
