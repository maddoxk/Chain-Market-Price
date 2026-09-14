---
id: topic-routing
title: Inverted Roaring Bitmap Topic Routing & Predicates
sidebar_label: Topic Routing
---

# Inverted Roaring Bitmap Topic Routing & Predicates

Broadcasting market data feeds to hundreds of connected client sessions with diverse subscription requirements requires high-throughput routing that minimizes latency and cache pollution.

---

## 1. 64-Bit Topic Key Packing

Every data feed stream is identified by a unique 64-bit integer (`TopicKey`):

```rust
#[repr(transparent)]
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct TopicKey(pub u64);

impl TopicKey {
    pub const fn new(venue_id: u16, market_id: u32, stream_type: u8, flags: u8) -> Self {
        let val = ((venue_id as u64) << 48)
            | ((market_id as u64) << 16)
            | ((stream_type as u64) << 8)
            | (flags as u64);
        TopicKey(val)
    }
}
```

---

## 2. Inverted Roaring Bitmap Indexing

- **Bitset Representation:** Each topic points to a `ClientBitmap` representing active subscriber IDs.
- **Hardware-Accelerated Traversal:** The router uses hardware `CTZ` (Count Trailing Zeros) and `BLSR` (`w &= w - 1`) instructions to iterate subscriber IDs in constant time $O(k)$ where $k$ is the number of active subscribers.
- **Lazy Zero-Alloc Serialization:** If multiple clients on the same topic request SBE, the binary frame is encoded once into a thread-local static buffer and fanned out directly.
- **Client-Side Predicates:** Clients can register dynamic filter predicates (`min_notional_usd`, `max_spread_bps`), suppressing unwanted ticks at the server level.
