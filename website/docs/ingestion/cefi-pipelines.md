---
id: cefi-pipelines
title: Centralized Exchange (CeFi) Ingestion Pipelines
sidebar_label: CeFi Ingestion
---

# Centralized Exchange (CeFi) Ingestion Pipelines

The CeFi ingestion pipeline consumes live WebSocket market data streams from top-tier venues (**Binance**, **OKX**, **Bybit**), normalizes heterogenous protocol schemas into unified `NormalizedBbo` and `UnifiedTrade` structs, and detects network packet drops.

---

## 1. Supported Venues & Streams

| Venue | Venue ID | Stream Type | Format | Sequence Tracking |
| :--- | :--- | :--- | :--- | :--- |
| **Binance** | `1` | `bookTicker`, `@depth`, `@trade` | JSON | Diff-depth Monotonic Sequence Checks (`U <= expected <= u`) |
| **Coinbase** | `2` | `ticker_batch`, `matches` | JSON | Monotonic Sequence Numbers |
| **Bybit** | `3` | `orderbook.1`, `publicTrade` | JSON | Monotonic Sequence Numbers |
| **OKX** | `4` | `bbo-tbt`, `trades` | JSON | Timestamp & Sequence Checking |

---

## 2. Zero-Allocation Fast-Path Parsing

Standard JSON libraries (`serde_json`) construct dynamic AST trees and allocate intermediate heap strings. In contrast, `CefiParser` operates directly on raw immutable byte slices `&[u8]`:

```rust
pub struct CefiParser;

impl CefiParser {
    /// Parses Binance Best Bid & Offer (BBO) with zero allocations
    pub fn parse_binance_bbo(
        market_id: u32,
        raw_json: &[u8],
        t1_ingest_nic_ns: u64,
    ) -> Result<NormalizedBbo, ()> {
        let bid_price_bytes = find_json_string_val(raw_json, b"\"b\":\"")?;
        let bid_qty_bytes = find_json_string_val(raw_json, b"\"B\":\"")?;
        let ask_price_bytes = find_json_string_val(raw_json, b"\"a\":\"")?;
        let ask_qty_bytes = find_json_string_val(raw_json, b"\"A\":\"")?;

        let bid_price = parse_fixed_point_8(bid_price_bytes);
        let bid_qty = parse_fixed_point_8(bid_qty_bytes) as u64;
        let ask_price = parse_fixed_point_8(ask_price_bytes);
        let ask_qty = parse_fixed_point_8(ask_qty_bytes) as u64;

        let spread_bps = if ask_price > bid_price && bid_price > 0 {
            (((ask_price - bid_price) * 10_000) / bid_price) as u16
        } else {
            0
        };

        Ok(NormalizedBbo {
            venue: 1, // Binance
            chain: 0, // Offchain CeFi
            market_id,
            sequence: 0,
            bid_price,
            bid_qty,
            ask_price,
            ask_qty,
            spread_bps,
            flags: 0,
            telemetry: TelemetryTimestamps {
                t0_exchange_ns: 0,
                t1_ingest_nic_ns,
                t2_engine_proc_ns: 0,
                t3_egress_ns: 0,
            },
        })
    }
}
```

---

## 3. Sequence Gap Detection & Reconnect Logic

To prevent stale quotes or crossed books caused by dropped TCP packets:

```rust
pub struct SequenceTracker {
    pub expected_sequence: u64,
    pub dropped_packets: u64,
    pub is_synchronized: bool,
}

impl SequenceTracker {
    pub fn process_binance_update(&mut self, first_id: u64, final_id: u64) -> Result<(), u64> {
        if !self.is_synchronized {
            self.expected_sequence = final_id + 1;
            self.is_synchronized = true;
            return Ok(());
        }

        if first_id <= self.expected_sequence && final_id >= self.expected_sequence {
            self.expected_sequence = final_id + 1;
            Ok(())
        } else {
            // Sequence Gap Detected! Trigger snapshot resynchronization
            let missed = first_id.saturating_sub(self.expected_sequence);
            self.dropped_packets += missed;
            self.is_synchronized = false;
            Err(missed)
        }
    }
}
```
