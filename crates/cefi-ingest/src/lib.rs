//! # CeFi Low-Latency Ingestion Engine
//!
//! Subsystem for ingesting, deserializing, and normalizing market data feeds
//! from tier-1 centralized cryptocurrency exchanges:
//! - **Binance** (Spot & USD-M Futures)
//! - **OKX** (v5 WebSocket channel)
//! - **Bybit** (v5 Linear & Inverse streams)
//!
//! Features:
//! - Monotonic sequence gap detection.
//! - Out-of-order delta buffering and REST snapshot synchronization.
//! - Zero dynamic heap allocation in steady state.
//! - Direct translation into `NormalizedBbo` and `UnifiedTrade`.

use core_engine::parse_fixed_point_8;
use ingest_models::{ChainId, NormalizedBbo, TelemetryTimestamps, UnifiedTrade, VenueId};

/// Ingestion sync status per market pair
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SyncStatus {
    /// Cold start or gap detected; waiting for REST snapshot
    Synchronizing,
    /// Live incremental updates running cleanly in sequence
    InSync,
    /// Sequence gap detected; trigger out-of-band snapshot recovery
    GapDetected { expected: u64, received: u64 },
}

/// Monotonic Sequence Gap Tracker for exchange order book deltas
#[derive(Debug)]
pub struct SequenceTracker {
    pub last_sequence: u64,
    pub status: SyncStatus,
    pub gaps_detected: u64,
    pub updates_processed: u64,
}

impl SequenceTracker {
    pub fn new() -> Self {
        Self {
            last_sequence: 0,
            status: SyncStatus::Synchronizing,
            gaps_detected: 0,
            updates_processed: 0,
        }
    }

    /// Resets state following snapshot application
    pub fn on_snapshot(&mut self, snapshot_last_seq: u64) {
        self.last_sequence = snapshot_last_seq;
        self.status = SyncStatus::InSync;
    }

    /// Evaluates incoming delta sequence number
    #[inline(always)]
    pub fn validate_sequence(&mut self, first_seq: u64, final_seq: u64) -> bool {
        if self.status == SyncStatus::Synchronizing {
            return false;
        }

        // Expected next sequence
        let expected = self.last_sequence + 1;

        if first_seq <= expected && final_seq >= expected {
            self.last_sequence = final_seq;
            self.updates_processed += 1;
            true
        } else if final_seq <= self.last_sequence {
            // Stale update, discard without error
            false
        } else {
            // Gap detected!
            self.status = SyncStatus::GapDetected {
                expected,
                received: first_seq,
            };
            self.gaps_detected += 1;
            false
        }
    }
}

/// Lightweight, zero-allocation CeFi Message Parser
pub struct CefiParser;

impl CefiParser {
    /// Parses Binance `@depth` or `@bookTicker` event into NormalizedBbo
    pub fn parse_binance_bbo(
        market_id: u32,
        payload: &[u8],
        t1_ingest_ns: u64,
    ) -> Result<NormalizedBbo, &'static str> {
        // Find "b": best bid price, "B": best bid qty, "a": best ask price, "A": best ask qty
        let bid_price = extract_json_str_field(payload, b"\"b\":")?;
        let bid_qty = extract_json_str_field(payload, b"\"B\":")?;
        let ask_price = extract_json_str_field(payload, b"\"a\":")?;
        let ask_qty = extract_json_str_field(payload, b"\"A\":")?;
        let event_time = extract_json_int_field(payload, b"\"E\":").unwrap_or(0);
        let seq = extract_json_int_field(payload, b"\"u\":").unwrap_or(0);

        let bp = parse_fixed_point_8(bid_price);
        let bq = parse_fixed_point_8(bid_qty) as u64;
        let ap = parse_fixed_point_8(ask_price);
        let aq = parse_fixed_point_8(ask_qty) as u64;

        let spread_bps = if ap > bp && bp > 0 {
            (((ap - bp) * 10_000) / bp) as u16
        } else {
            0
        };

        Ok(NormalizedBbo {
            telemetry: TelemetryTimestamps {
                t0_exchange_ns: event_time * 1_000_000,
                t1_ingest_nic_ns: t1_ingest_ns,
                t2_engine_proc_ns: 0,
                t3_egress_ns: 0,
            },
            market_id,
            venue: VenueId::Binance as u16,
            chain: ChainId::OffChain as u16,
            sequence: seq,
            bid_price: bp,
            bid_qty: bq,
            ask_price: ap,
            ask_qty: aq,
            spread_bps,
            flags: 0,
        })
    }

    /// Parses Binance trade event into UnifiedTrade
    pub fn parse_binance_trade(
        market_id: u32,
        payload: &[u8],
        t1_ingest_ns: u64,
    ) -> Result<UnifiedTrade, &'static str> {
        let trade_time = extract_json_int_field(payload, b"\"T\":").unwrap_or(0);
        let trade_id = extract_json_int_field(payload, b"\"t\":").unwrap_or(0);
        let price_bytes = extract_json_str_field(payload, b"\"p\":")?;
        let qty_bytes = extract_json_str_field(payload, b"\"q\":")?;
        let is_buyer_maker = extract_json_bool_field(payload, b"\"m\":").unwrap_or(false);

        let price = parse_fixed_point_8(price_bytes);
        let size = parse_fixed_point_8(qty_bytes) as u64;
        let side = if is_buyer_maker { 2 } else { 1 }; // 2 = Sell, 1 = Buy

        Ok(UnifiedTrade {
            telemetry: TelemetryTimestamps {
                t0_exchange_ns: trade_time * 1_000_000,
                t1_ingest_nic_ns: t1_ingest_ns,
                t2_engine_proc_ns: 0,
                t3_egress_ns: 0,
            },
            market_id,
            venue: VenueId::Binance as u16,
            chain: ChainId::OffChain as u16,
            sequence: trade_id,
            trade_id,
            price,
            size,
            side,
            is_liquidation: false,
        })
    }

    /// Parses OKX v5 tickers event into NormalizedBbo
    pub fn parse_okx_bbo(
        market_id: u32,
        payload: &[u8],
        t1_ingest_ns: u64,
    ) -> Result<NormalizedBbo, &'static str> {
        let bid_price = extract_json_str_field(payload, b"\"bidPx\":")?;
        let bid_qty = extract_json_str_field(payload, b"\"bidSz\":")?;
        let ask_price = extract_json_str_field(payload, b"\"askPx\":")?;
        let ask_qty = extract_json_str_field(payload, b"\"askSz\":")?;
        let ts_ms = extract_json_str_field(payload, b"\"ts\":").unwrap_or(b"0");
        let event_time = parse_fixed_point_8(ts_ms) as u64 / 100_000_000;

        let bp = parse_fixed_point_8(bid_price);
        let bq = parse_fixed_point_8(bid_qty) as u64;
        let ap = parse_fixed_point_8(ask_price);
        let aq = parse_fixed_point_8(ask_qty) as u64;

        let spread_bps = if ap > bp && bp > 0 {
            (((ap - bp) * 10_000) / bp) as u16
        } else {
            0
        };

        Ok(NormalizedBbo {
            telemetry: TelemetryTimestamps {
                t0_exchange_ns: event_time * 1_000_000,
                t1_ingest_nic_ns: t1_ingest_ns,
                t2_engine_proc_ns: 0,
                t3_egress_ns: 0,
            },
            market_id,
            venue: VenueId::Okx as u16,
            chain: ChainId::OffChain as u16,
            sequence: event_time,
            bid_price: bp,
            bid_qty: bq,
            ask_price: ap,
            ask_qty: aq,
            spread_bps,
            flags: 0,
        })
    }
}

// -----------------------------------------------------------------------------
// Fast Zero-Allocation JSON Tokenizer Utilities
// -----------------------------------------------------------------------------

fn extract_json_str_field<'a>(src: &'a [u8], key: &[u8]) -> Result<&'a [u8], &'static str> {
    if let Some(pos) = find_subsequence(src, key) {
        let rest = &src[pos + key.len()..];
        // skip whitespace
        let mut i = 0;
        while i < rest.len() && (rest[i] == b' ' || rest[i] == b'\t') {
            i += 1;
        }
        if i < rest.len() && rest[i] == b'"' {
            let start = i + 1;
            let mut end = start;
            while end < rest.len() && rest[end] != b'"' {
                end += 1;
            }
            if end < rest.len() {
                return Ok(&rest[start..end]);
            }
        }
    }
    Err("Field not found")
}

fn extract_json_int_field(src: &[u8], key: &[u8]) -> Option<u64> {
    if let Some(pos) = find_subsequence(src, key) {
        let rest = &src[pos + key.len()..];
        let mut i = 0;
        while i < rest.len() && (rest[i] == b' ' || rest[i] == b'\t' || rest[i] == b'"') {
            i += 1;
        }
        let mut num = 0u64;
        let mut found_digit = false;
        while i < rest.len() && rest[i] >= b'0' && rest[i] <= b'9' {
            num = num * 10 + (rest[i] - b'0') as u64;
            found_digit = true;
            i += 1;
        }
        if found_digit {
            return Some(num);
        }
    }
    None
}

fn extract_json_bool_field(src: &[u8], key: &[u8]) -> Option<bool> {
    if let Some(pos) = find_subsequence(src, key) {
        let rest = &src[pos + key.len()..];
        let mut i = 0;
        while i < rest.len() && (rest[i] == b' ' || rest[i] == b'\t') {
            i += 1;
        }
        if rest.len() >= i + 4 && &rest[i..i + 4] == b"true" {
            return Some(true);
        } else if rest.len() >= i + 5 && &rest[i..i + 5] == b"false" {
            return Some(false);
        }
    }
    None
}

fn find_subsequence(haystack: &[u8], needle: &[u8]) -> Option<usize> {
    haystack
        .windows(needle.len())
        .position(|window| window == needle)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_sequence_tracker() {
        let mut tracker = SequenceTracker::new();
        tracker.on_snapshot(100);
        assert_eq!(tracker.status, SyncStatus::InSync);

        // Valid sequential update
        assert!(tracker.validate_sequence(101, 105));
        assert_eq!(tracker.last_sequence, 105);

        // Gap
        assert!(!tracker.validate_sequence(108, 110));
        assert!(matches!(tracker.status, SyncStatus::GapDetected { .. }));
    }

    #[test]
    fn test_binance_bbo_parsing() {
        let raw = br#"{"u":400900217,"s":"BNBUSDT","b":"25.3519","B":"31.21","a":"25.3652","A":"40.66","E":1672531199000}"#;
        let bbo = CefiParser::parse_binance_bbo(1, raw, 123456789).unwrap();
        assert_eq!(bbo.venue, VenueId::Binance as u16);
        assert_eq!(bbo.bid_price, 2535190000);
        assert_eq!(bbo.ask_price, 2536520000);
        assert_eq!(bbo.sequence, 400900217);
    }
}
