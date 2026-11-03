//! # CeFi message parsing and sequence tracking
//!
//! Zero-allocation parsers that turn exchange JSON payloads into `NormalizedBbo` /
//! `UnifiedTrade`, plus a sequence-gap tracker and a set of transport adapters.
//!
//! Status of each venue parser:
//! - **Coinbase Exchange `ticker`**: exercised against the real public feed by the
//!   `live-feed` crate and tested on recorded real messages.
//! - **Binance `bookTicker` / trade, OKX v5 tickers**: tested on small static samples
//!   only. Nothing in this repository connects to those venues.
//! - **Bybit**: no parser exists.
//!
//! Notes:
//! - The "snapshot" side of `SequenceTracker` is a state machine only; this crate does
//!   not fetch REST snapshots.
//! - The JSON field extraction is a substring scan, not a full JSON parser. It is fast
//!   and allocation-free, but it assumes the key text is unique in the message.

#![allow(clippy::all)]

pub mod transport;

pub use transport::{
    IoUringTransport, MockNetworkTransport, NetworkTransport, SolarflareEfViTransport,
    StandardSocketTransport, TimestampNs, TransportBackendKind, TransportConfig, TransportError,
    TransportFactory, TransportStats,
};

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

    /// Parses a Coinbase Exchange `ticker` message into a `NormalizedBbo`.
    ///
    /// The ticker channel carries the best bid/ask and sizes alongside the last
    /// trade, so one message gives an L1 quote. `sequence` is Coinbase's
    /// per-product sequence number; `time` is an ISO-8601 UTC string.
    pub fn parse_coinbase_ticker_bbo(
        market_id: u32,
        payload: &[u8],
        t1_ingest_ns: u64,
    ) -> Result<NormalizedBbo, &'static str> {
        let bid_price = extract_json_str_field(payload, b"\"best_bid\":")?;
        let bid_qty = extract_json_str_field(payload, b"\"best_bid_size\":")?;
        let ask_price = extract_json_str_field(payload, b"\"best_ask\":")?;
        let ask_qty = extract_json_str_field(payload, b"\"best_ask_size\":")?;
        let seq = extract_json_int_field(payload, b"\"sequence\":").ok_or("sequence missing")?;
        let t0 = extract_json_str_field(payload, b"\"time\":")
            .ok()
            .and_then(parse_iso8601_utc_ns)
            .unwrap_or(0);

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
                t0_exchange_ns: t0,
                t1_ingest_nic_ns: t1_ingest_ns,
                t2_engine_proc_ns: 0,
                t3_egress_ns: 0,
            },
            market_id,
            venue: VenueId::Coinbase as u16,
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

    /// Parses the last-trade part of a Coinbase `ticker` message into a `UnifiedTrade`.
    /// `side` in the Coinbase ticker is the taker side ("buy" / "sell").
    pub fn parse_coinbase_ticker_trade(
        market_id: u32,
        payload: &[u8],
        t1_ingest_ns: u64,
    ) -> Result<UnifiedTrade, &'static str> {
        let price = extract_json_str_field(payload, b"\"price\":")?;
        let size = extract_json_str_field(payload, b"\"last_size\":")?;
        let side = extract_json_str_field(payload, b"\"side\":")?;
        let trade_id =
            extract_json_int_field(payload, b"\"trade_id\":").ok_or("trade_id missing")?;
        let seq = extract_json_int_field(payload, b"\"sequence\":").unwrap_or(0);
        let t0 = extract_json_str_field(payload, b"\"time\":")
            .ok()
            .and_then(parse_iso8601_utc_ns)
            .unwrap_or(0);

        Ok(UnifiedTrade {
            telemetry: TelemetryTimestamps {
                t0_exchange_ns: t0,
                t1_ingest_nic_ns: t1_ingest_ns,
                t2_engine_proc_ns: 0,
                t3_egress_ns: 0,
            },
            market_id,
            venue: VenueId::Coinbase as u16,
            chain: ChainId::OffChain as u16,
            sequence: seq,
            trade_id,
            price: parse_fixed_point_8(price),
            size: parse_fixed_point_8(size) as u64,
            side: if side == b"sell" { 2 } else { 1 },
            is_liquidation: false,
        })
    }

    /// Extracts the `"type"` field of a Coinbase message ("ticker", "heartbeat", ...).
    pub fn coinbase_message_type(payload: &[u8]) -> Option<&[u8]> {
        extract_json_str_field(payload, b"\"type\":").ok()
    }

    /// Extracts the integer `trade_id` field (ticker) or `last_trade_id` (heartbeat).
    pub fn coinbase_trade_id(payload: &[u8]) -> Option<u64> {
        extract_json_int_field(payload, b"\"trade_id\":")
            .or_else(|| extract_json_int_field(payload, b"\"last_trade_id\":"))
    }

    /// Extracts the per-product `sequence` integer.
    pub fn coinbase_sequence(payload: &[u8]) -> Option<u64> {
        extract_json_int_field(payload, b"\"sequence\":")
    }

    /// Extracts the `product_id` string, e.g. `BTC-USD`.
    pub fn coinbase_product_id(payload: &[u8]) -> Option<&[u8]> {
        extract_json_str_field(payload, b"\"product_id\":").ok()
    }
}

/// Parses `YYYY-MM-DDTHH:MM:SS[.ffffff]Z` (UTC) into nanoseconds since the Unix epoch.
/// Returns `None` on anything that does not match that shape.
pub fn parse_iso8601_utc_ns(s: &[u8]) -> Option<u64> {
    fn num(b: &[u8]) -> Option<u64> {
        let mut v = 0u64;
        for &c in b {
            if !(b'0'..=b'9').contains(&c) {
                return None;
            }
            v = v * 10 + (c - b'0') as u64;
        }
        Some(v)
    }
    if s.len() < 20
        || s[4] != b'-'
        || s[7] != b'-'
        || s[10] != b'T'
        || s[13] != b':'
        || s[16] != b':'
    {
        return None;
    }
    let (y, m, d) = (
        num(&s[0..4])? as i64,
        num(&s[5..7])? as i64,
        num(&s[8..10])? as i64,
    );
    let (hh, mm, ss) = (num(&s[11..13])?, num(&s[14..16])?, num(&s[17..19])?);
    if !(1..=12).contains(&m) || !(1..=31).contains(&d) || hh > 23 || mm > 59 || ss > 60 {
        return None;
    }
    // Fractional seconds: up to 9 digits, right-padded to nanoseconds.
    let mut idx = 19;
    let mut frac_ns = 0u64;
    if s[idx] == b'.' {
        idx += 1;
        let mut digits = 0;
        while idx < s.len() && s[idx] != b'Z' {
            if digits < 9 {
                frac_ns = frac_ns * 10 + (s[idx].checked_sub(b'0').filter(|v| *v <= 9)? as u64);
                digits += 1;
            } else if !s[idx].is_ascii_digit() {
                return None;
            }
            idx += 1;
        }
        while digits < 9 {
            frac_ns *= 10;
            digits += 1;
        }
    }
    if idx >= s.len() || s[idx] != b'Z' {
        return None;
    }
    // Days since 1970-01-01 from a proleptic Gregorian date (Howard Hinnant's algorithm).
    let y = if m <= 2 { y - 1 } else { y };
    let era = (if y >= 0 { y } else { y - 399 }) / 400;
    let yoe = y - era * 400;
    let mp = (m + 9) % 12;
    let doy = (153 * mp + 2) / 5 + d - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    let days = era * 146_097 + doe - 719_468;
    if days < 0 {
        return None;
    }
    let secs = days as u64 * 86_400 + hh * 3600 + mm * 60 + ss;
    Some(secs * 1_000_000_000 + frac_ns)
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

/// Finds `needle` in `haystack`. Checks the first and last byte of the needle before doing a
/// full compare, which avoids a slice comparison at every offset. (The first benchmark run
/// showed the naive `windows().position()` version dominating parse time: ~4.9 us per message.)
fn find_subsequence(haystack: &[u8], needle: &[u8]) -> Option<usize> {
    let n = needle.len();
    if n == 0 || haystack.len() < n {
        return None;
    }
    let first = needle[0];
    let last = needle[n - 1];
    let mut s = 0;
    let end = haystack.len() - n;
    while s <= end {
        if haystack[s] == first && haystack[s + n - 1] == last && &haystack[s..s + n] == needle {
            return Some(s);
        }
        s += 1;
    }
    None
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

    #[test]
    fn test_find_subsequence() {
        assert_eq!(find_subsequence(b"abcabc", b"abc"), Some(0));
        assert_eq!(find_subsequence(b"xxabc", b"abc"), Some(2));
        assert_eq!(find_subsequence(b"ab", b"abc"), None);
        assert_eq!(find_subsequence(b"abd abx", b"abc"), None);
        assert_eq!(find_subsequence(b"abc", b""), None);
        assert_eq!(find_subsequence(b"\"b\":\"1\"", b"\"b\":"), Some(0));
    }

    #[test]
    fn test_iso8601_parsing() {
        // 2023-01-01T00:00:00Z = 1672531200
        assert_eq!(
            parse_iso8601_utc_ns(b"2023-01-01T00:00:00Z"),
            Some(1_672_531_200_000_000_000)
        );
        assert_eq!(
            parse_iso8601_utc_ns(b"2023-01-01T00:00:01.041089Z"),
            Some(1_672_531_201_041_089_000)
        );
        // Leap day: 2024-02-29T12:00:00Z = 1709208000
        assert_eq!(
            parse_iso8601_utc_ns(b"2024-02-29T12:00:00Z"),
            Some(1_709_208_000_000_000_000)
        );
        assert_eq!(parse_iso8601_utc_ns(b"not a time"), None);
        assert_eq!(parse_iso8601_utc_ns(b"2023-13-01T00:00:00Z"), None);
        assert_eq!(parse_iso8601_utc_ns(b"2023-01-01T00:00:00"), None);
    }

    const CB_TICKER: &[u8] = br#"{"type":"ticker","sequence":138715980456,"product_id":"BTC-USD","price":"77328.57","open_24h":"77747.9","volume_24h":"7190.97519819","low_24h":"76238.54","high_24h":"77980.33","volume_30d":"159706.40974752","best_bid":"77328.56","best_bid_size":"0.00000069","best_ask":"77328.57","best_ask_size":"0.14181185","side":"buy","time":"2026-11-03T14:35:39.041089Z","trade_id":1114414143,"last_size":"0.00040846"}"#;

    #[test]
    fn test_coinbase_ticker_bbo_and_trade() {
        let bbo = CefiParser::parse_coinbase_ticker_bbo(7, CB_TICKER, 5).unwrap();
        assert_eq!(bbo.venue, VenueId::Coinbase as u16);
        assert_eq!(bbo.sequence, 138_715_980_456);
        assert_eq!(bbo.bid_price, 7_732_856_000_000);
        assert_eq!(bbo.bid_qty, 69);
        assert_eq!(bbo.ask_price, 7_732_857_000_000);
        assert_eq!(bbo.ask_qty, 14_181_185);
        assert_eq!(bbo.telemetry.t0_exchange_ns % 1_000_000_000, 41_089_000);

        let t = CefiParser::parse_coinbase_ticker_trade(7, CB_TICKER, 5).unwrap();
        assert_eq!(t.trade_id, 1_114_414_143);
        assert_eq!(t.price, 7_732_857_000_000);
        assert_eq!(t.size, 40_846);
        assert_eq!(t.side, 1);
    }

    #[test]
    fn test_coinbase_ticker_rejects_missing_fields() {
        assert!(CefiParser::parse_coinbase_ticker_bbo(1, br#"{"type":"ticker"}"#, 0).is_err());
    }
}
