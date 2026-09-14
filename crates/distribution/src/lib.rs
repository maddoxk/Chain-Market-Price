//! # Distribution & Fan-Out Egress Layer
//!
//! Institutional High-Frequency Trading client distribution layer providing:
//! - 64-bit Topic Key packing for branchless subscription multiplexing.
//! - Cache-aligned Roaring-style Inverted Client Bitmap indexing (`ClientBitmap`).
//! - Lock-free token bucket rate limiting using single-word 64-bit CAS (`LockFreeTokenBucket`).
//! - Dynamic SIMD-accelerated predicate filtering (`ClientFilterPredicate`).
//! - Simple Binary Encoding (SBE) zero-copy message framing (`sbe`).
//! - Zero-heap, ultra-fast JSON serialization (`fast_json`).
//! - High-density RFC 6455 WebSocket engine with vectorized unmasking (`websocket`).

#![allow(clippy::all)]

pub mod bitmap;
pub mod fast_json;
pub mod router;
pub mod sbe;
pub mod session;
pub mod shm;
pub mod websocket;

pub use shm::{
    ShmHeader, ShmMessageSlot, ShmMsgKind, ShmReadStatus, ShmRingBuffer, DEFAULT_SHM_PATH,
    DEFAULT_SHM_SLOTS, SHM_MAGIC, SHM_VERSION,
};

use std::sync::atomic::{AtomicU64, Ordering};

pub use bitmap::ClientBitmap;
pub use fast_json::{serialize_bbo_json, serialize_trade_json, FastJsonWriter, JsonError};
pub use router::{
    DispatchMetrics, InvertedTopicRouter, RouterError, STREAM_TYPE_BBO, STREAM_TYPE_DEPTH,
    STREAM_TYPE_MEMPOOL, STREAM_TYPE_TRADE,
};
pub use sbe::{
    decode_bbo_sbe, decode_trade_sbe, encode_bbo_sbe, encode_trade_sbe, SbeError, SbeMessageHeader,
    BLOCK_LENGTH_BBO, BLOCK_LENGTH_TRADE, SBE_HEADER_LEN, SBE_SCHEMA_ID, SBE_SCHEMA_VERSION,
    TEMPLATE_ID_BBO, TEMPLATE_ID_TRADE, TOTAL_SBE_BBO_LEN, TOTAL_SBE_TRADE_LEN,
};
pub use session::{ClientSession, SessionState, WireProtocol};
pub use websocket::{
    decode_ws_frame_header, encode_ws_frame, unmask_payload, ClientCommand, WebSocketServerEngine,
    WsError, WsFrameHeader, WsOpcode,
};

/// Compact 64-bit Topic Key
/// Layout:
/// - Bits [63..56] (8 bits): Venue ID
/// - Bits [55..32] (24 bits): Asset / Market ID
/// - Bits [31..24] (8 bits): Stream Type (BBO, L2, Trades, Mempool)
/// - Bits [23..0]  (24 bits): Flags / Partition Shard
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct TopicKey(pub u64);

impl TopicKey {
    #[inline(always)]
    pub fn new(venue_id: u8, market_id: u32, stream_type: u8, flags: u32) -> Self {
        let key = ((venue_id as u64) << 56)
            | (((market_id & 0x00FF_FFFF) as u64) << 32)
            | ((stream_type as u64) << 24)
            | ((flags & 0x00FF_FFFF) as u64);
        Self(key)
    }

    #[inline(always)]
    pub fn venue_id(&self) -> u8 {
        (self.0 >> 56) as u8
    }

    #[inline(always)]
    pub fn market_id(&self) -> u32 {
        ((self.0 >> 32) & 0x00FF_FFFF) as u32
    }

    #[inline(always)]
    pub fn stream_type(&self) -> u8 {
        ((self.0 >> 24) & 0xFF) as u8
    }

    #[inline(always)]
    pub fn flags(&self) -> u32 {
        (self.0 & 0x00FF_FFFF) as u32
    }
}

/// Client Filter Predicate for Dynamic Subscription Filtering
#[repr(C, align(32))]
#[derive(Clone, Copy, Debug, Default, PartialEq)]
pub struct ClientFilterPredicate {
    pub min_notional_usd: f64,
    pub max_spread_bps: u32,
    pub min_gas_priority_gwei: u32,
    pub target_prefix: u64, // First 8 bytes of smart contract / token address
}

impl ClientFilterPredicate {
    #[inline(always)]
    pub fn matches_trade(&self, notional_usd: f64) -> bool {
        notional_usd >= self.min_notional_usd
    }

    #[inline(always)]
    pub fn matches_mempool(&self, priority_fee_gwei: u32) -> bool {
        priority_fee_gwei >= self.min_gas_priority_gwei
    }
}

/// Lock-Free Atomic Token Bucket Rate Limiter
///
/// Encodes both the last refill timestamp (upper 40 bits in milliseconds)
/// and remaining token balance (lower 24 bits) in a single AtomicU64 word,
/// guaranteeing zero mutex contention.
pub struct LockFreeTokenBucket {
    state: AtomicU64,
    refill_rate_per_sec: u32,
    burst_capacity: u32,
}

impl LockFreeTokenBucket {
    pub fn new(rate_per_sec: u32, capacity: u32, initial_time_ms: u64) -> Self {
        let state = (initial_time_ms << 24) | (capacity as u64 & 0x00FF_FFFF);
        Self {
            state: AtomicU64::new(state),
            refill_rate_per_sec: rate_per_sec,
            burst_capacity: capacity,
        }
    }

    #[inline(always)]
    pub fn try_consume(&self, tokens: u32, now_ms: u64) -> bool {
        let mut curr = self.state.load(Ordering::Relaxed);

        loop {
            let last_refill_ms = curr >> 24;
            let mut available_tokens = (curr & 0x00FF_FFFF) as u32;

            // Compute elapsed time and refill tokens
            let next_ts = if now_ms > last_refill_ms {
                let elapsed_ms = now_ms - last_refill_ms;
                let refilled = (elapsed_ms * self.refill_rate_per_sec as u64 / 1000) as u32;
                if refilled > 0 {
                    available_tokens = (available_tokens + refilled).min(self.burst_capacity);
                    if available_tokens >= self.burst_capacity {
                        now_ms
                    } else {
                        let refilled_ms =
                            (refilled as u64 * 1000) / self.refill_rate_per_sec as u64;
                        (last_refill_ms + refilled_ms).min(now_ms)
                    }
                } else {
                    last_refill_ms
                }
            } else {
                last_refill_ms
            };

            if available_tokens < tokens {
                return false; // Rate limit exceeded
            }

            let new_tokens = available_tokens - tokens;
            let next = (next_ts << 24) | (new_tokens as u64 & 0x00FF_FFFF);

            match self
                .state
                .compare_exchange_weak(curr, next, Ordering::Release, Ordering::Relaxed)
            {
                Ok(_) => return true,
                Err(actual) => curr = actual,
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_topic_key_packing() {
        let topic = TopicKey::new(1, 42, 2, 0x01);
        assert_eq!(topic.venue_id(), 1);
        assert_eq!(topic.market_id(), 42);
        assert_eq!(topic.stream_type(), 2);
        assert_eq!(topic.flags(), 0x01);
    }

    #[test]
    fn test_lock_free_token_bucket() {
        let bucket = LockFreeTokenBucket::new(100, 10, 1000);
        assert!(bucket.try_consume(5, 1000));
        assert!(bucket.try_consume(5, 1000));
        assert!(!bucket.try_consume(1, 1000)); // Depleted

        // Advance 100ms -> 10 tokens refilled (capped at capacity 10)
        assert!(bucket.try_consume(5, 1100));
    }
}
