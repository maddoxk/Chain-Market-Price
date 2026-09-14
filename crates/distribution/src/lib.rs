//! # Distribution & Fan-Out Egress Layer
//!
//! Provides ultra-low latency client fan-out primitives:
//! - 64-bit Topic Key packing for branchless dispatch.
//! - Lock-free token bucket rate limiting using single-word 64-bit CAS.
//! - Dynamic filter predicate definitions.

use std::sync::atomic::{AtomicU64, Ordering};

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
}

/// Client Filter Predicate for Dynamic Subscription Filtering
#[repr(C, align(32))]
#[derive(Clone, Copy, Debug, Default)]
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
            if now_ms > last_refill_ms {
                let elapsed_ms = now_ms - last_refill_ms;
                let refilled = (elapsed_ms * self.refill_rate_per_sec as u64 / 1000) as u32;
                available_tokens = (available_tokens + refilled).min(self.burst_capacity);
            }

            if available_tokens < tokens {
                return false; // Rate limit exceeded
            }

            let new_tokens = available_tokens - tokens;
            let next = (now_ms << 24) | (new_tokens as u64 & 0x00FF_FFFF);

            match self.state.compare_exchange_weak(
                curr,
                next,
                Ordering::Release,
                Ordering::Relaxed,
            ) {
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
        let topic = TopicKey::new(1, 42, 2, 0);
        assert_eq!(topic.venue_id(), 1);
        assert_eq!(topic.market_id(), 42);
        assert_eq!(topic.stream_type(), 2);
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
