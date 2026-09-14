//! # Client Connection & Subscription Session State
//!
//! Encapsulates per-client session lifecycle, wire protocol negotiation (SBE / JSON),
//! dynamic filter predicate configuration, lock-free rate limiting, and ping/pong heartbeat tracking.

use std::sync::atomic::{AtomicU64, AtomicU8, Ordering};
use crate::{ClientFilterPredicate, LockFreeTokenBucket};

/// Client negotiated wire protocol encoding
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum WireProtocol {
    /// Simple Binary Encoding (Ultra-low latency, zero-copy)
    Sbe,
    /// Fast SIMD-friendly JSON (Browser dashboards, standard consumers)
    Json,
}

/// Client connection lifecycle states
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[repr(u8)]
pub enum SessionState {
    Disconnected = 0,
    Connected = 1,
    Subscribed = 2,
    Closed = 3,
}

impl From<u8> for SessionState {
    #[inline(always)]
    fn from(val: u8) -> Self {
        match val {
            1 => SessionState::Connected,
            2 => SessionState::Subscribed,
            3 => SessionState::Closed,
            _ => SessionState::Disconnected,
        }
    }
}

/// Cache-aligned per-client session metadata and state
#[repr(C, align(64))]
pub struct ClientSession {
    pub id: u32,
    pub protocol: WireProtocol,
    state: AtomicU8,
    pub rate_limiter: LockFreeTokenBucket,
    pub filter: ClientFilterPredicate,
    pub last_ping_ts_ns: AtomicU64,
    pub last_pong_ts_ns: AtomicU64,
    pub messages_sent: AtomicU64,
    pub bytes_sent: AtomicU64,
    pub dropped_rate_limit: AtomicU64,
}

impl ClientSession {
    pub fn new(
        id: u32,
        protocol: WireProtocol,
        rate_per_sec: u32,
        burst_capacity: u32,
        now_ms: u64,
    ) -> Self {
        Self {
            id,
            protocol,
            state: AtomicU8::new(SessionState::Connected as u8),
            rate_limiter: LockFreeTokenBucket::new(rate_per_sec, burst_capacity, now_ms),
            filter: ClientFilterPredicate::default(),
            last_ping_ts_ns: AtomicU64::new(0),
            last_pong_ts_ns: AtomicU64::new(0),
            messages_sent: AtomicU64::new(0),
            bytes_sent: AtomicU64::new(0),
            dropped_rate_limit: AtomicU64::new(0),
        }
    }

    pub fn with_filter(mut self, filter: ClientFilterPredicate) -> Self {
        self.filter = filter;
        self
    }

    #[inline(always)]
    pub fn state(&self) -> SessionState {
        self.state.load(Ordering::Acquire).into()
    }

    #[inline(always)]
    pub fn set_state(&self, new_state: SessionState) {
        self.state.store(new_state as u8, Ordering::Release);
    }

    #[inline(always)]
    pub fn is_active(&self) -> bool {
        let s = self.state();
        s == SessionState::Connected || s == SessionState::Subscribed
    }

    #[inline(always)]
    pub fn record_ping(&self, ts_ns: u64) {
        self.last_ping_ts_ns.store(ts_ns, Ordering::Relaxed);
    }

    #[inline(always)]
    pub fn record_pong(&self, ts_ns: u64) {
        self.last_pong_ts_ns.store(ts_ns, Ordering::Relaxed);
    }

    #[inline(always)]
    pub fn is_heartbeat_healthy(&self, now_ns: u64, timeout_ns: u64) -> bool {
        let last_pong = self.last_pong_ts_ns.load(Ordering::Relaxed);
        if last_pong == 0 {
            // No pong received yet, check ping
            let last_ping = self.last_ping_ts_ns.load(Ordering::Relaxed);
            last_ping == 0 || now_ns.saturating_sub(last_ping) <= timeout_ns
        } else {
            now_ns.saturating_sub(last_pong) <= timeout_ns
        }
    }

    #[inline(always)]
    pub fn try_consume_token(&self, tokens: u32, now_ms: u64) -> bool {
        let allowed = self.rate_limiter.try_consume(tokens, now_ms);
        if !allowed {
            self.dropped_rate_limit.fetch_add(1, Ordering::Relaxed);
        }
        allowed
    }

    #[inline(always)]
    pub fn record_delivery(&self, bytes: usize) {
        self.messages_sent.fetch_add(1, Ordering::Relaxed);
        self.bytes_sent.fetch_add(bytes as u64, Ordering::Relaxed);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_client_session_lifecycle() {
        let session = ClientSession::new(1, WireProtocol::Sbe, 1000, 50, 1000);
        assert_eq!(session.state(), SessionState::Connected);
        assert!(session.is_active());

        session.set_state(SessionState::Subscribed);
        assert_eq!(session.state(), SessionState::Subscribed);
        assert!(session.is_active());

        session.set_state(SessionState::Closed);
        assert_eq!(session.state(), SessionState::Closed);
        assert!(!session.is_active());
    }

    #[test]
    fn test_client_session_heartbeat() {
        let session = ClientSession::new(1, WireProtocol::Json, 1000, 50, 1000);
        session.record_ping(1_000_000_000);
        session.record_pong(1_000_000_500);

        assert!(session.is_heartbeat_healthy(1_000_001_000, 10_000_000));
        assert!(!session.is_heartbeat_healthy(2_000_000_000, 10_000_000));
    }

    #[test]
    fn test_client_session_delivery_accounting() {
        let session = ClientSession::new(42, WireProtocol::Sbe, 100, 10, 0);
        session.record_delivery(92);
        session.record_delivery(92);

        assert_eq!(session.messages_sent.load(Ordering::Relaxed), 2);
        assert_eq!(session.bytes_sent.load(Ordering::Relaxed), 184);
    }
}
