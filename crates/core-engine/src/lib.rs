//! # Core Low-Latency Engine
//!
//! Enterprise market data primitives implemented with mechanical sympathy:
//! - Lock-free Single-Producer Single-Consumer (SPSC) ring buffer with 64-byte cache padding.
//! - Contiguous L1d-resident flat array order book price ladder.
//! - Ultra-fast zero-allocation fixed-point scaler.
//! - Vectorized WebSocket unmasking routine.

#![allow(
    clippy::result_unit_err,
    clippy::new_without_default,
    clippy::derivable_impls,
    clippy::needless_range_loop,
    clippy::manual_range_contains,
    clippy::inconsistent_digit_grouping
)]

pub mod kernel_bypass;
pub mod simulator;

pub use kernel_bypass::{
    BypassBackendKind, BypassConfig, BypassMetrics, DmaBufferPool, HwTimestampNs,
    KernelBypassBridge, PacketDescriptor, DEFAULT_RING_CAPACITY, MAX_FRAME_SIZE,
};
pub use simulator::{
    FastPrng, FeedBurstConfig, FeedEventKind, IsolatedConsumerQueue, MockExchangeFeedSimulator,
    PushResult, QueueMetrics, SaturationDropPolicy, SimulatedFeedEvent, SimulatedVenue,
};

use std::cell::UnsafeCell;
use std::sync::atomic::{AtomicU64, Ordering};

/// CPU Cache Line size on modern x86-64 and ARM64 architectures
pub const CACHE_LINE_SIZE: usize = 64;

/// Cache-padded wrapper to eliminate false sharing between cores
#[repr(align(64))]
pub struct CachePadded<T>(pub T);

/// Lock-free Single-Producer Single-Consumer (SPSC) Circular Queue
/// Based on the LMAX Disruptor pattern with acquire-release memory fences.
pub struct SpscRingBuffer<T, const N: usize> {
    producer_cursor: CachePadded<AtomicU64>,
    cached_consumer_cursor: CachePadded<AtomicU64>,

    consumer_cursor: CachePadded<AtomicU64>,
    cached_producer_cursor: CachePadded<AtomicU64>,

    buffer: Box<[UnsafeCell<T>]>,
}

unsafe impl<T: Send, const N: usize> Send for SpscRingBuffer<T, N> {}
unsafe impl<T: Send, const N: usize> Sync for SpscRingBuffer<T, N> {}

impl<T: Copy + Default, const N: usize> SpscRingBuffer<T, N> {
    pub fn new() -> Self {
        assert!(
            N.is_power_of_two(),
            "Buffer capacity must be a power of two"
        );
        let mut vec = Vec::with_capacity(N);
        for _ in 0..N {
            vec.push(UnsafeCell::new(T::default()));
        }
        let buffer = vec.into_boxed_slice();

        Self {
            producer_cursor: CachePadded(AtomicU64::new(0)),
            cached_consumer_cursor: CachePadded(AtomicU64::new(0)),
            consumer_cursor: CachePadded(AtomicU64::new(0)),
            cached_producer_cursor: CachePadded(AtomicU64::new(0)),
            buffer,
        }
    }

    /// Producer: Enqueue item without allocation or blocking
    #[inline(always)]
    pub fn try_push(&self, item: T) -> Result<(), ()> {
        let current_head = self.producer_cursor.0.load(Ordering::Relaxed);
        let cached_tail = self.cached_consumer_cursor.0.load(Ordering::Relaxed);

        // Check if buffer is full using cached consumer sequence
        if current_head >= cached_tail + N as u64 {
            let actual_tail = self.consumer_cursor.0.load(Ordering::Acquire);
            self.cached_consumer_cursor
                .0
                .store(actual_tail, Ordering::Relaxed);
            if current_head >= actual_tail + N as u64 {
                return Err(()); // Buffer Full
            }
        }

        let slot_index = (current_head as usize) & (N - 1);
        unsafe {
            *self.buffer[slot_index].get() = item;
        }

        // Release barrier ensures payload write is visible before cursor increments
        self.producer_cursor
            .0
            .store(current_head + 1, Ordering::Release);
        Ok(())
    }

    /// Consumer: Dequeue item without locks
    #[inline(always)]
    pub fn try_pop(&self) -> Option<T> {
        let current_tail = self.consumer_cursor.0.load(Ordering::Relaxed);
        let cached_head = self.cached_producer_cursor.0.load(Ordering::Relaxed);

        if current_tail >= cached_head {
            let actual_head = self.producer_cursor.0.load(Ordering::Acquire);
            self.cached_producer_cursor
                .0
                .store(actual_head, Ordering::Relaxed);
            if current_tail >= actual_head {
                return None; // Buffer Empty
            }
        }

        let slot_index = (current_tail as usize) & (N - 1);
        let item = unsafe { *self.buffer[slot_index].get() };

        self.consumer_cursor
            .0
            .store(current_tail + 1, Ordering::Release);
        Some(item)
    }
}

// -----------------------------------------------------------------------------
// Contiguous Flat Array Order Book Ladder
// -----------------------------------------------------------------------------

pub const PRICE_LEVELS_COUNT: usize = 2048;
pub const TICK_SIZE: i64 = 100_000; // 0.001 in 10^8 fixed scale

#[repr(C, align(64))]
#[derive(Clone, Copy, Default, Debug, PartialEq, Eq)]
pub struct PriceLevel {
    pub price: i64,    // Scaled 10^8
    pub quantity: u64, // Scaled 10^8
    pub order_count: u32,
    pub last_update_ts: u64, // Hardware nanosecond timestamp
}

#[repr(C, align(64))]
pub struct ContiguousOrderBook {
    pub bids: [PriceLevel; PRICE_LEVELS_COUNT],
    pub asks: [PriceLevel; PRICE_LEVELS_COUNT],
    pub best_bid_idx: usize,
    pub best_ask_idx: usize,
    pub base_price: i64, // Anchor reference price
}

impl ContiguousOrderBook {
    pub fn new(base_price: i64) -> Self {
        Self {
            bids: [PriceLevel::default(); PRICE_LEVELS_COUNT],
            asks: [PriceLevel::default(); PRICE_LEVELS_COUNT],
            best_bid_idx: 0,
            best_ask_idx: PRICE_LEVELS_COUNT - 1,
            base_price,
        }
    }

    #[inline(always)]
    pub fn update_bid(&mut self, price: i64, quantity: u64, order_count: u32, ts: u64) {
        let diff = if price >= self.base_price {
            (price - self.base_price) / TICK_SIZE
        } else {
            (self.base_price - price) / TICK_SIZE
        };
        if diff >= 0 && (diff as usize) < PRICE_LEVELS_COUNT {
            let idx = (diff as usize).min(PRICE_LEVELS_COUNT - 1);
            self.bids[idx] = PriceLevel {
                price,
                quantity,
                order_count,
                last_update_ts: ts,
            };
            if quantity > 0
                && (self.bids[self.best_bid_idx].quantity == 0
                    || price > self.bids[self.best_bid_idx].price)
            {
                self.best_bid_idx = idx;
            } else if quantity == 0 && idx == self.best_bid_idx {
                self.recalculate_best_bid();
            }
        }
    }

    #[inline(always)]
    pub fn update_ask(&mut self, price: i64, quantity: u64, order_count: u32, ts: u64) {
        let diff = if price >= self.base_price {
            (price - self.base_price) / TICK_SIZE
        } else {
            (self.base_price - price) / TICK_SIZE
        };
        if diff >= 0 && (diff as usize) < PRICE_LEVELS_COUNT {
            let idx = (diff as usize).min(PRICE_LEVELS_COUNT - 1);
            self.asks[idx] = PriceLevel {
                price,
                quantity,
                order_count,
                last_update_ts: ts,
            };
            if quantity > 0
                && (self.asks[self.best_ask_idx].quantity == 0
                    || price < self.asks[self.best_ask_idx].price)
            {
                self.best_ask_idx = idx;
            } else if quantity == 0 && idx == self.best_ask_idx {
                self.recalculate_best_ask();
            }
        }
    }

    #[inline(always)]
    fn recalculate_best_bid(&mut self) {
        let mut best_idx = 0;
        let mut best_price = 0;
        for (i, level) in self.bids.iter().enumerate() {
            if level.quantity > 0 && level.price > best_price {
                best_price = level.price;
                best_idx = i;
            }
        }
        self.best_bid_idx = best_idx;
    }

    #[inline(always)]
    fn recalculate_best_ask(&mut self) {
        let mut best_idx = 0;
        let mut best_price = i64::MAX;
        for (i, level) in self.asks.iter().enumerate() {
            if level.quantity > 0 && level.price < best_price {
                best_price = level.price;
                best_idx = i;
            }
        }
        self.best_ask_idx = best_idx;
    }

    #[inline(always)]
    pub fn bbo_bid(&self) -> Option<&PriceLevel> {
        let level = &self.bids[self.best_bid_idx];
        if level.quantity > 0 {
            Some(level)
        } else {
            None
        }
    }

    #[inline(always)]
    pub fn bbo_ask(&self) -> Option<&PriceLevel> {
        let level = &self.asks[self.best_ask_idx];
        if level.quantity > 0 {
            Some(level)
        } else {
            None
        }
    }
}

// -----------------------------------------------------------------------------
// Fixed-Point Scaler (Zero Allocation)
// -----------------------------------------------------------------------------

/// Parses ASCII number string directly to 10^8 fixed-point integer
#[inline(always)]
pub fn parse_fixed_point_8(val: &[u8]) -> i64 {
    let mut result: i64 = 0;
    let mut decimal_places: i32 = -1;
    let mut is_negative = false;
    let mut i = 0;

    if !val.is_empty() && val[0] == b'-' {
        is_negative = true;
        i = 1;
    }

    while i < val.len() {
        let byte = val[i];
        if byte == b'.' {
            decimal_places = 0;
        } else if byte >= b'0' && byte <= b'9' {
            result = result * 10 + (byte - b'0') as i64;
            if decimal_places >= 0 {
                decimal_places += 1;
            }
        }
        i += 1;
    }

    let mut shift = 8 - if decimal_places < 0 {
        0
    } else {
        decimal_places
    };
    while shift > 0 {
        result *= 10;
        shift -= 1;
    }
    while shift < 0 {
        result /= 10;
        shift += 1;
    }

    if is_negative {
        -result
    } else {
        result
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_spsc_ring_buffer() {
        let queue: SpscRingBuffer<u64, 1024> = SpscRingBuffer::new();
        assert!(queue.try_push(42).is_ok());
        assert_eq!(queue.try_pop(), Some(42));
        assert_eq!(queue.try_pop(), None);
    }

    #[test]
    fn test_fixed_point_parser() {
        assert_eq!(parse_fixed_point_8(b"123.45"), 12345000000);
        assert_eq!(parse_fixed_point_8(b"0.00000001"), 1);
        assert_eq!(parse_fixed_point_8(b"65000"), 6500000000000);
    }
}
