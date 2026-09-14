//! # High-Throughput Mock Exchange Feed Simulator & Stress Test Harness
//!
//! Subsystem for generating institutional-grade market data feeds at micro-burst rates
//! (100k – 500k+ messages/sec) with realistic price action, nanosecond jitter injection,
//! and slow consumer backpressure isolation via configurable queue saturation drop policies.
//!
//! Mechanical Sympathy Guarantees:
//! - Zero dynamic heap allocation in steady-state generation and dispatch loops.
//! - 64-byte cache line alignment for all feed event buffers and ring buffer cursors.
//! - Lock-free concurrency with acquire-release memory fences.

use std::cell::UnsafeCell;
use std::sync::atomic::{AtomicU64, Ordering};
use ingest_models::{ChainId, NormalizedBbo, TelemetryTimestamps, UnifiedTrade, VenueId};
use crate::CachePadded;

/// Fast pseudo-random number generator (XorShift64Star)
/// Guaranteed zero-allocation and sub-nanosecond cycle execution time.
#[derive(Debug, Clone)]
pub struct FastPrng {
    state: u64,
}

impl FastPrng {
    #[inline(always)]
    pub fn new(seed: u64) -> Self {
        let s = if seed == 0 { 0xdead_beef_cafe_babe } else { seed };
        Self { state: s }
    }

    #[inline(always)]
    pub fn next_u64(&mut self) -> u64 {
        let mut x = self.state;
        x ^= x >> 12;
        x ^= x << 25;
        x ^= x >> 27;
        self.state = x;
        x.wrapping_mul(0x2545_f491_4f6c_dd1d)
    }

    /// Uniform integer in range [min, max]
    #[inline(always)]
    pub fn gen_range(&mut self, min: u64, max: u64) -> u64 {
        if min >= max {
            return min;
        }
        min + (self.next_u64() % (max - min + 1))
    }

    /// Symmetric price delta in fixed scale (10^8)
    #[inline(always)]
    pub fn gen_price_delta(&mut self, max_tick_jump: i64) -> i64 {
        let raw = (self.next_u64() % ((max_tick_jump * 2 + 1) as u64)) as i64;
        raw - max_tick_jump
    }
}

/// Supported Simulated Market Venues
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SimulatedVenue {
    BinanceCeFi,
    OkxCeFi,
    UniswapV3DeFi,
}

/// Tagged Simulated Market Event
#[repr(u8)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum FeedEventKind {
    Bbo = 1,
    Trade = 2,
}

impl Default for FeedEventKind {
    fn default() -> Self {
        FeedEventKind::Bbo
    }
}

/// 64-Byte Cache-Aligned Normalized Feed Event
#[repr(C, align(64))]
#[derive(Clone, Copy, Default, Debug, PartialEq, Eq)]
pub struct SimulatedFeedEvent {
    pub kind: FeedEventKind,
    pub venue_id: u16,
    pub market_id: u32,
    pub bbo: NormalizedBbo,
    pub trade: UnifiedTrade,
}

impl SimulatedFeedEvent {
    #[inline(always)]
    pub fn from_bbo(bbo: NormalizedBbo) -> Self {
        Self {
            kind: FeedEventKind::Bbo,
            venue_id: bbo.venue,
            market_id: bbo.market_id,
            bbo,
            trade: UnifiedTrade::default(),
        }
    }

    #[inline(always)]
    pub fn from_trade(trade: UnifiedTrade) -> Self {
        Self {
            kind: FeedEventKind::Trade,
            venue_id: trade.venue,
            market_id: trade.market_id,
            bbo: NormalizedBbo::default(),
            trade,
        }
    }
}

/// Queue Saturation Drop Policy for Slow Consumer Backpressure Isolation
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SaturationDropPolicy {
    /// Drop incoming newest item if ring buffer is saturated.
    /// Preserves client consumption latency while recording drop telemetry.
    DropNewest,
    /// Evict oldest unconsumed item and write newest item.
    /// Keeps lagging consumer pinned to the freshest real-time market front.
    DropOldest,
}

/// Result of pushing to an isolated consumer queue
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum PushResult {
    Enqueued,
    DroppedNewest,
    EvictedOldest,
}

/// Atomic Telemetry Metrics for Queue Saturation and Backpressure Isolation
pub struct QueueMetrics {
    pub total_enqueued: AtomicU64,
    pub total_dropped: AtomicU64,
    pub total_evicted: AtomicU64,
    pub total_consumed: AtomicU64,
    pub saturation_events: AtomicU64,
}

impl Default for QueueMetrics {
    fn default() -> Self {
        Self::new()
    }
}

impl QueueMetrics {
    pub fn new() -> Self {
        Self {
            total_enqueued: AtomicU64::new(0),
            total_dropped: AtomicU64::new(0),
            total_evicted: AtomicU64::new(0),
            total_consumed: AtomicU64::new(0),
            saturation_events: AtomicU64::new(0),
        }
    }
}

/// Lock-Free Single-Producer Single-Consumer Isolated Consumer Queue
//!
/// Implements configurable saturation drop policies so a slow or stalled
/// consumer never blocks the producer or induces backpressure on peer consumers.
pub struct IsolatedConsumerQueue<T, const N: usize> {
    producer_cursor: CachePadded<AtomicU64>,
    cached_consumer_cursor: CachePadded<UnsafeCell<u64>>,

    consumer_cursor: CachePadded<AtomicU64>,
    cached_producer_cursor: CachePadded<UnsafeCell<u64>>,

    buffer: Box<[UnsafeCell<T>]>,
    pub metrics: QueueMetrics,
}

unsafe impl<T: Send, const N: usize> Send for IsolatedConsumerQueue<T, N> {}
unsafe impl<T: Send, const N: usize> Sync for IsolatedConsumerQueue<T, N> {}

impl<T: Copy + Default, const N: usize> IsolatedConsumerQueue<T, N> {
    pub fn new() -> Self {
        assert!(N.is_power_of_two(), "Queue capacity must be a power of two");
        let mut vec = Vec::with_capacity(N);
        for _ in 0..N {
            vec.push(UnsafeCell::new(T::default()));
        }
        let buffer = vec.into_boxed_slice();

        Self {
            producer_cursor: CachePadded(AtomicU64::new(0)),
            cached_consumer_cursor: CachePadded(UnsafeCell::new(0)),
            consumer_cursor: CachePadded(AtomicU64::new(0)),
            cached_producer_cursor: CachePadded(UnsafeCell::new(0)),
            buffer,
            metrics: QueueMetrics::new(),
        }
    }

    /// Push an item with the configured saturation policy.
    /// Never blocks the caller regardless of consumer speed.
    #[inline(always)]
    pub fn push_with_policy(&self, item: T, policy: SaturationDropPolicy) -> PushResult {
        let current_head = self.producer_cursor.0.load(Ordering::Relaxed);
        let cached_tail = unsafe { *self.cached_consumer_cursor.0.get() };

        // Fast path check: capacity available
        if current_head < cached_tail + N as u64 {
            let slot = (current_head as usize) & (N - 1);
            unsafe {
                *self.buffer[slot].get() = item;
            }
            self.producer_cursor.0.store(current_head + 1, Ordering::Release);
            self.metrics.total_enqueued.fetch_add(1, Ordering::Relaxed);
            return PushResult::Enqueued;
        }

        // Potential saturation: reload actual consumer cursor with acquire
        let actual_tail = self.consumer_cursor.0.load(Ordering::Acquire);
        unsafe { *self.cached_consumer_cursor.0.get() = actual_tail };

        if current_head < actual_tail + N as u64 {
            let slot = (current_head as usize) & (N - 1);
            unsafe {
                *self.buffer[slot].get() = item;
            }
            self.producer_cursor.0.store(current_head + 1, Ordering::Release);
            self.metrics.total_enqueued.fetch_add(1, Ordering::Relaxed);
            return PushResult::Enqueued;
        }

        // Queue is fully saturated! Apply drop policy
        self.metrics.saturation_events.fetch_add(1, Ordering::Relaxed);

        match policy {
            SaturationDropPolicy::DropNewest => {
                self.metrics.total_dropped.fetch_add(1, Ordering::Relaxed);
                PushResult::DroppedNewest
            }
            SaturationDropPolicy::DropOldest => {
                // Evict oldest unconsumed item by forcefully advancing consumer tail
                let new_tail = actual_tail + 1;
                self.consumer_cursor.0.store(new_tail, Ordering::Release);
                unsafe { *self.cached_consumer_cursor.0.get() = new_tail };

                let slot = (current_head as usize) & (N - 1);
                unsafe {
                    *self.buffer[slot].get() = item;
                }
                self.producer_cursor.0.store(current_head + 1, Ordering::Release);
                self.metrics.total_evicted.fetch_add(1, Ordering::Relaxed);
                PushResult::EvictedOldest
            }
        }
    }

    /// Consumer pop: non-blocking dequeue
    #[inline(always)]
    pub fn try_pop(&self) -> Option<T> {
        let current_tail = self.consumer_cursor.0.load(Ordering::Relaxed);
        let cached_head = unsafe { *self.cached_producer_cursor.0.get() };

        if current_tail >= cached_head {
            let actual_head = self.producer_cursor.0.load(Ordering::Acquire);
            unsafe { *self.cached_producer_cursor.0.get() = actual_head };
            if current_tail >= actual_head {
                return None;
            }
        }

        let slot = (current_tail as usize) & (N - 1);
        let item = unsafe { *self.buffer[slot].get() };

        self.consumer_cursor.0.store(current_tail + 1, Ordering::Release);
        self.metrics.total_consumed.fetch_add(1, Ordering::Relaxed);
        Some(item)
    }

    #[inline(always)]
    pub fn is_empty(&self) -> bool {
        let tail = self.consumer_cursor.0.load(Ordering::Relaxed);
        let head = self.producer_cursor.0.load(Ordering::Relaxed);
        tail >= head
    }

    #[inline(always)]
    pub fn len(&self) -> usize {
        let head = self.producer_cursor.0.load(Ordering::Relaxed);
        let tail = self.consumer_cursor.0.load(Ordering::Relaxed);
        head.saturating_sub(tail) as usize
    }
}

/// Configuration for Realistic Micro-Burst Generation
#[derive(Clone, Copy, Debug)]
pub struct FeedBurstConfig {
    /// Target nominal rate in messages per second
    pub target_msg_rate_per_sec: u64,
    /// Number of market updates generated in each continuous micro-burst
    pub burst_size: usize,
    /// Microseconds between burst batches
    pub burst_interval_micros: u64,
    /// Minimum injected jitter in nanoseconds
    pub jitter_min_ns: u64,
    /// Maximum injected jitter in nanoseconds
    pub jitter_max_ns: u64,
    /// Base reference price for BTC/USDT (scaled 10^8)
    pub base_btc_price: i64,
    /// Base reference price for ETH/USDT (scaled 10^8)
    pub base_eth_price: i64,
    /// Base reference price for SOL/USDT (scaled 10^8)
    pub base_sol_price: i64,
}

impl Default for FeedBurstConfig {
    fn default() -> Self {
        Self {
            target_msg_rate_per_sec: 250_000,
            burst_size: 1_000,
            burst_interval_micros: 4_000,
            jitter_min_ns: 200,
            jitter_max_ns: 3_500,
            base_btc_price: 65_000_00000000,
            base_eth_price: 3_500_00000000,
            base_sol_price: 150_00000000,
        }
    }
}

/// High-Throughput Realistic Exchange Feed Simulator
pub struct MockExchangeFeedSimulator {
    prng: FastPrng,
    btc_cur_price: i64,
    eth_cur_price: i64,
    sol_cur_price: i64,
    binance_seq: u64,
    okx_seq: u64,
    uniswap_seq: u64,
    trade_id_counter: u64,
}

impl MockExchangeFeedSimulator {
    pub fn new(seed: u64, config: &FeedBurstConfig) -> Self {
        Self {
            prng: FastPrng::new(seed),
            btc_cur_price: config.base_btc_price,
            eth_cur_price: config.base_eth_price,
            sol_cur_price: config.base_sol_price,
            binance_seq: 100_000_000,
            okx_seq: 200_000_000,
            uniswap_seq: 1_000_000,
            trade_id_counter: 50_000_000,
        }
    }

    /// Generates a micro-burst batch into pre-allocated memory slice without dynamic heap allocations.
    /// Returns the number of events written into `out_buffer`.
    pub fn generate_burst(
        &mut self,
        config: &FeedBurstConfig,
        out_buffer: &mut [SimulatedFeedEvent],
        base_timestamp_ns: u64,
    ) -> usize {
        let count = config.burst_size.min(out_buffer.len());

        for i in 0..count {
            // Select venue: 50% Binance, 30% OKX, 20% UniswapV3
            let venue_choice = self.prng.next_u64() % 10;
            // Select asset: 50% BTC, 35% ETH, 15% SOL
            let asset_choice = self.prng.next_u64() % 100;
            // Select event type: 85% BBO, 15% Trade
            let is_trade = (self.prng.next_u64() % 100) < 15;

            let (market_id, mut base_price, tick_jump) = if asset_choice < 50 {
                (1001, self.btc_cur_price, 50_0000) // BTC/USDT, $0.50 tick jump
            } else if asset_choice < 85 {
                (1002, self.eth_cur_price, 10_0000) // ETH/USDT, $0.10 tick jump
            } else {
                (1003, self.sol_cur_price, 1_0000)  // SOL/USDT, $0.01 tick jump
            };

            // Random price walk
            let delta = self.prng.gen_price_delta(tick_jump);
            base_price = (base_price + delta).max(1_00000000);

            // Update persistent price state
            if market_id == 1001 {
                self.btc_cur_price = base_price;
            } else if market_id == 1002 {
                self.eth_cur_price = base_price;
            } else {
                self.sol_cur_price = base_price;
            }

            // Injected simulated jitter
            let jitter_ns = self.prng.gen_range(config.jitter_min_ns, config.jitter_max_ns);
            let t0_exchange = base_timestamp_ns + (i as u64 * 10);
            let t1_nic = t0_exchange + jitter_ns;
            let t2_engine = t1_nic + self.prng.gen_range(500, 1_500); // 0.5-1.5 µs engine time
            let t3_egress = t2_engine + self.prng.gen_range(200, 800);  // 0.2-0.8 µs wire egress

            let telemetry = TelemetryTimestamps {
                t0_exchange_ns: t0_exchange,
                t1_ingest_nic_ns: t1_nic,
                t2_engine_proc_ns: t2_engine,
                t3_egress_ns: t3_egress,
            };

            let event = if venue_choice < 5 {
                // Binance (CeFi)
                self.binance_seq += 1;
                if is_trade {
                    self.trade_id_counter += 1;
                    let side = if (self.prng.next_u64() & 1) == 0 { 1 } else { 2 };
                    let size = self.prng.gen_range(1_00000000, 25_00000000);
                    SimulatedFeedEvent::from_trade(UnifiedTrade {
                        telemetry,
                        market_id,
                        venue: VenueId::Binance as u16,
                        chain: ChainId::OffChain as u16,
                        sequence: self.binance_seq,
                        trade_id: self.trade_id_counter,
                        price: base_price,
                        size,
                        side,
                        is_liquidation: false,
                    })
                } else {
                    let spread_bps = self.prng.gen_range(1, 3) as u16; // 1-3 bps tight spread
                    let half_spread = (base_price * spread_bps as i64) / 20_000;
                    let bid_price = base_price - half_spread;
                    let ask_price = base_price + half_spread;
                    let bid_qty = self.prng.gen_range(5_00000000, 50_00000000);
                    let ask_qty = self.prng.gen_range(5_00000000, 50_00000000);
                    SimulatedFeedEvent::from_bbo(NormalizedBbo {
                        telemetry,
                        market_id,
                        venue: VenueId::Binance as u16,
                        chain: ChainId::OffChain as u16,
                        sequence: self.binance_seq,
                        bid_price,
                        bid_qty,
                        ask_price,
                        ask_qty,
                        spread_bps,
                        flags: 0,
                    })
                }
            } else if venue_choice < 8 {
                // OKX (CeFi)
                self.okx_seq += 1;
                if is_trade {
                    self.trade_id_counter += 1;
                    let side = if (self.prng.next_u64() & 1) == 0 { 1 } else { 2 };
                    let size = self.prng.gen_range(50000000, 15_00000000);
                    SimulatedFeedEvent::from_trade(UnifiedTrade {
                        telemetry,
                        market_id,
                        venue: VenueId::Okx as u16,
                        chain: ChainId::OffChain as u16,
                        sequence: self.okx_seq,
                        trade_id: self.trade_id_counter,
                        price: base_price,
                        size,
                        side,
                        is_liquidation: false,
                    })
                } else {
                    let spread_bps = self.prng.gen_range(2, 5) as u16;
                    let half_spread = (base_price * spread_bps as i64) / 20_000;
                    let bid_price = base_price - half_spread;
                    let ask_price = base_price + half_spread;
                    let bid_qty = self.prng.gen_range(2_00000000, 30_00000000);
                    let ask_qty = self.prng.gen_range(2_00000000, 30_00000000);
                    SimulatedFeedEvent::from_bbo(NormalizedBbo {
                        telemetry,
                        market_id,
                        venue: VenueId::Okx as u16,
                        chain: ChainId::OffChain as u16,
                        sequence: self.okx_seq,
                        bid_price,
                        bid_qty,
                        ask_price,
                        ask_qty,
                        spread_bps,
                        flags: 0,
                    })
                }
            } else {
                // UniswapV3 (DeFi)
                self.uniswap_seq += 1;
                let spread_bps = self.prng.gen_range(5, 15) as u16; // 5-15 bps AMM spread
                let half_spread = (base_price * spread_bps as i64) / 20_000;
                let bid_price = base_price - half_spread;
                let ask_price = base_price + half_spread;
                let pool_liquidity = self.prng.gen_range(50_00000000, 200_00000000);
                SimulatedFeedEvent::from_bbo(NormalizedBbo {
                    telemetry,
                    market_id,
                    venue: VenueId::UniswapV3 as u16,
                    chain: ChainId::Ethereum as u16,
                    sequence: self.uniswap_seq,
                    bid_price,
                    bid_qty: pool_liquidity,
                    ask_price,
                    ask_qty: pool_liquidity,
                    spread_bps,
                    flags: 0x01, // Synthetic AMM flag
                })
            };

            out_buffer[i] = event;
        }

        count
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_prng_deterministic() {
        let mut prng1 = FastPrng::new(42);
        let mut prng2 = FastPrng::new(42);
        for _ in 0..1000 {
            assert_eq!(prng1.next_u64(), prng2.next_u64());
        }
    }

    #[test]
    fn test_feed_generator_burst() {
        let config = FeedBurstConfig::default();
        let mut sim = MockExchangeFeedSimulator::new(1337, &config);
        let mut buf = [SimulatedFeedEvent::default(); 1000];

        let generated = sim.generate_burst(&config, &mut buf, 1_700_000_000_000_000_000);
        assert_eq!(generated, 1000);

        let first = buf[0];
        assert!(first.market_id >= 1001 && first.market_id <= 1003);
        assert!(first.venue_id == VenueId::Binance as u16
            || first.venue_id == VenueId::Okx as u16
            || first.venue_id == VenueId::UniswapV3 as u16);
    }

    #[test]
    fn test_saturation_policy_drop_newest() {
        let queue: IsolatedConsumerQueue<u64, 4> = IsolatedConsumerQueue::new();

        // Enqueue 4 items to fill buffer
        assert_eq!(queue.push_with_policy(1, SaturationDropPolicy::DropNewest), PushResult::Enqueued);
        assert_eq!(queue.push_with_policy(2, SaturationDropPolicy::DropNewest), PushResult::Enqueued);
        assert_eq!(queue.push_with_policy(3, SaturationDropPolicy::DropNewest), PushResult::Enqueued);
        assert_eq!(queue.push_with_policy(4, SaturationDropPolicy::DropNewest), PushResult::Enqueued);

        // 5th item: buffer full, should drop newest
        assert_eq!(queue.push_with_policy(5, SaturationDropPolicy::DropNewest), PushResult::DroppedNewest);
        assert_eq!(queue.metrics.total_dropped.load(Ordering::Relaxed), 1);
        assert_eq!(queue.metrics.saturation_events.load(Ordering::Relaxed), 1);

        // Consumer drains original 4 items
        assert_eq!(queue.try_pop(), Some(1));
        assert_eq!(queue.try_pop(), Some(2));
        assert_eq!(queue.try_pop(), Some(3));
        assert_eq!(queue.try_pop(), Some(4));
        assert_eq!(queue.try_pop(), None);
    }

    #[test]
    fn test_saturation_policy_drop_oldest() {
        let queue: IsolatedConsumerQueue<u64, 4> = IsolatedConsumerQueue::new();

        assert_eq!(queue.push_with_policy(10, SaturationDropPolicy::DropOldest), PushResult::Enqueued);
        assert_eq!(queue.push_with_policy(20, SaturationDropPolicy::DropOldest), PushResult::Enqueued);
        assert_eq!(queue.push_with_policy(30, SaturationDropPolicy::DropOldest), PushResult::Enqueued);
        assert_eq!(queue.push_with_policy(40, SaturationDropPolicy::DropOldest), PushResult::Enqueued);

        // 5th item: buffer full, should evict oldest (10) and write 50
        assert_eq!(queue.push_with_policy(50, SaturationDropPolicy::DropOldest), PushResult::EvictedOldest);
        assert_eq!(queue.metrics.total_evicted.load(Ordering::Relaxed), 1);

        // 6th item: evict 20 and write 60
        assert_eq!(queue.push_with_policy(60, SaturationDropPolicy::DropOldest), PushResult::EvictedOldest);
        assert_eq!(queue.metrics.total_evicted.load(Ordering::Relaxed), 2);

        // Consumer now reads 30, 40, 50, 60
        assert_eq!(queue.try_pop(), Some(30));
        assert_eq!(queue.try_pop(), Some(40));
        assert_eq!(queue.try_pop(), Some(50));
        assert_eq!(queue.try_pop(), Some(60));
        assert_eq!(queue.try_pop(), None);
    }
}
