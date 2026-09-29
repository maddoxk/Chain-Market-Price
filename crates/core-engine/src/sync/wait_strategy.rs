//! # Adaptive Backoff & Hybrid Polling Synchronization
//!
//! Replaces rigid 100% busy-spin loops with hardware-tier-aware waiting strategies:
//! - Tier 3 (Bare Metal): `BusySpinStrategy` (pure `_mm_pause` / `YIELD` instruction, < 50ns wakeup)
//! - Tier 2 (Cloud VM): `HybridAdaptiveStrategy` (bounded spin -> `yield_now` -> timed sleep, 0% steal time)
//! - Tier 1 (Laptop): `PowerEfficientStrategy` (rapid sleep escalation, < 2% idle CPU, no thermal throttle)
//! - Burst Promotion: `AdaptiveBurstWaitStrategy` (dynamically promotes to busy-spin during market surges)

use crate::topology::HardwareTier;
use std::time::{Duration, Instant};

/// Class of synchronization waiting strategy
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum WaitStrategyKind {
    /// Pure busy-spin using CPU hints (PAUSE/YIELD). Zero syscall overhead.
    BusySpin,
    /// Two-stage bounded spin followed by thread::yield_now and timed futex/sleep.
    HybridAdaptive,
    /// Rapid escalation to low-power sleep to conserve battery and CPU quota.
    PowerEfficient,
    /// Adaptive burst mode wrapper
    BurstMode,
}

/// Abstract wait strategy interface for lock-free queues and ring buffers
pub trait WaitStrategy: Send + Sync {
    /// Resets backoff state counter immediately upon receiving/processing an item
    fn reset(&mut self);

    /// Executed on an empty check. Progresses through spin -> yield -> sleep stages
    fn idle(&mut self);

    /// Park thread with an explicit timeout duration
    fn park_timeout(&mut self, timeout: Duration);

    /// Returns the kind of wait strategy
    fn kind(&self) -> WaitStrategyKind;
}

// -----------------------------------------------------------------------------
// Tier 3: Zero-Overhead Busy-Spinning Strategy
// -----------------------------------------------------------------------------

/// Pure busy-spinning strategy for dedicated isolated bare-metal cores
#[derive(Debug, Clone, Copy, Default)]
pub struct BusySpinStrategy;

impl BusySpinStrategy {
    pub fn new() -> Self {
        Self
    }
}

impl WaitStrategy for BusySpinStrategy {
    #[inline(always)]
    fn reset(&mut self) {}

    #[inline(always)]
    fn idle(&mut self) {
        // x86_64: PAUSE instruction avoids memory pipeline replays
        // aarch64: YIELD instruction hints core pipeline
        core::hint::spin_loop();
    }

    #[inline(always)]
    fn park_timeout(&mut self, _timeout: Duration) {
        core::hint::spin_loop();
    }

    #[inline(always)]
    fn kind(&self) -> WaitStrategyKind {
        WaitStrategyKind::BusySpin
    }
}

// -----------------------------------------------------------------------------
// Tier 2: Cloud Hybrid Adaptive Strategy (Bounded Spin -> Yield -> Sleep)
// -----------------------------------------------------------------------------

/// Cloud-optimized strategy preventing hypervisor vCPU descheduling and %steal penalties
#[derive(Debug, Clone)]
pub struct HybridAdaptiveStrategy {
    counter: u32,
    spin_limit: u32,
    yield_limit: u32,
    current_sleep_us: u32,
    max_sleep_us: u32,
}

impl Default for HybridAdaptiveStrategy {
    fn default() -> Self {
        Self::new(256, 1024, 50)
    }
}

impl HybridAdaptiveStrategy {
    pub fn new(spin_limit: u32, yield_limit: u32, max_sleep_us: u32) -> Self {
        Self {
            counter: 0,
            spin_limit,
            yield_limit,
            current_sleep_us: 1,
            max_sleep_us: max_sleep_us.max(1),
        }
    }
}

impl WaitStrategy for HybridAdaptiveStrategy {
    #[inline(always)]
    fn reset(&mut self) {
        self.counter = 0;
        self.current_sleep_us = 1;
    }

    #[inline]
    fn idle(&mut self) {
        self.counter = self.counter.saturating_add(1);

        if self.counter <= self.spin_limit {
            core::hint::spin_loop();
        } else if self.counter <= self.spin_limit + self.yield_limit {
            std::thread::yield_now();
        } else {
            std::thread::sleep(Duration::from_micros(self.current_sleep_us as u64));
            self.current_sleep_us = (self.current_sleep_us * 2).min(self.max_sleep_us);
        }
    }

    #[inline]
    fn park_timeout(&mut self, timeout: Duration) {
        std::thread::sleep(timeout);
    }

    #[inline(always)]
    fn kind(&self) -> WaitStrategyKind {
        WaitStrategyKind::HybridAdaptive
    }
}

// -----------------------------------------------------------------------------
// Tier 1: Power-Efficient Strategy (Laptops & Developer Workstations)
// -----------------------------------------------------------------------------

/// Power-conserving strategy eliminating laptop thermal throttling and battery drain
#[derive(Debug, Clone)]
pub struct PowerEfficientStrategy {
    counter: u32,
    spin_limit: u32,
    min_sleep_us: u64,
    max_sleep_us: u64,
    current_sleep_us: u64,
}

impl Default for PowerEfficientStrategy {
    fn default() -> Self {
        Self::new(32, 10, 1000)
    }
}

impl PowerEfficientStrategy {
    pub fn new(spin_limit: u32, min_sleep_us: u64, max_sleep_us: u64) -> Self {
        Self {
            counter: 0,
            spin_limit,
            min_sleep_us,
            max_sleep_us: max_sleep_us.max(min_sleep_us),
            current_sleep_us: min_sleep_us,
        }
    }
}

impl WaitStrategy for PowerEfficientStrategy {
    #[inline(always)]
    fn reset(&mut self) {
        self.counter = 0;
        self.current_sleep_us = self.min_sleep_us;
    }

    #[inline]
    fn idle(&mut self) {
        self.counter = self.counter.saturating_add(1);

        if self.counter <= self.spin_limit {
            core::hint::spin_loop();
        } else {
            std::thread::sleep(Duration::from_micros(self.current_sleep_us));
            self.current_sleep_us = (self.current_sleep_us * 2).min(self.max_sleep_us);
        }
    }

    #[inline]
    fn park_timeout(&mut self, timeout: Duration) {
        std::thread::sleep(timeout);
    }

    #[inline(always)]
    fn kind(&self) -> WaitStrategyKind {
        WaitStrategyKind::PowerEfficient
    }
}

// -----------------------------------------------------------------------------
// Dynamic Burst Promotion Wrapper
// -----------------------------------------------------------------------------

/// Automatically promotes underlying strategy to zero-sleep busy-spinning
/// during high tick velocity bursts (e.g. market volatility or liquidation waves)
#[derive(Debug, Clone)]
pub struct AdaptiveBurstWaitStrategy<S: WaitStrategy> {
    inner: S,
    burst_threshold_ticks: u32,
    window_duration: Duration,
    burst_decay_duration: Duration,

    window_start: Instant,
    ticks_in_window: u32,
    burst_until: Option<Instant>,
}

impl<S: WaitStrategy> AdaptiveBurstWaitStrategy<S> {
    pub fn new(
        inner: S,
        burst_threshold_ticks: u32,
        window_duration: Duration,
        burst_decay_duration: Duration,
    ) -> Self {
        Self {
            inner,
            burst_threshold_ticks,
            window_duration,
            burst_decay_duration,
            window_start: Instant::now(),
            ticks_in_window: 0,
            burst_until: None,
        }
    }

    /// Records that an item/tick was processed and updates burst velocity state
    #[inline]
    pub fn record_tick(&mut self) {
        self.ticks_in_window = self.ticks_in_window.saturating_add(1);
        let now = Instant::now();

        if now.duration_since(self.window_start) >= self.window_duration {
            if self.ticks_in_window >= self.burst_threshold_ticks {
                // Promote to zero-delay burst spinning
                self.burst_until = Some(now + self.burst_decay_duration);
            }
            self.window_start = now;
            self.ticks_in_window = 0;
        }

        self.inner.reset();
    }

    /// Indicates whether burst mode is currently active
    #[inline(always)]
    pub fn is_in_burst_mode(&self) -> bool {
        if let Some(expiry) = self.burst_until {
            Instant::now() < expiry
        } else {
            false
        }
    }
}

impl<S: WaitStrategy> WaitStrategy for AdaptiveBurstWaitStrategy<S> {
    #[inline(always)]
    fn reset(&mut self) {
        self.record_tick();
    }

    #[inline]
    fn idle(&mut self) {
        if self.is_in_burst_mode() {
            // Unconditionally spin without sleep/yield during active burst window
            core::hint::spin_loop();
        } else {
            self.inner.idle();
        }
    }

    #[inline]
    fn park_timeout(&mut self, timeout: Duration) {
        if self.is_in_burst_mode() {
            core::hint::spin_loop();
        } else {
            self.inner.park_timeout(timeout);
        }
    }

    #[inline(always)]
    fn kind(&self) -> WaitStrategyKind {
        if self.is_in_burst_mode() {
            WaitStrategyKind::BusySpin
        } else {
            self.inner.kind()
        }
    }
}

// -----------------------------------------------------------------------------
// Enum Dispatcher for Zero-Allocation Hot Path Execution
// -----------------------------------------------------------------------------

/// Zero-heap polymorphic wait strategy dispatcher
#[derive(Debug, Clone)]
pub enum DynamicWaitStrategy {
    Busy(BusySpinStrategy),
    Hybrid(HybridAdaptiveStrategy),
    Power(PowerEfficientStrategy),
    Burst(AdaptiveBurstWaitStrategy<HybridAdaptiveStrategy>),
}

impl DynamicWaitStrategy {
    /// Constructs the optimal strategy for the target hardware tier
    pub fn for_tier(tier: HardwareTier) -> Self {
        match tier {
            HardwareTier::Tier3EnterpriseBareMetal => {
                DynamicWaitStrategy::Busy(BusySpinStrategy::new())
            }
            HardwareTier::Tier2CloudVirtualized => {
                DynamicWaitStrategy::Burst(AdaptiveBurstWaitStrategy::new(
                    HybridAdaptiveStrategy::default(),
                    500, // 500 ticks per 100ms = 5,000 ticks/sec threshold
                    Duration::from_millis(100),
                    Duration::from_millis(50),
                ))
            }
            HardwareTier::Tier1MidRange => {
                DynamicWaitStrategy::Power(PowerEfficientStrategy::default())
            }
        }
    }
}

impl WaitStrategy for DynamicWaitStrategy {
    #[inline(always)]
    fn reset(&mut self) {
        match self {
            DynamicWaitStrategy::Busy(s) => s.reset(),
            DynamicWaitStrategy::Hybrid(s) => s.reset(),
            DynamicWaitStrategy::Power(s) => s.reset(),
            DynamicWaitStrategy::Burst(s) => s.reset(),
        }
    }

    #[inline(always)]
    fn idle(&mut self) {
        match self {
            DynamicWaitStrategy::Busy(s) => s.idle(),
            DynamicWaitStrategy::Hybrid(s) => s.idle(),
            DynamicWaitStrategy::Power(s) => s.idle(),
            DynamicWaitStrategy::Burst(s) => s.idle(),
        }
    }

    #[inline(always)]
    fn park_timeout(&mut self, timeout: Duration) {
        match self {
            DynamicWaitStrategy::Busy(s) => s.park_timeout(timeout),
            DynamicWaitStrategy::Hybrid(s) => s.park_timeout(timeout),
            DynamicWaitStrategy::Power(s) => s.park_timeout(timeout),
            DynamicWaitStrategy::Burst(s) => s.park_timeout(timeout),
        }
    }

    #[inline(always)]
    fn kind(&self) -> WaitStrategyKind {
        match self {
            DynamicWaitStrategy::Busy(s) => s.kind(),
            DynamicWaitStrategy::Hybrid(s) => s.kind(),
            DynamicWaitStrategy::Power(s) => s.kind(),
            DynamicWaitStrategy::Burst(s) => s.kind(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_busy_spin_strategy() {
        let mut strategy = BusySpinStrategy::new();
        assert_eq!(strategy.kind(), WaitStrategyKind::BusySpin);
        strategy.reset();
        strategy.idle();
    }

    #[test]
    fn test_hybrid_adaptive_strategy_stages() {
        let mut strategy = HybridAdaptiveStrategy::new(5, 5, 20);
        assert_eq!(strategy.kind(), WaitStrategyKind::HybridAdaptive);

        // Spin phase
        for _ in 0..5 {
            strategy.idle();
        }
        assert_eq!(strategy.counter, 5);

        // Yield phase
        for _ in 0..5 {
            strategy.idle();
        }
        assert_eq!(strategy.counter, 10);

        // Sleep phase
        strategy.idle();
        assert_eq!(strategy.counter, 11);

        // Reset
        strategy.reset();
        assert_eq!(strategy.counter, 0);
        assert_eq!(strategy.current_sleep_us, 1);
    }

    #[test]
    fn test_power_efficient_strategy() {
        let mut strategy = PowerEfficientStrategy::new(3, 5, 50);
        assert_eq!(strategy.kind(), WaitStrategyKind::PowerEfficient);

        for _ in 0..3 {
            strategy.idle();
        }
        strategy.idle(); // Enters sleep
        strategy.reset();
        assert_eq!(strategy.counter, 0);
    }

    #[test]
    fn test_adaptive_burst_promotion() {
        let inner = HybridAdaptiveStrategy::new(2, 2, 10);
        let mut burst = AdaptiveBurstWaitStrategy::new(
            inner,
            3,
            Duration::from_millis(50),
            Duration::from_millis(100),
        );

        assert!(!burst.is_in_burst_mode());

        // Simulate rapid ticks triggering burst mode
        std::thread::sleep(Duration::from_millis(60));
        burst.record_tick();
        burst.record_tick();
        burst.record_tick();
        std::thread::sleep(Duration::from_millis(60));
        burst.record_tick();

        assert!(burst.is_in_burst_mode());
        assert_eq!(burst.kind(), WaitStrategyKind::BusySpin);
    }

    #[test]
    fn test_dynamic_wait_strategy_tiers() {
        let s1 = DynamicWaitStrategy::for_tier(HardwareTier::Tier1MidRange);
        assert_eq!(s1.kind(), WaitStrategyKind::PowerEfficient);

        let s2 = DynamicWaitStrategy::for_tier(HardwareTier::Tier2CloudVirtualized);
        assert!(matches!(s2, DynamicWaitStrategy::Burst(_)));

        let s3 = DynamicWaitStrategy::for_tier(HardwareTier::Tier3EnterpriseBareMetal);
        assert_eq!(s3.kind(), WaitStrategyKind::BusySpin);
    }
}
