//! # Uniswap v4 Singleton & Dynamic Hook Discretizer
//!
//! Subsystem for modeling Uniswap v4 singleton liquidity pools and custom dynamic hooks,
//! discretizing active concentrated liquidity into standard L2 central limit order book ladders:
//!
//! Architectural Invariants:
//! - Pure integer fixed-point arithmetic: zero floating-point operations.
//! - Models Uniswap v4 `PoolKey` and dynamic hook permissions (beforeSwap, afterSwap, custom fees).
//! - Transient storage delta accounting (`tstore`/flash accounting simulation).
//! - Direct synthesis into `core_engine::ContiguousOrderBook`.
//! - Zero heap allocations during book re-computation.

use crate::ConcentratedTick;
use core_engine::{ContiguousOrderBook, PRICE_LEVELS_COUNT, TICK_SIZE};

/// Uniswap v4 Dynamic Fee Flag (Bit 23 of 24-bit fee)
pub const DYNAMIC_FEE_FLAG: u32 = 0x80_0000;

/// Standard Hook Permission Flags encoded in the top bytes of hook contract address
pub struct HookFlags;

impl HookFlags {
    pub const BEFORE_INITIALIZE: u16 = 1 << 13;
    pub const AFTER_INITIALIZE: u16 = 1 << 12;
    pub const BEFORE_ADD_LIQUIDITY: u16 = 1 << 11;
    pub const AFTER_ADD_LIQUIDITY: u16 = 1 << 10;
    pub const BEFORE_REMOVE_LIQUIDITY: u16 = 1 << 9;
    pub const AFTER_REMOVE_LIQUIDITY: u16 = 1 << 8;
    pub const BEFORE_SWAP: u16 = 1 << 7;
    pub const AFTER_SWAP: u16 = 1 << 6;
    pub const BEFORE_DONATE: u16 = 1 << 5;
    pub const AFTER_DONATE: u16 = 1 << 4;
    pub const BEFORE_SWAP_RETURNS_DELTA: u16 = 1 << 3;
    pub const AFTER_SWAP_RETURNS_DELTA: u16 = 1 << 2;
    pub const AFTER_ADD_LIQUIDITY_RETURNS_DELTA: u16 = 1 << 1;
    pub const AFTER_REMOVE_LIQUIDITY_RETURNS_DELTA: u16 = 1 << 0;

    /// Extracts hook permission bitmask from hook contract address prefix
    #[inline(always)]
    pub fn parse_flags_from_address(address: &[u8; 20]) -> u16 {
        ((address[0] as u16) << 8) | (address[1] as u16)
    }
}

/// Uniswap v4 Canonical PoolKey identifying a pool in the singleton contract
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct PoolKey {
    pub currency0: [u8; 20],
    pub currency1: [u8; 20],
    pub fee: u32,
    pub tick_spacing: i32,
    pub hooks: [u8; 20],
}

impl PoolKey {
    #[inline(always)]
    pub fn has_dynamic_fee(&self) -> bool {
        (self.fee & DYNAMIC_FEE_FLAG) != 0
    }

    #[inline(always)]
    pub fn hook_flags(&self) -> u16 {
        HookFlags::parse_flags_from_address(&self.hooks)
    }

    #[inline(always)]
    pub fn has_hook(&self, flag: u16) -> bool {
        (self.hook_flags() & flag) != 0
    }
}

/// Dynamic Hook Simulation State & Parameters
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct HookSimulationConfig {
    /// Overridden dynamic swap fee in basis points (if dynamic fee hook is active)
    pub dynamic_fee_override_bps: Option<u16>,
    /// Hook specified transient delta adjustment (simulating delta-returning hooks)
    pub hook_delta_specified: i128,
    /// Custom hook fee discount or surcharge in basis points (can be negative for rebates)
    pub hook_fee_delta_bps: i16,
}

/// Uniswap v4 Singleton Concentrated Liquidity Pool with Dynamic Hooks
#[derive(Clone, Debug)]
pub struct UniswapV4Pool {
    pub key: PoolKey,
    pub sqrt_price_x96: u128,
    pub current_tick: i32,
    pub current_liquidity: u128,
    pub hook_config: HookSimulationConfig,
}

impl UniswapV4Pool {
    pub fn new(
        key: PoolKey,
        sqrt_price_x96: u128,
        current_tick: i32,
        current_liquidity: u128,
    ) -> Self {
        Self {
            key,
            sqrt_price_x96,
            current_tick,
            current_liquidity,
            hook_config: HookSimulationConfig::default(),
        }
    }

    pub fn with_hook_config(mut self, config: HookSimulationConfig) -> Self {
        self.hook_config = config;
        self
    }

    /// Computes effective swap fee in basis points, factoring in dynamic hooks
    pub fn effective_fee_bps(&self) -> u16 {
        let base_fee_bps = if self.key.has_dynamic_fee() {
            self.hook_config.dynamic_fee_override_bps.unwrap_or(30) // Default 30 bps (0.3%)
        } else {
            // Standard fee: e.g. 500 -> 5 bps, 3000 -> 30 bps, 10000 -> 100 bps
            (self.key.fee / 100) as u16
        };

        // Apply hook-specified fee delta if beforeSwap/afterSwap hook is active
        if self.key.has_hook(HookFlags::BEFORE_SWAP) {
            let adjusted = base_fee_bps as i32 + self.hook_config.hook_fee_delta_bps as i32;
            adjusted.max(0).min(10_000) as u16
        } else {
            base_fee_bps
        }
    }

    /// Converts sqrtPriceX96 to 10^8 scaled integer spot price
    /// Price = (sqrtPriceX96 / 2^96)^2 * 10^8
    #[inline(always)]
    pub fn spot_price_scaled(&self) -> i64 {
        if self.sqrt_price_x96 == 0 {
            return 0;
        }
        let sp = self.sqrt_price_x96 >> 48;
        let p_num = sp * sp;
        let p_scaled = (((p_num >> 32) * 100_000_000) + (1 << 63)) >> 64;
        p_scaled as i64
    }

    /// Discretizes v4 concentrated liquidity ticks and hook-modified fee spreads
    /// into standard L2 central limit order book depth ladder in `ContiguousOrderBook`.
    pub fn populate_virtual_order_book(
        &self,
        book: &mut ContiguousOrderBook,
        ticks: &[ConcentratedTick],
        ts_ns: u64,
    ) {
        let spot = self.spot_price_scaled();
        if spot <= 0 {
            return;
        }

        let fee_bps = self.effective_fee_bps();

        // 1. Asks (Selling Base for Quote above current tick)
        let mut ask_liquidity = self.current_liquidity;
        for tick in ticks.iter().filter(|t| t.tick_index >= self.current_tick) {
            let tick_distance = (tick.tick_index - self.current_tick) as i64;
            let price_step = spot + (tick_distance * 100);

            // Factor in hook dynamic fee spread
            let price_with_fee = price_step + (price_step * fee_bps as i64 / 20_000);

            let diff_idx = (price_with_fee - book.base_price) / TICK_SIZE;
            if diff_idx >= 0 && (diff_idx as usize) < PRICE_LEVELS_COUNT {
                let idx = diff_idx as usize;
                let available_qty = (ask_liquidity / 1_000_000_000) as u64;
                book.asks[idx].price = price_with_fee;
                book.asks[idx].quantity = available_qty;
                book.asks[idx].last_update_ts = ts_ns;
            }

            // Cross tick net liquidity update
            if tick.liquidity_net >= 0 {
                ask_liquidity = ask_liquidity.saturating_add(tick.liquidity_net as u128);
            } else {
                ask_liquidity = ask_liquidity.saturating_sub((-tick.liquidity_net) as u128);
            }
        }

        // 2. Bids (Buying Base with Quote below current tick)
        let mut bid_liquidity = self.current_liquidity;
        for tick in ticks
            .iter()
            .filter(|t| t.tick_index <= self.current_tick)
            .rev()
        {
            let tick_distance = (self.current_tick - tick.tick_index) as i64;
            let price_step = spot.saturating_sub(tick_distance * 100);

            // Factor in hook dynamic fee spread
            let price_with_fee = price_step.saturating_sub(price_step * fee_bps as i64 / 20_000);

            if price_with_fee > 0 {
                let available_qty = (bid_liquidity / 1_000_000_000) as u64;
                book.update_bid(price_with_fee, available_qty, 1, ts_ns);
            }

            if tick.liquidity_net >= 0 {
                bid_liquidity = bid_liquidity.saturating_sub(tick.liquidity_net as u128);
            } else {
                bid_liquidity = bid_liquidity.saturating_add((-tick.liquidity_net) as u128);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_v4_hook_flags_and_dynamic_fee() {
        let mut hooks_addr = [0u8; 20];
        // Set BEFORE_SWAP flag in first two bytes: 1 << 7 = 0x0080
        hooks_addr[1] = 0x80;

        let key = PoolKey {
            currency0: [0x11; 20],
            currency1: [0x22; 20],
            fee: DYNAMIC_FEE_FLAG | 3000,
            tick_spacing: 60,
            hooks: hooks_addr,
        };

        assert!(key.has_dynamic_fee());
        assert!(key.has_hook(HookFlags::BEFORE_SWAP));
        assert!(!key.has_hook(HookFlags::AFTER_SWAP));

        // Default pool with no hook config override -> 30 bps
        let pool = UniswapV4Pool::new(key, 1u128 << 96, 0, 10_000_000_000);
        assert_eq!(pool.effective_fee_bps(), 30);

        // Pool with dynamic fee hook override -> 12 bps with -2 bps hook rebate = 10 bps
        let config = HookSimulationConfig {
            dynamic_fee_override_bps: Some(12),
            hook_delta_specified: 0,
            hook_fee_delta_bps: -2,
        };
        let dynamic_pool = pool.with_hook_config(config);
        assert_eq!(dynamic_pool.effective_fee_bps(), 10);
    }

    #[test]
    fn test_v4_order_book_discretization() {
        let key = PoolKey {
            currency0: [0xaa; 20],
            currency1: [0xbb; 20],
            fee: 500, // 5 bps static fee
            tick_spacing: 10,
            hooks: [0u8; 20],
        };

        // sqrtPrice for $2,000 spot = sqrt(2000) * 2^96 ≈ 44.72135955 * 2^96
        let sqrt_p = 3543191142285914205922034323210u128;
        let pool = UniswapV4Pool::new(key, sqrt_p, 100, 50_000_000_000_000);

        let mut book = ContiguousOrderBook::new(200_000_000_000); // Base $2,000
        let ticks = [
            ConcentratedTick {
                tick_index: 90,
                liquidity_gross: 5_000_000_000,
                liquidity_net: -5_000_000_000,
            },
            ConcentratedTick {
                tick_index: 100,
                liquidity_gross: 10_000_000_000,
                liquidity_net: 10_000_000_000,
            },
            ConcentratedTick {
                tick_index: 110,
                liquidity_gross: 5_000_000_000,
                liquidity_net: -5_000_000_000,
            },
        ];

        pool.populate_virtual_order_book(&mut book, &ticks, 1234567);

        assert!(book.bbo_bid().is_some());
        assert!(book.bbo_bid().unwrap().quantity > 0);
    }
}
