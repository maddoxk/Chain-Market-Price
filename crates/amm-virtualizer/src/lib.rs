//! # AMM to Virtual Order Book Reconstruction Engine
//!
//! Converts decentralized automated market maker (AMM) pools and concentrated
//! liquidity models into standard Central Limit Order Book (CLOB) discrete L2 depth ladders:
//! - **Uniswap v2**: Constant product invariant ($x \cdot y = k$).
//! - **Uniswap v3 / v4**: Concentrated liquidity active tick arrays and bitmask scanning.
//! - **Raydium CLMM & Orca Whirlpools**: Solana discrete price bins and liquidity ladders.
//!
//! Outputs directly into `core_engine::ContiguousOrderBook` for sub-nanosecond lookups.

#![allow(clippy::all)]

pub mod curve;
pub mod v4;

pub use curve::CurvePool;
pub use v4::{HookFlags, HookSimulationConfig, PoolKey, UniswapV4Pool, DYNAMIC_FEE_FLAG};

use core_engine::{ContiguousOrderBook, PRICE_LEVELS_COUNT, TICK_SIZE};

/// Uniswap v2 Constant Product ($x \cdot y = k$) Virtualizer
#[derive(Clone, Debug)]
pub struct UniswapV2Pool {
    pub reserve_base: u128,   // Reserve 0 (e.g. WETH in wei)
    pub reserve_quote: u128,  // Reserve 1 (e.g. USDC in 6 or 18 decimals)
    pub fee_numerator: u128,  // e.g. 997 (0.3% fee)
    pub fee_denominator: u128,// 1000
    pub base_decimals: u8,
    pub quote_decimals: u8,
}

impl UniswapV2Pool {
    pub fn new(reserve_base: u128, reserve_quote: u128, fee_bps: u16) -> Self {
        let fee_numerator = 10_000 - fee_bps as u128;
        Self {
            reserve_base,
            reserve_quote,
            fee_numerator,
            fee_denominator: 10_000,
            base_decimals: 18,
            quote_decimals: 6,
        }
    }

    /// Computes spot price scaled to 10^8 fixed-point
    pub fn spot_price_scaled(&self) -> i64 {
        if self.reserve_base == 0 {
            return 0;
        }
        // Price = (reserve_quote / reserve_base) scaled by 1e8
        let price = (self.reserve_quote * 100_000_000) / self.reserve_base;
        price as i64
    }

    /// Discretizes continuous constant product depth into virtual L2 bids and asks
    /// around spot price for N percentage depth bands.
    pub fn populate_virtual_order_book(
        &self,
        book: &mut ContiguousOrderBook,
        bands_bps: &[u16],
        ts_ns: u64,
    ) {
        let spot = self.spot_price_scaled();
        if spot <= 0 {
            return;
        }

        // Generate virtual ask levels: buying base consumes reserve_base
        for &bps in bands_bps {
            let target_price = spot + (spot * bps as i64 / 10_000);
            let target_price_u128 = target_price as u128;

            // Using invariant x * y = k to calculate available output
            // delta_base = reserve_base - sqrt(reserve_base * reserve_quote / target_price)
            if target_price_u128 > 0 {
                let k = self.reserve_base * self.reserve_quote;
                let target_base_squared = (k * 100_000_000) / target_price_u128;
                let target_base = integer_sqrt(target_base_squared);

                if self.reserve_base > target_base {
                    let delta_base = self.reserve_base - target_base;
                    let qty_scaled = ((delta_base * 100_000_000) / (10u128.pow(self.base_decimals as u32))) as u64;

                    // Update ask level in book
                    let diff = (target_price - book.base_price) / TICK_SIZE;
                    if diff >= 0 && (diff as usize) < PRICE_LEVELS_COUNT {
                        let idx = diff as usize;
                        book.asks[idx].price = target_price;
                        book.asks[idx].quantity = qty_scaled;
                        book.asks[idx].last_update_ts = ts_ns;
                    }
                }
            }

            // Generate virtual bid levels: selling base adds to reserve_base
            let target_bid_price = spot.saturating_sub(spot * bps as i64 / 10_000);
            if target_bid_price > 0 {
                let target_bid_price_u128 = target_bid_price as u128;
                let k = self.reserve_base * self.reserve_quote;
                let target_base_squared = (k * 100_000_000) / target_bid_price_u128;
                let target_base = integer_sqrt(target_base_squared);

                if target_base > self.reserve_base {
                    let delta_base = target_base - self.reserve_base;
                    let qty_scaled = ((delta_base * 100_000_000) / (10u128.pow(self.base_decimals as u32))) as u64;
                    book.update_bid(target_bid_price, qty_scaled, 1, ts_ns);
                }
            }
        }
    }
}

/// Concentrated Liquidity Active Tick for Uniswap v3 & Raydium CLMM
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ConcentratedTick {
    pub tick_index: i32,
    pub liquidity_gross: u128,
    pub liquidity_net: i128,
}

/// Concentrated Liquidity Pool Virtualizer (Uniswap v3 / v4 & Raydium CLMM)
#[derive(Clone, Debug)]
pub struct ConcentratedLiquidityPool {
    pub current_tick: i32,
    pub sqrt_price_x96: u128,
    pub current_liquidity: u128,
    pub tick_spacing: i32,
}

impl ConcentratedLiquidityPool {
    pub fn new(current_tick: i32, sqrt_price_x96: u128, current_liquidity: u128, tick_spacing: i32) -> Self {
        Self {
            current_tick,
            sqrt_price_x96,
            current_liquidity,
            tick_spacing,
        }
    }

    /// Converts sqrtPriceX96 to 10^8 scaled integer price
    /// Price = (sqrtPriceX96 / 2^96)^2 * 1e8
    pub fn spot_price_scaled(&self) -> i64 {
        if self.sqrt_price_x96 == 0 {
            return 0;
        }
        let sp = self.sqrt_price_x96 >> 32;
        let p_num = sp * sp;
        let p_scaled = ((p_num >> 64) * 100_000_000) >> 64;
        p_scaled as i64
    }

    /// Synthesizes L2 depth from current tick and active concentrated liquidity
    pub fn populate_order_book(
        &self,
        book: &mut ContiguousOrderBook,
        ticks: &[ConcentratedTick],
        ts_ns: u64,
    ) {
        let spot = self.spot_price_scaled();
        if spot <= 0 {
            return;
        }

        // Active liquidity immediately around the current tick
        let mut active_liquidity = self.current_liquidity;

        // Iterate through adjacent ticks above current_tick (asks)
        for tick in ticks.iter().filter(|t| t.tick_index > self.current_tick) {
            let price_step = spot + ((tick.tick_index - self.current_tick) as i64 * 100);
            let available_qty = (active_liquidity / 1_000_000_000) as u64;

            let diff = (price_step - book.base_price) / TICK_SIZE;
            if diff >= 0 && (diff as usize) < PRICE_LEVELS_COUNT {
                let idx = diff as usize;
                book.asks[idx].price = price_step;
                book.asks[idx].quantity = available_qty;
                book.asks[idx].last_update_ts = ts_ns;
            }

            if tick.liquidity_net >= 0 {
                active_liquidity = active_liquidity.saturating_add(tick.liquidity_net as u128);
            } else {
                active_liquidity = active_liquidity.saturating_sub((-tick.liquidity_net) as u128);
            }
        }

        // Reset and iterate through ticks below current_tick (bids)
        let mut bid_liquidity = self.current_liquidity;
        for tick in ticks.iter().filter(|t| t.tick_index <= self.current_tick).rev() {
            let price_step = spot.saturating_sub((self.current_tick - tick.tick_index) as i64 * 100);
            if price_step > 0 {
                let available_qty = (bid_liquidity / 1_000_000_000) as u64;
                book.update_bid(price_step, available_qty, 1, ts_ns);
            }

            if tick.liquidity_net >= 0 {
                bid_liquidity = bid_liquidity.saturating_sub(tick.liquidity_net as u128);
            } else {
                bid_liquidity = bid_liquidity.saturating_add((-tick.liquidity_net) as u128);
            }
        }
    }
}

/// Fast integer square root using Newton-Raphson approximation
fn integer_sqrt(n: u128) -> u128 {
    if n == 0 {
        return 0;
    }
    let mut x0 = n / 2;
    if x0 == 0 {
        return 1;
    }
    let mut x1 = (x0 + n / x0) / 2;
    while x1 < x0 {
        x0 = x1;
        x1 = (x0 + n / x0) / 2;
    }
    x0
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_uniswap_v2_discretization() {
        // Pool: 1,000 WETH and 2,000,000 USDC -> Spot = $2,000
        let pool = UniswapV2Pool::new(1_000 * 10u128.pow(18), 2_000_000 * 10u128.pow(6), 30);
        assert_eq!(pool.spot_price_scaled(), 200000000000);

        let mut book = ContiguousOrderBook::new(200000000000);
        pool.populate_virtual_order_book(&mut book, &[10, 25, 50, 100], 1000);

        // BBO bid should now exist
        assert!(book.bbo_bid().is_some());
        assert!(book.bbo_bid().unwrap().quantity > 0);
    }
}
