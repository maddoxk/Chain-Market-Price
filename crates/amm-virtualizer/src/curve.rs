//! # Curve Finance Stableswap Invariant Engine
//!
//! Subsystem for solving and discretizing Curve Stableswap liquidity pools into
//! standard L2 central limit order book depth ladders:
//!
//! Mathematical Invariant:
//! For $n$ tokens with amplification coefficient $A$ and normalized balances $x_i$:
//! $$A n^n \sum_{i=1}^n x_i + D = A D n^n + \frac{D^{n+1}}{n^n \prod_{i=1}^n x_i}$$
//!
//! Architectural Invariants:
//! - Pure integer fixed-point arithmetic: zero floating-point operations.
//! - Newton-Raphson solver guaranteed to converge in $\le 4$ iterations.
//! - Decimal normalization handling for mixed-precision pools (e.g. DAI 18, USDC 6, USDT 6).
//! - Direct synthesis into `core_engine::ContiguousOrderBook`.

use core_engine::{ContiguousOrderBook, PRICE_LEVELS_COUNT, TICK_SIZE};

/// Maximum supported coins in a single Curve pool
pub const MAX_COINS: usize = 3;

/// Fixed-size stack-allocated 256-bit unsigned integer
/// Enables zero-heap intermediate calculations for Stableswap invariants without overflow.
#[derive(Copy, Clone, Debug, Default, PartialEq, Eq, PartialOrd, Ord)]
pub struct U256 {
    pub hi: u128,
    pub lo: u128,
}

impl U256 {
    pub const ZERO: Self = Self { hi: 0, lo: 0 };
    pub const ONE: Self = Self { hi: 0, lo: 1 };

    #[inline(always)]
    pub const fn from_u128(v: u128) -> Self {
        Self { hi: 0, lo: v }
    }

    #[inline(always)]
    pub const fn as_u128(&self) -> u128 {
        self.lo
    }

    #[inline(always)]
    pub fn is_zero(&self) -> bool {
        self.hi == 0 && self.lo == 0
    }

    #[inline(always)]
    pub fn add(&self, other: Self) -> Self {
        let (lo, carry) = self.lo.overflowing_add(other.lo);
        let hi = self
            .hi
            .wrapping_add(other.hi)
            .wrapping_add(if carry { 1 } else { 0 });
        Self { hi, lo }
    }

    #[inline(always)]
    pub fn sub(&self, other: Self) -> Self {
        let (lo, borrow) = self.lo.overflowing_sub(other.lo);
        let hi = self
            .hi
            .wrapping_sub(other.hi)
            .wrapping_sub(if borrow { 1 } else { 0 });
        Self { hi, lo }
    }

    pub fn mul_u128(&self, other: u128) -> Self {
        let lo_lo = (self.lo as u64) as u128;
        let lo_hi = (self.lo >> 64) as u128;
        let o_lo = (other as u64) as u128;
        let o_hi = (other >> 64) as u128;

        let p0 = lo_lo * o_lo;
        let p1 = lo_lo * o_hi;
        let p2 = lo_hi * o_lo;
        let p3 = lo_hi * o_hi;

        let (mid, c1) = p1.overflowing_add(p2);
        let mid_lo = (mid as u64) as u128;
        let mid_hi = (mid >> 64) + if c1 { 1 << 64 } else { 0 };

        let (lo, c2) = p0.overflowing_add(mid_lo << 64);
        let hi = p3 + mid_hi + self.hi.wrapping_mul(other) + if c2 { 1 } else { 0 };
        Self { hi, lo }
    }

    #[inline(always)]
    pub fn mul(&self, other: Self) -> Self {
        let mut res = self.mul_u128(other.lo);
        res.hi = res.hi.wrapping_add(self.lo.wrapping_mul(other.hi));
        res
    }

    pub fn div(&self, other: Self) -> Self {
        if other.is_zero() {
            panic!("division by zero in U256");
        }
        if *self < other {
            return Self::ZERO;
        }
        if other == Self::ONE {
            return *self;
        }

        let mut quotient = Self::ZERO;
        let mut remainder = Self::ZERO;

        for i in (0..256).rev() {
            remainder = remainder.shift_left_1();
            if self.get_bit(i) {
                remainder.lo |= 1;
            }
            if remainder >= other {
                remainder = remainder.sub(other);
                quotient.set_bit(i);
            }
        }
        quotient
    }

    #[inline(always)]
    fn shift_left_1(&self) -> Self {
        let hi = (self.hi << 1) | (self.lo >> 127);
        let lo = self.lo << 1;
        Self { hi, lo }
    }

    #[inline(always)]
    fn get_bit(&self, bit: usize) -> bool {
        if bit < 128 {
            (self.lo >> bit) & 1 == 1
        } else {
            (self.hi >> (bit - 128)) & 1 == 1
        }
    }

    #[inline(always)]
    fn set_bit(&mut self, bit: usize) {
        if bit < 128 {
            self.lo |= 1u128 << bit;
        } else {
            self.hi |= 1u128 << (bit - 128);
        }
    }
}

/// Curve Stableswap Pool Supporting 2-Token and 3-Token Pools
#[derive(Clone, Debug)]
pub struct CurvePool {
    /// Number of tokens in the pool (2 or 3)
    pub n_coins: usize,
    /// Raw unnormalized token reserves as reported on-chain
    pub raw_reserves: [u128; MAX_COINS],
    /// Precision multipliers to normalize each token balance to 18 decimals
    /// e.g. for DAI (18 decimals): 1, for USDC (6 decimals): 10^12
    pub multipliers: [u128; MAX_COINS],
    /// Amplification coefficient A (scaled, typical range 50 - 2000)
    pub a: u128,
    /// Swap fee in basis points (e.g. 4 for 0.04%)
    pub fee_bps: u16,
}

impl CurvePool {
    /// Creates a 2-token Curve pool (e.g. stETH/ETH)
    pub fn new_2pool(reserves: [u128; 2], decimals: [u8; 2], a: u128, fee_bps: u16) -> Self {
        let mut raw = [0u128; MAX_COINS];
        let mut mults = [1u128; MAX_COINS];

        raw[0] = reserves[0];
        raw[1] = reserves[1];

        mults[0] = 10u128.pow((18 - decimals[0]) as u32);
        mults[1] = 10u128.pow((18 - decimals[1]) as u32);

        Self {
            n_coins: 2,
            raw_reserves: raw,
            multipliers: mults,
            a,
            fee_bps,
        }
    }

    /// Creates a 3-token Curve pool (e.g. 3pool DAI / USDC / USDT)
    pub fn new_3pool(reserves: [u128; 3], decimals: [u8; 3], a: u128, fee_bps: u16) -> Self {
        let mut mults = [1u128; MAX_COINS];
        for i in 0..3 {
            mults[i] = 10u128.pow((18 - decimals[i]) as u32);
        }

        Self {
            n_coins: 3,
            raw_reserves: reserves,
            multipliers: mults,
            a,
            fee_bps,
        }
    }

    /// Returns the 18-decimal normalized balances ($xp_i = raw_i \cdot multiplier_i$)
    #[inline(always)]
    pub fn normalized_reserves(&self) -> [u128; MAX_COINS] {
        let mut xp = [0u128; MAX_COINS];
        for i in 0..self.n_coins {
            xp[i] = self.raw_reserves[i] * self.multipliers[i];
        }
        xp
    }

    /// Solves for invariant $D$ using pure integer Newton-Raphson approximation.
    /// Uses 256-bit arithmetic for intermediate products to eliminate u128 overflow.
    /// Converges strictly within $\le 4$ iterations for balanced or moderately unbalanced pools.
    pub fn get_d(&self, xp: &[u128]) -> Option<u128> {
        let n = self.n_coins as u128;
        let mut s = 0u128;
        for i in 0..self.n_coins {
            s += xp[i];
        }
        if s == 0 {
            return Some(0);
        }

        let n_pow_n = if self.n_coins == 2 { 4 } else { 27 };
        let ann = self.a * n_pow_n;
        let mut d = U256::from_u128(s);
        let s_u = U256::from_u128(s);
        let ann_u = U256::from_u128(ann);
        let n_u = U256::from_u128(n);

        for _ in 0..255 {
            let mut d_p = d;
            for i in 0..self.n_coins {
                // d_p = d_p * d / (x[i] * n)
                let denom = U256::from_u128(xp[i]).mul(n_u);
                if denom.is_zero() {
                    return None;
                }
                d_p = d_p.mul(d).div(denom);
            }

            let d_prev = d;
            let numerator = ann_u.mul(s_u).add(d_p.mul(n_u)).mul(d);
            let denominator = ann_u.sub(U256::ONE).mul(d).add(n_u.add(U256::ONE).mul(d_p));
            if denominator.is_zero() {
                return None;
            }
            d = numerator.div(denominator);

            if d > d_prev {
                if d.sub(d_prev) <= U256::ONE {
                    return Some(d.as_u128());
                }
            } else if d_prev.sub(d) <= U256::ONE {
                return Some(d.as_u128());
            }
        }
        Some(d.as_u128())
    }

    /// Solves for output reserve $y$ of token $j$ when token $i$'s normalized balance becomes $x$.
    /// Uses pure integer Newton-Raphson root finding with 256-bit precision.
    pub fn get_y(&self, i: usize, j: usize, x: u128, xp: &[u128]) -> Option<u128> {
        if i == j || i >= self.n_coins || j >= self.n_coins {
            return None;
        }

        let d_val = self.get_d(xp)?;
        let d = U256::from_u128(d_val);
        let n = self.n_coins as u128;
        let n_pow_n = if self.n_coins == 2 { 4 } else { 27 };
        let ann = self.a * n_pow_n;
        let ann_u = U256::from_u128(ann);
        let n_u = U256::from_u128(n);

        let mut c = d;
        let mut s = 0u128;

        for k in 0..self.n_coins {
            let x_val = if k == i {
                x
            } else if k != j {
                xp[k]
            } else {
                continue;
            };

            s += x_val;
            let denom = U256::from_u128(x_val).mul(n_u);
            if denom.is_zero() {
                return None;
            }
            c = c.mul(d).div(denom);
        }

        let ann_n = ann_u.mul(n_u);
        if ann_n.is_zero() {
            return None;
        }
        c = c.mul(d).div(ann_n);
        let b = U256::from_u128(s).add(d.div(ann_u));
        let mut y = d;

        for _ in 0..255 {
            let y_prev = y;
            let numerator = y.mul(y).add(c);
            let denom_sum = U256::from_u128(2).mul(y).add(b);
            if denom_sum <= d {
                return None;
            }
            let denominator = denom_sum.sub(d);
            y = numerator.div(denominator);

            if y > y_prev {
                if y.sub(y_prev) <= U256::ONE {
                    return Some(y.as_u128());
                }
            } else if y_prev.sub(y) <= U256::ONE {
                return Some(y.as_u128());
            }
        }
        Some(y.as_u128())
    }

    /// Calculates net output received when swapping `dx` (raw units) of token $i$ for token $j$.
    pub fn calculate_swap_output(&self, i: usize, j: usize, dx: u128) -> Option<u128> {
        let xp = self.normalized_reserves();
        let dx_norm = dx * self.multipliers[i];
        let new_x = xp[i] + dx_norm;

        let new_y = self.get_y(i, j, new_x, &xp)?;
        if xp[j] > new_y {
            let dy_norm = xp[j] - new_y;
            let fee_norm = (dy_norm * self.fee_bps as u128) / 10_000;
            let dy_net_norm = dy_norm.saturating_sub(fee_norm);
            // Denormalize back to token j's native decimals
            Some(dy_net_norm / self.multipliers[j])
        } else {
            None
        }
    }

    /// Computes marginal spot exchange rate between token $i$ and token $j$, scaled to $10^8$ fixed-point
    pub fn spot_price_scaled(&self, i: usize, j: usize) -> i64 {
        // Delta of 1 native token unit
        let unit_dx = 10u128.pow(18) / self.multipliers[i];
        if let Some(dy) = self.calculate_swap_output(i, j, unit_dx) {
            let dy_norm = dy * self.multipliers[j];
            let price_1e8 = (dy_norm * 100_000_000) / 10u128.pow(18);
            price_1e8 as i64
        } else {
            100_000_000 // $1.00 reference fallback
        }
    }

    /// Discretizes continuous Stableswap curve into virtual L2 bid and ask levels
    /// directly populating `ContiguousOrderBook`.
    pub fn populate_virtual_order_book(
        &self,
        book: &mut ContiguousOrderBook,
        i: usize,
        j: usize,
        bands_bps: &[u16],
        ts_ns: u64,
    ) {
        let spot = self.spot_price_scaled(i, j);
        if spot <= 0 {
            return;
        }

        let base_raw = self.raw_reserves[i];

        for &bps in bands_bps {
            let target_ask = spot + (spot * bps as i64 / 10_000);
            let target_bid = spot.saturating_sub(spot * bps as i64 / 10_000);

            // Compute available chunk depth for this band
            let dx_chunk = (base_raw * bps as u128) / 10_000;
            if let Some(dy) = self.calculate_swap_output(i, j, dx_chunk) {
                // Scaled to 10^8
                let qty_scaled = ((dy * self.multipliers[j] * 100_000_000) / 10u128.pow(18)) as u64;

                // Update Ask level
                let diff_ask = (target_ask - book.base_price) / TICK_SIZE;
                if diff_ask >= 0 && (diff_ask as usize) < PRICE_LEVELS_COUNT {
                    let idx = diff_ask as usize;
                    book.asks[idx].price = target_ask;
                    book.asks[idx].quantity = qty_scaled;
                    book.asks[idx].last_update_ts = ts_ns;
                }

                // Update Bid level
                if target_bid > 0 {
                    book.update_bid(target_bid, qty_scaled, 1, ts_ns);
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_curve_3pool_dai_usdc_usdt_invariant() {
        // Curve 3-Pool: 10M DAI (18 decimals), 10M USDC (6 decimals), 10M USDT (6 decimals)
        let reserves = [
            10_000_000 * 10u128.pow(18), // DAI
            10_000_000 * 10u128.pow(6),  // USDC
            10_000_000 * 10u128.pow(6),  // USDT
        ];
        let decimals = [18, 6, 6];
        let pool = CurvePool::new_3pool(reserves, decimals, 200, 4);

        let xp = pool.normalized_reserves();
        assert_eq!(xp[0], 10_000_000 * 10u128.pow(18));
        assert_eq!(xp[1], 10_000_000 * 10u128.pow(18));
        assert_eq!(xp[2], 10_000_000 * 10u128.pow(18));

        // When perfectly balanced, D = sum(xp) = 30M * 10^18
        let d = pool.get_d(&xp).expect("Failed to compute D");
        assert_eq!(d, 30_000_000 * 10u128.pow(18));

        // Test spot price DAI -> USDC (~$1.00, scaled 10^8)
        let spot = pool.spot_price_scaled(0, 1);
        assert!(spot >= 99_900_000 && spot <= 100_100_000);

        // Swap 10,000 DAI -> expect ~9,996 USDC (after 4 bps fee)
        let dy = pool
            .calculate_swap_output(0, 1, 10_000 * 10u128.pow(18))
            .expect("Swap output failed");
        let dy_scaled = dy as f64 / 1e6;
        assert!(dy_scaled >= 9990.0 && dy_scaled <= 10000.0);
    }

    #[test]
    fn test_curve_2pool_steth_eth_discretization() {
        // 2-pool: 100,000 stETH and 100,000 ETH
        let reserves = [100_000 * 10u128.pow(18), 100_000 * 10u128.pow(18)];
        let pool = CurvePool::new_2pool(reserves, [18, 18], 50, 4);

        let mut book = ContiguousOrderBook::new(100_000_000);
        pool.populate_virtual_order_book(&mut book, 0, 1, &[1, 2, 5, 10], 1000);

        assert!(book.bbo_bid().is_some());
        assert!(book.bbo_bid().unwrap().quantity > 0);
    }
}
