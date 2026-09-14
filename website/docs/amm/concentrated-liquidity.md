---
id: concentrated-liquidity
title: Concentrated Liquidity Virtualizer (Uniswap v3 & Raydium CLMM)
sidebar_label: Concentrated Liquidity
---

# Concentrated Liquidity Virtualizer (Uniswap v3 & Raydium CLMM)

Concentrated liquidity market makers allocate capital to discrete price intervals `[P_l, P_u]`, creating non-linear depth distributions that shift dynamically as trades cross tick boundaries.

---

## 1. Mathematical Principles

In Uniswap v3 and Raydium CLMM:
```text
sqrt(P_x96) = sqrt(P) * 2^96
Price = (sqrt(P_x96) / 2^96)^2
```

### Overflow-Free Spot Price Extraction
Squaring a 96-bit fixed-point number directly (`2^96 * 2^96 = 2^192`) exceeds 128 bits. The engine uses a 48-bit shift:

```rust
pub fn spot_price_scaled(&self) -> i64 {
    if self.sqrt_price_x96 == 0 {
        return 0;
    }
    // Shift by 48 bits: sp has ~54 bits for prices up to $10,000,000
    let sp = self.sqrt_price_x96 >> 48;
    let p_num = sp * sp; // Fits in 108 bits (< 128 bits)
    let p_scaled = (((p_num >> 32) * 100_000_000) + (1 << 63)) >> 64;
    p_scaled as i64
}
```

---

## 2. Order Book Ladder Synthesis

Given active liquidity $L$ and adjacent initialized ticks:
1. **Asks:** Step upwards through tick indices above `current_tick`. As each tick boundary is crossed, add `liquidity_net` to active liquidity.
2. **Bids:** Step downwards through tick indices below `current_tick`. Subtract `liquidity_net` as ticks are crossed.
3. Update discrete levels into the 2048-slot contiguous order book with sub-microsecond latency.
