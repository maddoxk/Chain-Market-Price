---
id: constant-product
title: Uniswap v2 Constant Product Virtualizer
sidebar_label: Uniswap v2 (x * y = k)
---

# Uniswap v2 Constant Product Virtualizer

Decentralized AMM pools maintain continuous bonding curves rather than discrete limit orders. The `amm-virtualizer` transforms continuous `x * y = k` curves into discrete central limit order book (CLOB) price levels for quantitative algorithms.

---

## 1. Mathematical Formulation

Given token reserves `x` (base) and `y` (quote) with swap fee `phi` (e.g. 30 bps = 0.3%):
```text
x * y = k
```

Spot price normalized to `10^8` fixed-point:
```text
P_spot = (y * 10^base_decimals * 10^8) / (x * 10^quote_decimals)
```

### Virtual Depth Discretization
For a target price level `P_target`, the required base reserve `x_target` is:
```text
x_target = x * sqrt(P_spot / P_target)
```

The available executable quantity `delta_x` at that price band is:
- **Asks (`P_target > P_spot`):** `delta_x = x - x_target`
- **Bids (`P_target < P_spot`):** `delta_x = x_target - x`

---

## 2. Zero-Overflow Integer Implementation

Direct multiplication of `x * y * 10^8` overflows 128-bit unsigned integers for standard high-decimal pairs (e.g. WETH 18 decimals, USDC 6 decimals). The engine evaluates the square root ratio using scaled integer operations:

```rust
// Compute square root ratio in 10^18 fixed point
let ratio_sq = (spot as u128 * 1_000_000_000_000_000_000) / target_price_u128;
let ratio = integer_sqrt(ratio_sq); // 10^9 fixed point

// Target base reserve without intermediate overflow
let target_base = (self.reserve_base * ratio) / 1_000_000_000;
let delta_base = self.reserve_base.saturating_sub(target_base);
let qty_scaled = ((delta_base * 100_000_000) / (10u128.pow(self.base_decimals as u32))) as u64;

book.update_ask(target_price, qty_scaled, 1, ts_ns);
```
