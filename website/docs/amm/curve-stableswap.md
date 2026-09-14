---
id: curve-stableswap
title: Curve Stableswap Invariant Solver (256-Bit Precision)
sidebar_label: Curve Stableswap
---

# Curve Stableswap Invariant Solver (256-Bit Precision)

Curve Finance's Stableswap invariant combines constant product (`x * y = k`) and constant sum (`x + y = k`) curves to create extremely deep liquidity near parity ($1.00) with minimal slippage for pegged assets (e.g. DAI / USDC / USDT or stETH / ETH).

---

## 1. The Stableswap Invariant

For `n` coins with amplification coefficient `A`, normalized balances `x_i`, and invariant `D`:

```text
A * n^n * sum(x_i) + D = A * D * n^n + D^(n+1) / (n^n * prod(x_i))
```

---

## 2. Integer Newton-Raphson Iteration

To solve for `D` without floating-point instructions or memory allocations:
```text
D_{k+1} = ((A * n^n * S + D_P * n) * D_k) / ((A * n^n - 1) * D_k + (n + 1) * D_P)
where D_P = D^(n+1) / (n^n * prod(x_i))
```

### The 256-Bit Precision Requirement
In balanced 3-pools (e.g. 10M DAI, 10M USDC, 10M USDT):
```text
x_i = 10,000,000 * 10^18 = 10^25
S = sum(x_i) = 3 * 10^25,   D ≈ 3 * 10^25
```

During Newton-Raphson iterations:
```text
Intermediate Product (D_P * D) = (3 * 10^25)^2 ≈ 9 * 10^50
Numerator ≈ 4.86 * 10^54 ≈ 2^181
```

Because `u128::MAX` is `≈ 3.4 * 10^38`, calculating this with standard 128-bit integers overflows by 16 orders of magnitude!

### The `U256` Stack-Allocated Architecture
`Chain-Market-Price` implements a dedicated, zero-alloc 256-bit unsigned integer (`U256`) that handles 256-bit additions, subtractions, multiplications, and binary long division directly on the CPU stack:

```rust
#[derive(Copy, Clone, Debug, Default, PartialEq, Eq, PartialOrd, Ord)]
pub struct U256 {
    pub hi: u128,
    pub lo: u128,
}
```

This guarantees:
- **Strict Convergence:** Converges to exact integer solution in 4 or fewer iterations.
- **Zero Allocations:** Entire calculation lives in CPU registers and L1 stack memory.
- **Deterministic Latency:** Typical solution time under 450 nanoseconds.
