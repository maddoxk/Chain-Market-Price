---
id: v4-hooks
title: Uniswap v4 Dynamic Hook & PoolKey Discretizer
sidebar_label: Uniswap v4 Hooks
---

# Uniswap v4 Dynamic Hook & PoolKey Discretizer

Uniswap v4 introduces singleton pool architecture and customizable **Hooks**—smart contracts that execute arbitrary logic before or after pool actions (`beforeSwap`, `afterSwap`, `beforeAddLiquidity`, `afterAddLiquidity`).

---

## 1. PoolKey & Hook Flag Bitmask Architecture

In Uniswap v4, pools are identified by a unique `PoolKey` rather than independent contract addresses:

```rust
pub struct PoolKey {
    pub currency0: [u8; 20],
    pub currency1: [u8; 20],
    pub fee: u32,
    pub tick_spacing: i32,
    pub hooks: [u8; 20],
}
```

The leading byte of the `hooks` address encodes enabled capabilities via bitflags:

| Flag Name | Mask | Behavior |
| :--- | :--- | :--- |
| `BEFORE_INITIALIZE` | `1 << 13` | Hook runs before pool initialization |
| `AFTER_INITIALIZE` | `1 << 12` | Hook runs after pool initialization |
| `BEFORE_ADD_LIQUIDITY`| `1 << 11` | Hook runs before liquidity mint |
| `AFTER_ADD_LIQUIDITY` | `1 << 10` | Hook runs after liquidity mint |
| `BEFORE_SWAP` | `1 << 7` | Hook executes dynamic fee adjustments before swap |
| `AFTER_SWAP` | `1 << 6` | Hook executes post-swap rebalancing or fee capture |

---

## 2. Dynamic Fee Discretization

When the `DYNAMIC_FEE_FLAG` (`0x800000`) is active, the pool's static fee is replaced by a dynamic fee computed at swap time:

```rust
pub fn effective_fee_bps(&self) -> u16 {
    let base_fee_bps = if self.key.has_dynamic_fee() {
        if let Some(dyn_fee) = self.hook_config.dynamic_fee_override_bps {
            dyn_fee
        } else {
            (self.key.fee & !DYNAMIC_FEE_FLAG) as u16
        }
    } else {
        (self.key.fee / 100) as u16
    };

    if self.key.has_hook(HookFlags::BEFORE_SWAP) {
        let adjusted = base_fee_bps as i32 + self.hook_config.hook_fee_delta_bps as i32;
        adjusted.max(0).min(10_000) as u16
    } else {
        base_fee_bps
    }
}
```

The virtualizer integrates the hook-modified fee into the bid-ask spread calculation, feeding accurate executable net pricing directly into statistical arbitrage models.
