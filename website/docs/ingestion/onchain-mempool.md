---
id: onchain-mempool
title: On-Chain & Mempool Ingestion Pipelines
sidebar_label: On-Chain & Mempool
---

# On-Chain & Mempool Ingestion Pipelines

High-frequency statistical arbitrage and MEV strategies require pre-block quote awareness. The `onchain-ingest` crate monitors both EVM execution clients (**Reth IPC**) and high-speed Solana validator streams (**Yellowstone Geyser & Jito ShredStream**).

---

## 1. EVM Mempool & Speculative State Sandbox

When pending transactions arrive via local IPC, the engine projects the post-transaction state of active liquidity pools before miners include them in a block.

```
       Pending Swap Transaction (P2P Gossip)
                         |
                         v
       +------------------------------------+
       |   Speculative State Simulator      |
       |   - Calculate amount_in with fee   |
       |   - Compute constant product out   |
       |   - Update synthetic reserves      |
       +------------------------------------+
                         |
                         v
       Dispatched Normalized BBO (Flags: 0x02 = Mempool Inferred)
```

### Invariant State Transition
Given an existing pool with reserves `(R_base, R_quote)`:
```text
delta_y = (x_in * (1 - phi) * R_quote) / (R_base + x_in * (1 - phi))
```
The simulated pool updates its reserves and publishes an updated synthetic BBO with flag `0x02` (`Mempool Inferred`), alerting arbitrage algorithms before block confirmation.

---

## 2. Solana Yellowstone Geyser & ShredStream Ingestion

Solana delivers state updates via gRPC streaming protocols directly from validator nodes.

### Pipeline Highlights
- **Yellowstone Geyser gRPC:** Connects via zero-copy protocol buffer streaming to capture account changes for Raydium CLMM and Orca Whirlpools.
- **Jito ShredStream:** Ingests raw TPU (Transaction Processing Unit) shreds before leader block assembly, achieving sub-10ms transaction visibility on Solana mainnet.
- **Borsh Deserialization:** Unpacks fixed-layout account data into normalized ticks with hardware nanosecond timestamps.
