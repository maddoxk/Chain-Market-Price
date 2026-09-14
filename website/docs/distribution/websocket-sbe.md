---
id: websocket-sbe
title: Simple Binary Encoding (SBE) & Dual-Wire WebSocket Dispatch
sidebar_label: WebSocket & SBE
---

# Simple Binary Encoding (SBE) & Dual-Wire WebSocket Dispatch

For cross-datacenter and edge consumers, `Chain-Market-Price` provides RFC 6455 WebSockets with a **dual-wire dispatch engine**, allowing clients to subscribe to either binary SBE frames or SIMD-formatted JSON.

---

## 1. Simple Binary Encoding (SBE) Protocol

SBE is the institutional standard for low-latency market data framing (used by CME, Eurex, and LMAX):
- **Zero-Copy Serialization:** Fields are packed in little-endian binary format matching hardware memory layout.
- **Direct Pointer Casting:** Consumers cast byte buffers directly into struct definitions with zero string parsing or allocations.

### Message Framing Header (8 Bytes)
```text
+-------------------+--------------------+-------------------+-------------------+
| blockLength (u16) | templateId (u16)   | schemaId (u16: 1) | version (u16: 1)  |
| Bytes [0..1]      | Bytes [2..3]       | Bytes [4..5]      | Bytes [6..7]      |
+-------------------+--------------------+-------------------+-------------------+
```

### Template 101: `BBOReport` (92 Bytes Total)
```text
Offset  Field              Type           Description
--------------------------------------------------------------------------------
0       blockLength        uint16         84
2       templateId         uint16         101
4       schemaId           uint16         1
6       version            uint16         1
8       t0_exchange_ns     uint64         Venue matching engine / chain timestamp
16      t1_ingest_nic_ns   uint64         Hardware PTP packet timestamp at NIC
24      t2_engine_proc_ns  uint64         Engine normalization completion timestamp
32      t3_egress_ns       uint64         Wire dispatch timestamp
40      marketId           uint32         Global token pair ID
44      sequence           uint64         Monotonic sequence number
52      bidPrice           int64          Scaled fixed-point (Value * 1e8)
60      bidQty             uint64         Scaled fixed-point (Value * 1e8)
68      askPrice           int64          Scaled fixed-point (Value * 1e8)
76      askQty             uint64         Scaled fixed-point (Value * 1e8)
84      spreadBps          uint16         Bid-Ask spread in basis points
86      venue              uint16         Venue ID (1=Binance, 102=UniV3, etc.)
88      chain              uint16         Chain ID (0=Offchain, 1=ETH, 501=SOL)
90      flags              uint16         Bitmask flags (0x01=Synthetic, 0x02=Mempool)
```

---

## 2. Vectorized Frame Unmasking

RFC 6455 requires client-to-server WebSocket frames to be XOR-masked with a 4-byte key. Standard implementations loop byte-by-byte.

`Chain-Market-Price` unmasks payloads 8 bytes at a time by repeating the 4-byte mask into a 64-bit integer, unmasking full 64-bit words in a single XOR instruction:

```rust
#[inline(always)]
pub fn unmask_payload(payload: &mut [u8], mask: [u8; 4]) {
    let mask_u32 = u32::from_ne_bytes(mask);
    let mask_u64 = ((mask_u32 as u64) << 32) | (mask_u32 as u64);

    let mut chunks_exact = payload.chunks_exact_mut(8);
    for chunk in &mut chunks_exact {
        let val = u64::from_ne_bytes(chunk.try_into().unwrap());
        chunk.copy_from_slice(&(val ^ mask_u64).to_ne_bytes());
    }

    let remainder = chunks_exact.into_remainder();
    let offset = payload.len() - remainder.len();
    for (i, byte) in remainder.iter_mut().enumerate() {
        *byte ^= mask[(offset + i) % 4];
    }
}
```
