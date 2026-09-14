# Quant Client Integration Guide
## High-Performance WebSocket & SBE Binary Market Data Feed

Welcome to the **Chain-Market-Price** market data platform. This guide provides quantitative developers, HFT arbitrageurs, and algorithmic trading teams with the exact technical specifications needed to connect, subscribe, and consume data feeds with sub-microsecond latency.

---

## 1. Protocol Architecture & Connection Options

Depending on your colocation topology and latency requirements, the engine provides three access tiers:

| Tier | Protocol | Target Latency | Best Suited For |
| :--- | :--- | :--- | :--- |
| **Tier 1 (Colocated IPC)** | POSIX Shared Memory (`/dev/shm`) | **< 100 nanoseconds** | Algorithms colocated in the same bare-metal server chassis |
| **Tier 2 (DC LAN)** | NASDAQ MoldUDP64 Multicast / TCPDirect | **< 5 microseconds** | Institutional servers in Equinix LD4, TY3, or NY4 cross-connects |
| **Tier 3 (Edge WAN)** | RFC 6455 WebSockets (`wss://`) | **< 250 microseconds** | Remote quant bots, arbitrage traders, and cloud-hosted systems |

---

## 2. WebSocket Connection Lifecycle

### 2.1 Handshake
Connect to the server endpoint via standard WebSocket (or TLS/WSS):
```text
ws://<gateway-host>:9001/v1/marketdata
```

### 2.2 Wire Protocol Negotiation
Clients negotiate their wire format upon connection using the subscription handshake:
- **`sbe` (Recommended for HFT):** Binary zero-copy Simple Binary Encoding frames. Eliminates string parsing, heap allocation, and garbage collection.
- **`json`:** Fast SIMD-friendly JSON format for dashboards and standard algorithmic engines.

### 2.3 Subscription Command Syntax (JSON over WebSocket)
Clients send a text frame containing the subscription payload:
```json
{
  "action": "subscribe",
  "venue_id": 1,
  "market_id": 1001,
  "stream_type": 1,
  "protocol": "sbe",
  "filter": {
    "min_notional_usd": 10000.0,
    "max_spread_bps": 25,
    "min_gas_priority_gwei": 30
  }
}
```

#### Field Reference:
- **`venue_id` (u8):**
  - `1`: Binance
  - `2`: Coinbase
  - `3`: Bybit
  - `4`: OKX
  - `101`: Uniswap v2
  - `102`: Uniswap v3
  - `201`: Raydium CLMM
  - `202`: Orca Whirlpools
- **`stream_type` (u8):**
  - `1`: Best Bid & Offer (L1 BBO)
  - `2`: Aggregated Order Book Depth (L2)
  - `3`: Individual Trades / Swaps (L3)
  - `4`: Speculative Mempool Shifts & Pending Swaps
- **`filter` (Optional):** Server-side predicate evaluation that suppresses market events below your threshold, preserving bandwidth and client CPU.

---

## 3. Simple Binary Encoding (SBE) Wire Format

When subscribing with `"protocol": "sbe"`, messages are dispatched as **WebSocket Binary Frames** (Opcode `0x2`).

### 3.1 SBE Message Header (8 Bytes, Little-Endian)
Every message begins with an 8-byte framing header:
```text
+-------------------+--------------------+-------------------+-------------------+
| blockLength (u16) | templateId (u16)   | schemaId (u16: 1) | version (u16: 1)  |
| Bytes [0..1]      | Bytes [2..3]       | Bytes [4..5]      | Bytes [6..7]      |
+-------------------+--------------------+-------------------+-------------------+
```

### 3.2 Template 101: Best Bid & Offer (`BBOReport`)
Total Frame Length: **92 Bytes** (8 bytes header + 84 bytes payload).

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
90      flags              uint16         0x01=AMM Synthetic, 0x02=Crossed
```

### 3.3 Template 104: Trade Execution / DEX Swap (`TradeExecutionReport`)
Total Frame Length: **95 Bytes** (8 bytes header + 87 bytes payload).

---

## 4. Nanosecond Telemetry Timestamps & Latency Accounting

Every market data frame includes four synchronized nanosecond timestamps:
$$\text{Internal Engine Latency} = T_2 - T_1$$
$$\text{Wire-to-Egress Latency} = T_3 - T_1$$
$$\text{Total Tick-to-Bot Latency} = T_{\text{client\_recv}} - T_0$$

This allows client bots to:
1. Detect stale quotes before sending fill requests.
2. Measure latency arbitrage windows against pending block times.
3. Automatically adjust gas priority tips for MEV execution.

---

## 5. Python Integration Example

```python
import asyncio
import struct
import websockets
import json

async def run_bot():
    uri = "ws://localhost:9001/v1/marketdata"
    async with websockets.connect(uri) as ws:
        # Subscribe to Binance BTC/USDT with SBE binary encoding
        sub_msg = {
            "action": "subscribe",
            "venue_id": 1,
            "market_id": 1001,
            "stream_type": 1,
            "protocol": "sbe",
            "filter": {
                "min_notional_usd": 5000.0,
                "max_spread_bps": 20
            }
        }
        await ws.send(json.dumps(sub_msg))
        print("Subscribed! Listening for SBE binary market ticks...")

        while True:
            msg = await ws.recv()
            if isinstance(msg, bytes):
                # Unpack 8-byte SBE Header
                block_len, template_id, schema_id, version = struct.unpack_from("<HHHH", msg, 0)
                
                if template_id == 101: # BBO Report
                    t0, t1, t2, t3, market_id, seq, bid_p, bid_q, ask_p, ask_q, spread = struct.unpack_from(
                        "<QQQQIQQqqqH", msg, 8
                    )
                    bid_usd = bid_p / 1e8
                    ask_usd = ask_p / 1e8
                    latency_us = (t2 - t1) / 1000.0
                    print(f"[BBO] Market: {market_id} | Bid: ${bid_usd:.2f} | Ask: ${ask_usd:.2f} | Engine Latency: {latency_us:.2f}µs")

if __name__ == "__main__":
    asyncio.run(run_bot())
```
