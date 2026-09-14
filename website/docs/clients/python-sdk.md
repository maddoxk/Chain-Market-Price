---
id: python-sdk
title: Python 3.11+ Memory-Mapped Feed Reader
sidebar_label: Python SDK
---

# Python 3.11+ Memory-Mapped Feed Reader

For quantitative researchers, Jupyter research environments, and Python-based algorithmic trading systems, [`examples/python/hft_alpha_feed.py`](https://github.com/maddoxk/Chain-Market-Price/blob/main/examples/python/hft_alpha_feed.py) provides zero-copy memory-mapped access directly to the POSIX shared memory buffer.

---

## 1. Quick Start

```python
from examples.python.hft_alpha_feed import ShmFeedReader

reader = ShmFeedReader("/dev/shm/cmp_market_data.shm")

if not reader.connect():
    print("Could not connect to shared memory ring!")
    exit(1)

print("Connected to CMP Shared Memory! Polling for market ticks...")

while True:
    tick = reader.read_tick()
    if tick:
        print(f"Seq: {tick.sequence} | Venue: {tick.venue_id} | Price: ${tick.price:.2f} | "
              f"Wire Latency: {tick.telemetry.wire_to_egress_latency_ns} ns")
```

---

## 2. High-Performance WebSocket Consumer (SBE Binary Frames)

```python
import asyncio
import struct
import websockets
import json

async def run_alpha_feed():
    uri = "ws://localhost:9001/v1/marketdata"
    async with websockets.connect(uri) as ws:
        # Subscribe to Binance BTC/USDT using binary SBE
        await ws.send(json.dumps({
            "action": "subscribe",
            "venue_id": 1,
            "market_id": 1001,
            "stream_type": 1,
            "protocol": "sbe",
            "filter": { "min_notional_usd": 10000.0 }
        }))
        print("Subscribed! Receiving SBE frames...")

        while True:
            frame = await ws.recv()
            if isinstance(frame, bytes):
                block_len, template_id, schema, ver = struct.unpack_from("<HHHH", frame, 0)
                if template_id == 101: # BBO Report
                    t0, t1, t2, t3, mkt, seq, bid_p, bid_q, ask_p, ask_q, spread = struct.unpack_from(
                        "<QQQQIQQqqqH", frame, 8
                    )
                    print(f"[BBO] Market {mkt} | Bid: ${bid_p / 1e8:.2f} | Ask: ${ask_p / 1e8:.2f} | "
                          f"Engine Latency: {(t2 - t1) / 1000:.2f} µs")

if __name__ == "__main__":
    asyncio.run(run_alpha_feed())
```
