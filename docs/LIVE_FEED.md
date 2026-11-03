# Live feed (Coinbase Exchange public WebSocket)

Crate: `crates/live-feed`. Binary: `live_feed`. Everything else in the workspace has no third-party dependencies;
this crate is the only one that does (`tokio`, `tokio-tungstenite`, `rustls`).

```bash
cargo run --release -p live-feed --bin live_feed -- --products BTC-USD,ETH-USD --seconds 30
```

Flags: `--products A,B`, `--seconds N` (0 = run until Ctrl-C), `--url U`, `--min-messages N` (exit 1 if fewer arrive).
No API key is needed or read.

## Why Coinbase

Binance.com answers HTTP 451 to US IP addresses (including GitHub's US runners), and the earlier README claims about
Binance/OKX/Bybit feeds were never backed by code that connects to them. The Coinbase Exchange `ticker` and `heartbeat`
channels are public and reachable from the US without credentials. The `level2` channel needs authentication, so
**this feed is L1 (best bid/ask) plus trades, not an order book.**

## Data path

```
wss frame (text JSON)
  -> Session::on_message          integrity checks, decides Action
  -> CefiParser::parse_coinbase_* zero-allocation field extraction -> NormalizedBbo / UnifiedTrade (1e-8 fixed point)
  -> SpscRingBuffer hand-off      (single-threaded in this binary; exercises the API)
  -> encode_bbo_sbe               92-byte SBE message
```

`t1` is the wall-clock time the frame reached user space after TLS and WebSocket framing. It is **not** a NIC or
hardware timestamp. "Exchange timestamp to local receive" in the summary includes network transit and clock skew between
Coinbase and the local machine, so it is only a rough sanity number.

## Failure handling

All decisions live in `Session` (`crates/live-feed/src/session.rs`), which has no sockets and no clock, so they are
unit-tested from fixtures.

| Situation | Detection | Response |
|---|---|---|
| Duplicate or reordered delivery | per-product `sequence` must strictly increase (ticker and heartbeat share the sequence space) | Resubscribe |
| Missed ticker message | `trade_id` on ticker is contiguous per product; `trade_id != previous + 1` | Resubscribe |
| Missed tail of stream | heartbeat `last_trade_id` is ahead of the last ticker we processed | Resubscribe |
| Silent / half-open connection | no message of any kind for 10 s (heartbeats arrive about once per second per product) | Reconnect |
| Server `error` message, TCP close, read error | message type / stream end / `Err` | Reconnect |
| Reconnect loop | exponential backoff 1 s, 2 s, 4 s ... capped at 30 s, reset after the first message of a connection | |
| Malformed message, unknown product | counted in `Stats`, otherwise ignored | |

**Resubscribe = send `unsubscribe` then `subscribe` on the same socket.** Coinbase replies with a snapshot ticker that
repeats the latest trade (verified against the live feed on 2026-11-03). That ticker re-baselines the sequence and
trade-id state. The repeated trade is not emitted twice.

Note on why we check `trade_id` and not only `sequence`: the ticker channel only carries match events, while the
`sequence` counter also advances for orders that never appear on this channel, so `sequence` has legitimate holes.
`trade_id` has none (verified on the recorded fixture: 135 tickers, 0 holes).

### Known gaps in this handling

- A heartbeat can in principle be delivered ahead of the ticker it refers to; if that happens we resubscribe
  unnecessarily (harmless but noisy). It did not occur in the recorded fixture.
- There is no persistence or replay of missed trades: a gap triggers a fresh snapshot, and the trades in the gap are lost.
- One connection, one thread. No multi-venue support.

## Tests

`crates/live-feed/tests/session_fixtures.rs` replays `tests/fixtures/coinbase_ticker_btc_eth_recorded.jsonl`, a real
12-second capture (158 messages, BTC-USD and ETH-USD) recorded on 2026-11-03. Failure cases (dropped ticker, duplicated
ticker, lost tail, silence, malformed input) are produced by editing that recording inside the test; they are synthetic
and the test names say so. The CI job `live-connect` runs the real binary against the real feed for 20 seconds and is
`continue-on-error`, because it depends on the network.
