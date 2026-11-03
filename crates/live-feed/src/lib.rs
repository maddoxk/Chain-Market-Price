//! Live market-data ingest for the Coinbase Exchange public WebSocket feed.
//!
//! This crate is deliberately separate from the zero-dependency core crates. It has two parts:
//!
//! * [`session`]: a pure state machine (no sockets, no clock) that decides what to do with each
//!   message: emit normalized events, request a resubscribe, or request a reconnect.
//!   Everything interesting is tested from recorded message fixtures.
//! * `bin/live_feed.rs`: the thin async runner that owns the socket (tokio + tokio-tungstenite).

pub mod session;
