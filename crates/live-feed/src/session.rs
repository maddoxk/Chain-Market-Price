//! Message-level state machine for the Coinbase Exchange `ticker` + `heartbeat` channels.
//!
//! # What is checked, and why
//!
//! Coinbase gives every product a `sequence` number shared by all its channels, and a
//! `trade_id` that is contiguous per product. The `ticker` channel emits one message per
//! match, so its `sequence` skips (other order events are not on this channel) but its
//! `trade_id` does not. That gives us two usable integrity checks:
//!
//! 1. **Sequence regression**: `sequence` must strictly increase per product. A repeat or a
//!    decrease means duplicated or reordered delivery.
//! 2. **Trade-id gap**: for each ticker, `trade_id` must be exactly `previous + 1`. A jump means we
//!    missed one or more ticker messages. A heartbeat carries `last_trade_id`; if it is ahead
//!    of the last ticker we saw, we missed the *tail* of the stream (which a per-message check
//!    cannot see until the next ticker arrives).
//!
//! Either failure asks the runner to **resubscribe**. Coinbase answers a fresh subscription
//! with a snapshot ticker (the latest ticker, repeating the last `trade_id`), which becomes
//! the new baseline. A silent connection (no message at all for `stale_after_ms`) or an
//! `error` message asks for a full **reconnect**.
//!
//! The `ticker` channel has no depth, so this is an L1 quote + trade feed, not an order book.

use cefi_ingest::CefiParser;
use ingest_models::{NormalizedBbo, UnifiedTrade};

/// Normalized output of one message.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Event {
    Bbo(NormalizedBbo),
    Trade(UnifiedTrade),
}

/// Why the session wants the runner to do something.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Reason {
    SequenceRegression { last: u64, got: u64 },
    TradeIdGap { expected: u64, got: u64 },
    HeartbeatBehindExchange { ours: u64, exchange: u64 },
    ServerError,
    Stale,
    Disconnected,
}

/// What the runner must do next.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Action {
    /// Send unsubscribe + subscribe on the existing socket to get a fresh snapshot.
    Resubscribe(Reason),
    /// Drop the socket and reconnect (with backoff).
    Reconnect(Reason),
}

#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct Stats {
    pub messages: u64,
    pub tickers: u64,
    pub heartbeats: u64,
    pub events_emitted: u64,
    pub malformed: u64,
    pub unknown_product: u64,
    pub sequence_regressions: u64,
    pub trade_id_gaps: u64,
    pub heartbeat_behind: u64,
    pub resubscribes: u64,
    pub reconnects: u64,
}

#[derive(Debug, Clone)]
struct Product {
    name: String,
    market_id: u32,
    /// Highest `sequence` seen on this connection/subscription; `None` until the baseline.
    last_seq: Option<u64>,
    /// Last `trade_id` seen on this connection/subscription; `None` until the baseline.
    last_trade_id: Option<u64>,
    /// Highest trade_id we have emitted as a `Trade` event, kept across resubscribes so the
    /// snapshot ticker (which repeats the last trade) is not emitted twice.
    max_emitted_trade_id: u64,
}

pub struct Session {
    products: Vec<Product>,
    last_message_ms: u64,
    stale_after_ms: u64,
    pub stats: Stats,
}

impl Session {
    /// `products` are Coinbase product ids such as `BTC-USD`. `market_id` is the 1-based index.
    pub fn new(products: &[&str], stale_after_ms: u64) -> Self {
        Self {
            products: products
                .iter()
                .enumerate()
                .map(|(i, p)| Product {
                    name: (*p).to_string(),
                    market_id: i as u32 + 1,
                    last_seq: None,
                    last_trade_id: None,
                    max_emitted_trade_id: 0,
                })
                .collect(),
            last_message_ms: 0,
            stale_after_ms,
            stats: Stats::default(),
        }
    }

    pub fn product_name(&self, market_id: u32) -> Option<&str> {
        self.products
            .get((market_id as usize).wrapping_sub(1))
            .map(|p| p.name.as_str())
    }

    /// Call after each (re)connect. Clears baselines so the first ticker after subscribing is
    /// treated as a snapshot, and arms the staleness watchdog.
    pub fn on_connected(&mut self, now_ms: u64) {
        self.reset_baselines();
        self.last_message_ms = now_ms;
    }

    fn reset_baselines(&mut self) {
        for p in &mut self.products {
            p.last_seq = None;
            p.last_trade_id = None;
        }
    }

    /// Called by the runner after it has acted on an [`Action::Resubscribe`] /
    /// [`Action::Reconnect`], for bookkeeping.
    pub fn note_action_taken(&mut self, action: Action) {
        match action {
            Action::Resubscribe(_) => self.stats.resubscribes += 1,
            Action::Reconnect(_) => self.stats.reconnects += 1,
        }
        self.reset_baselines();
    }

    /// Periodic watchdog. Coinbase sends a heartbeat per product about once a second, so a
    /// long silence means a dead connection even if TCP has not noticed.
    pub fn on_tick(&self, now_ms: u64) -> Option<Action> {
        if now_ms.saturating_sub(self.last_message_ms) > self.stale_after_ms {
            Some(Action::Reconnect(Reason::Stale))
        } else {
            None
        }
    }

    /// Handles one text frame. Normalized events are appended to `out`.
    /// `t1_ns` is the wall-clock time the frame was received in user space.
    pub fn on_message(
        &mut self,
        payload: &[u8],
        t1_ns: u64,
        now_ms: u64,
        out: &mut Vec<Event>,
    ) -> Option<Action> {
        self.stats.messages += 1;
        self.last_message_ms = now_ms;

        let ty = match CefiParser::coinbase_message_type(payload) {
            Some(t) => t,
            None => {
                self.stats.malformed += 1;
                return None;
            }
        };

        match ty {
            b"ticker" => self.on_ticker(payload, t1_ns, out),
            b"heartbeat" => self.on_heartbeat(payload),
            b"error" => Some(Action::Reconnect(Reason::ServerError)),
            // "subscriptions" acks and anything else we did not ask for.
            _ => None,
        }
    }

    fn product_index(&mut self, payload: &[u8]) -> Option<usize> {
        let id = match CefiParser::coinbase_product_id(payload) {
            Some(id) => id,
            None => {
                self.stats.malformed += 1;
                return None;
            }
        };
        let idx = self.products.iter().position(|p| p.name.as_bytes() == id);
        if idx.is_none() {
            self.stats.unknown_product += 1;
        }
        idx
    }

    fn on_ticker(&mut self, payload: &[u8], t1_ns: u64, out: &mut Vec<Event>) -> Option<Action> {
        self.stats.tickers += 1;
        let idx = self.product_index(payload)?;
        let market_id = self.products[idx].market_id;

        let bbo = CefiParser::parse_coinbase_ticker_bbo(market_id, payload, t1_ns);
        let trade = CefiParser::parse_coinbase_ticker_trade(market_id, payload, t1_ns);
        let (bbo, trade) = match (bbo, trade) {
            (Ok(b), Ok(t)) => (b, t),
            _ => {
                self.stats.malformed += 1;
                return None;
            }
        };

        let p = &mut self.products[idx];
        let mut action = None;

        // Check 1: sequence must strictly increase.
        if let Some(last) = p.last_seq {
            if bbo.sequence <= last {
                self.stats.sequence_regressions += 1;
                action = Some(Action::Resubscribe(Reason::SequenceRegression {
                    last,
                    got: bbo.sequence,
                }));
            }
        }
        // Check 2: trade_id must be contiguous.
        if let Some(last) = p.last_trade_id {
            let expected = last + 1;
            if trade.trade_id != expected && action.is_none() {
                self.stats.trade_id_gaps += 1;
                action = Some(Action::Resubscribe(Reason::TradeIdGap {
                    expected,
                    got: trade.trade_id,
                }));
            }
        }

        // The data in this message is still valid, so emit it even when we also ask for a
        // resubscribe. Only the Trade is deduplicated (snapshot repeats the last trade).
        p.last_seq = Some(bbo.sequence);
        p.last_trade_id = Some(trade.trade_id);
        out.push(Event::Bbo(bbo));
        self.stats.events_emitted += 1;
        if trade.trade_id > p.max_emitted_trade_id {
            p.max_emitted_trade_id = trade.trade_id;
            out.push(Event::Trade(trade));
            self.stats.events_emitted += 1;
        }
        action
    }

    fn on_heartbeat(&mut self, payload: &[u8]) -> Option<Action> {
        self.stats.heartbeats += 1;
        let idx = self.product_index(payload)?;
        let seq = CefiParser::coinbase_sequence(payload)?;
        let exchange_last_trade = CefiParser::coinbase_trade_id(payload)?;
        let p = &mut self.products[idx];

        if let Some(last) = p.last_seq {
            if seq <= last {
                self.stats.sequence_regressions += 1;
                return Some(Action::Resubscribe(Reason::SequenceRegression {
                    last,
                    got: seq,
                }));
            }
            p.last_seq = Some(seq);
        }
        // If the exchange says trade N happened and our newest is older, a ticker was lost.
        if let Some(ours) = p.last_trade_id {
            if exchange_last_trade > ours {
                self.stats.heartbeat_behind += 1;
                return Some(Action::Resubscribe(Reason::HeartbeatBehindExchange {
                    ours,
                    exchange: exchange_last_trade,
                }));
            }
        }
        None
    }
}
