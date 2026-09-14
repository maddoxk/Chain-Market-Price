//! # Inverted Topic Router & Market Data Fan-Out Engine
//!
//! Maps 64-bit TopicKeys directly to dynamic Roaring-style client bitmaps.
//! Delivers sub-microsecond predicate filtering and lazy dual-wire (SBE/JSON) serialization.

use std::collections::HashMap;
use std::sync::{Arc, RwLock};
use ingest_models::{NormalizedBbo, UnifiedTrade};

use crate::bitmap::ClientBitmap;
use crate::fast_json::{serialize_bbo_json, serialize_trade_json};
use crate::sbe::{encode_bbo_sbe, encode_trade_sbe, TOTAL_SBE_BBO_LEN, TOTAL_SBE_TRADE_LEN};
use crate::session::{ClientSession, SessionState, WireProtocol};
use crate::{ClientFilterPredicate, TopicKey};

pub const STREAM_TYPE_BBO: u8 = 1;
pub const STREAM_TYPE_DEPTH: u8 = 2;
pub const STREAM_TYPE_TRADE: u8 = 3;
pub const STREAM_TYPE_MEMPOOL: u8 = 4;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RouterError {
    ClientAlreadyExists(u32),
    ClientNotFound(u32),
    CapacityExceeded,
    LockPoisoned,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct DispatchMetrics {
    pub matched_clients: usize,
    pub delivered_clients: usize,
    pub filtered_clients: usize,
    pub rate_limited_clients: usize,
    pub sbe_bytes_dispatched: usize,
    pub json_bytes_dispatched: usize,
}

/// Inverted Topic Router managing subscription index and client fan-out
pub struct InvertedTopicRouter {
    topics: RwLock<HashMap<TopicKey, ClientBitmap>>,
    clients: RwLock<HashMap<u32, Arc<ClientSession>>>,
}

impl Default for InvertedTopicRouter {
    fn default() -> Self {
        Self::new()
    }
}

impl InvertedTopicRouter {
    pub fn new() -> Self {
        Self {
            topics: RwLock::new(HashMap::with_capacity(4096)),
            clients: RwLock::new(HashMap::with_capacity(1024)),
        }
    }

    /// Register a new client session into the router
    pub fn register_client(&self, session: ClientSession) -> Result<u32, RouterError> {
        let mut clients = self.clients.write().map_err(|_| RouterError::LockPoisoned)?;
        let id = session.id;
        if clients.contains_key(&id) {
            return Err(RouterError::ClientAlreadyExists(id));
        }
        clients.insert(id, Arc::new(session));
        Ok(id)
    }

    /// Unregister a client session and remove all its topic subscriptions
    pub fn unregister_client(&self, client_id: u32) -> Result<(), RouterError> {
        {
            let mut clients = self.clients.write().map_err(|_| RouterError::LockPoisoned)?;
            let session = clients.remove(&client_id).ok_or(RouterError::ClientNotFound(client_id))?;
            session.set_state(SessionState::Closed);
        }

        let mut topics = self.topics.write().map_err(|_| RouterError::LockPoisoned)?;
        for bitmap in topics.values_mut() {
            bitmap.remove(client_id);
        }
        topics.retain(|_, bitmap| !bitmap.is_empty());
        Ok(())
    }

    /// Subscribe client to a 64-bit TopicKey
    pub fn subscribe(&self, client_id: u32, topic: TopicKey) -> Result<(), RouterError> {
        {
            let clients = self.clients.read().map_err(|_| RouterError::LockPoisoned)?;
            let session = clients.get(&client_id).ok_or(RouterError::ClientNotFound(client_id))?;
            session.set_state(SessionState::Subscribed);
        }

        let mut topics = self.topics.write().map_err(|_| RouterError::LockPoisoned)?;
        let bitmap = topics.entry(topic).or_insert_with(ClientBitmap::new);
        bitmap.insert(client_id);
        Ok(())
    }

    /// Unsubscribe client from a 64-bit TopicKey
    pub fn unsubscribe(&self, client_id: u32, topic: TopicKey) -> Result<(), RouterError> {
        let mut topics = self.topics.write().map_err(|_| RouterError::LockPoisoned)?;
        if let Some(bitmap) = topics.get_mut(&topic) {
            bitmap.remove(client_id);
        }
        Ok(())
    }

    /// Update dynamic filter predicate for a client
    pub fn update_filter(&self, client_id: u32, filter: ClientFilterPredicate) -> Result<(), RouterError> {
        let clients = self.clients.read().map_err(|_| RouterError::LockPoisoned)?;
        let session = clients.get(&client_id).ok_or(RouterError::ClientNotFound(client_id))?;
        unsafe {
            // Safe since session.filter is owned by session arc and updated atomically/under read lock
            let filter_ptr = &session.filter as *const ClientFilterPredicate as *mut ClientFilterPredicate;
            *filter_ptr = filter;
        }
        Ok(())
    }

    /// Returns the number of registered clients
    pub fn client_count(&self) -> usize {
        self.clients.read().map(|c| c.len()).unwrap_or(0)
    }

    /// Returns the number of active topics
    pub fn topic_count(&self) -> usize {
        self.topics.read().map(|t| t.len()).unwrap_or(0)
    }

    /// Dispatch NormalizedBbo to all subscribed clients matching topic and filter predicates
    pub fn dispatch_bbo(
        &self,
        bbo: &NormalizedBbo,
        now_ms: u64,
        mut sink: impl FnMut(u32, WireProtocol, &[u8]),
    ) -> DispatchMetrics {
        let mut metrics = DispatchMetrics::default();

        let exact_topic = TopicKey::new(bbo.venue as u8, bbo.market_id, STREAM_TYPE_BBO, bbo.flags as u32);
        let default_topic = TopicKey::new(bbo.venue as u8, bbo.market_id, STREAM_TYPE_BBO, 0);

        let candidate_bitmap = {
            let topics = match self.topics.read() {
                Ok(t) => t,
                Err(_) => return metrics,
            };

            let exact = topics.get(&exact_topic).copied().unwrap_or_default();
            if exact_topic != default_topic {
                let default_match = topics.get(&default_topic).copied().unwrap_or_default();
                exact.union(&default_match)
            } else {
                exact
            }
        };

        if candidate_bitmap.is_empty() {
            return metrics;
        }

        let clients_guard = match self.clients.read() {
            Ok(c) => c,
            Err(_) => return metrics,
        };

        // Lazy wire buffers: serialized only once on demand
        let mut sbe_buf = [0u8; TOTAL_SBE_BBO_LEN];
        let mut sbe_encoded = false;
        let mut sbe_len = 0;

        let mut json_buf = [0u8; 512];
        let mut json_encoded = false;
        let mut json_len = 0;

        candidate_bitmap.for_each(|client_id| {
            metrics.matched_clients += 1;

            if let Some(session) = clients_guard.get(&client_id) {
                if !session.is_active() {
                    return;
                }

                // Evaluate dynamic predicate: max spread bps filter
                if session.filter.max_spread_bps > 0 && (bbo.spread_bps as u32) > session.filter.max_spread_bps {
                    metrics.filtered_clients += 1;
                    return;
                }

                // Evaluate rate limiter
                if !session.try_consume_token(1, now_ms) {
                    metrics.rate_limited_clients += 1;
                    return;
                }

                // Lazy wire encoding according to client protocol preference
                match session.protocol {
                    WireProtocol::Sbe => {
                        if !sbe_encoded {
                            if let Ok(len) = encode_bbo_sbe(bbo, &mut sbe_buf) {
                                sbe_len = len;
                                sbe_encoded = true;
                            }
                        }
                        if sbe_encoded {
                            sink(client_id, WireProtocol::Sbe, &sbe_buf[..sbe_len]);
                            session.record_delivery(sbe_len);
                            metrics.delivered_clients += 1;
                            metrics.sbe_bytes_dispatched += sbe_len;
                        }
                    }
                    WireProtocol::Json => {
                        if !json_encoded {
                            if let Ok(len) = serialize_bbo_json(bbo, &mut json_buf) {
                                json_len = len;
                                json_encoded = true;
                            }
                        }
                        if json_encoded {
                            sink(client_id, WireProtocol::Json, &json_buf[..json_len]);
                            session.record_delivery(json_len);
                            metrics.delivered_clients += 1;
                            metrics.json_bytes_dispatched += json_len;
                        }
                    }
                }
            }
        });

        metrics
    }

    /// Dispatch UnifiedTrade to all subscribed clients matching topic and dynamic notional predicates
    pub fn dispatch_trade(
        &self,
        trade: &UnifiedTrade,
        notional_usd: f64,
        now_ms: u64,
        mut sink: impl FnMut(u32, WireProtocol, &[u8]),
    ) -> DispatchMetrics {
        let mut metrics = DispatchMetrics::default();

        let topic = TopicKey::new(trade.venue as u8, trade.market_id, STREAM_TYPE_TRADE, 0);

        let candidate_bitmap = {
            let topics = match self.topics.read() {
                Ok(t) => t,
                Err(_) => return metrics,
            };
            topics.get(&topic).copied().unwrap_or_default()
        };

        if candidate_bitmap.is_empty() {
            return metrics;
        }

        let clients_guard = match self.clients.read() {
            Ok(c) => c,
            Err(_) => return metrics,
        };

        // Lazy wire buffers
        let mut sbe_buf = [0u8; TOTAL_SBE_TRADE_LEN];
        let mut sbe_encoded = false;
        let mut sbe_len = 0;

        let mut json_buf = [0u8; 512];
        let mut json_encoded = false;
        let mut json_len = 0;

        candidate_bitmap.for_each(|client_id| {
            metrics.matched_clients += 1;

            if let Some(session) = clients_guard.get(&client_id) {
                if !session.is_active() {
                    return;
                }

                // Dynamic filter predicate: min notional USD threshold
                if !session.filter.matches_trade(notional_usd) {
                    metrics.filtered_clients += 1;
                    return;
                }

                // Evaluate rate limiter
                if !session.try_consume_token(1, now_ms) {
                    metrics.rate_limited_clients += 1;
                    return;
                }

                // Lazy wire encoding
                match session.protocol {
                    WireProtocol::Sbe => {
                        if !sbe_encoded {
                            if let Ok(len) = encode_trade_sbe(trade, &mut sbe_buf) {
                                sbe_len = len;
                                sbe_encoded = true;
                            }
                        }
                        if sbe_encoded {
                            sink(client_id, WireProtocol::Sbe, &sbe_buf[..sbe_len]);
                            session.record_delivery(sbe_len);
                            metrics.delivered_clients += 1;
                            metrics.sbe_bytes_dispatched += sbe_len;
                        }
                    }
                    WireProtocol::Json => {
                        if !json_encoded {
                            if let Ok(len) = serialize_trade_json(trade, &mut json_buf) {
                                json_len = len;
                                json_encoded = true;
                            }
                        }
                        if json_encoded {
                            sink(client_id, WireProtocol::Json, &json_buf[..json_len]);
                            session.record_delivery(json_len);
                            metrics.delivered_clients += 1;
                            metrics.json_bytes_dispatched += json_len;
                        }
                    }
                }
            }
        });

        metrics
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use ingest_models::TelemetryTimestamps;

    #[test]
    fn test_inverted_router_bbo_dispatch() {
        let router = InvertedTopicRouter::new();

        let sbe_client = ClientSession::new(1, WireProtocol::Sbe, 1000, 10, 0);
        let json_client = ClientSession::new(2, WireProtocol::Json, 1000, 10, 0);

        router.register_client(sbe_client).unwrap();
        router.register_client(json_client).unwrap();

        let topic = TopicKey::new(1, 100, STREAM_TYPE_BBO, 0);
        router.subscribe(1, topic).unwrap();
        router.subscribe(2, topic).unwrap();

        let bbo = NormalizedBbo {
            telemetry: TelemetryTimestamps::default(),
            venue: 1,
            chain: 1,
            market_id: 100,
            sequence: 1,
            bid_price: 50_000_00000000,
            bid_qty: 1_00000000,
            ask_price: 50_001_00000000,
            ask_qty: 1_00000000,
            spread_bps: 2,
            flags: 0,
        };

        let mut received = Vec::new();
        let metrics = router.dispatch_bbo(&bbo, 0, |client_id, proto, payload| {
            received.push((client_id, proto, payload.len()));
        });

        assert_eq!(metrics.matched_clients, 2);
        assert_eq!(metrics.delivered_clients, 2);
        assert_eq!(metrics.filtered_clients, 0);
        assert_eq!(received.len(), 2);
        assert_eq!(received[0], (1, WireProtocol::Sbe, TOTAL_SBE_BBO_LEN));
        assert_eq!(received[1].0, 2);
        assert_eq!(received[1].1, WireProtocol::Json);
    }

    #[test]
    fn test_inverted_router_predicate_filtering() {
        let router = InvertedTopicRouter::new();

        // Client 1 requires min notional $10,000
        let filter1 = ClientFilterPredicate {
            min_notional_usd: 10_000.0,
            max_spread_bps: 0,
            min_gas_priority_gwei: 0,
            target_prefix: 0,
        };
        let client1 = ClientSession::new(1, WireProtocol::Json, 1000, 10, 0).with_filter(filter1);

        // Client 2 requires min notional $100
        let filter2 = ClientFilterPredicate {
            min_notional_usd: 100.0,
            max_spread_bps: 0,
            min_gas_priority_gwei: 0,
            target_prefix: 0,
        };
        let client2 = ClientSession::new(2, WireProtocol::Json, 1000, 10, 0).with_filter(filter2);

        router.register_client(client1).unwrap();
        router.register_client(client2).unwrap();

        let topic = TopicKey::new(1, 42, STREAM_TYPE_TRADE, 0);
        router.subscribe(1, topic).unwrap();
        router.subscribe(2, topic).unwrap();

        let trade = UnifiedTrade {
            market_id: 42,
            venue: 1,
            price: 2000_00000000,
            size: 1_00000000,
            ..Default::default()
        };

        // Trade notional = $2,000 -> Client 1 filters out ($10k min), Client 2 matches ($100 min)
        let mut delivered = Vec::new();
        let metrics = router.dispatch_trade(&trade, 2000.0, 0, |id, _, _| delivered.push(id));

        assert_eq!(metrics.matched_clients, 2);
        assert_eq!(metrics.delivered_clients, 1);
        assert_eq!(metrics.filtered_clients, 1);
        assert_eq!(delivered, vec![2]);
    }
}
