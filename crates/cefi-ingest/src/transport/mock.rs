//! # Mock Network Transport
//!
//! In-memory packet ingestion queue for deterministic testing, unit validation,
//! and high-speed simulation.

use std::collections::VecDeque;

use super::{NetworkTransport, TimestampNs, TransportBackendKind, TransportError, TransportStats};

/// In-memory queued synthetic packet
#[derive(Clone, Debug)]
pub struct MockPacket {
    pub payload: Vec<u8>,
    pub timestamp_ns: TimestampNs,
}

/// In-memory Mock Network Transport
pub struct MockNetworkTransport {
    queue: VecDeque<MockPacket>,
    stats: TransportStats,
    is_active: bool,
}

impl MockNetworkTransport {
    pub fn new() -> Self {
        Self {
            queue: VecDeque::new(),
            stats: TransportStats::default(),
            is_active: true,
        }
    }

    /// Enqueues a packet to be retrieved in the next poll_batch
    pub fn push_packet(&mut self, payload: &[u8], timestamp_ns: TimestampNs) {
        self.queue.push_back(MockPacket {
            payload: payload.to_vec(),
            timestamp_ns,
        });
    }

    pub fn set_active(&mut self, active: bool) {
        self.is_active = active;
    }

    pub fn queue_len(&self) -> usize {
        self.queue.len()
    }
}

impl Default for MockNetworkTransport {
    fn default() -> Self {
        Self::new()
    }
}

impl NetworkTransport for MockNetworkTransport {
    fn poll_batch(
        &mut self,
        max_batch: usize,
        handler: &mut dyn FnMut(&[u8], TimestampNs),
    ) -> Result<usize, TransportError> {
        if !self.is_active {
            return Err(TransportError::NotConnected);
        }

        self.stats.poll_calls += 1;

        if self.queue.is_empty() {
            self.stats.empty_polls += 1;
            return Ok(0);
        }

        let count = max_batch.min(self.queue.len());
        let mut processed = 0;
        let mut batch_bytes = 0;

        for _ in 0..count {
            if let Some(pkt) = self.queue.pop_front() {
                batch_bytes += pkt.payload.len() as u64;
                handler(&pkt.payload, pkt.timestamp_ns);
                processed += 1;
            }
        }

        self.stats.rx_packets += processed as u64;
        self.stats.rx_bytes += batch_bytes;

        Ok(processed)
    }

    fn stats(&self) -> TransportStats {
        self.stats.clone()
    }

    fn backend_kind(&self) -> TransportBackendKind {
        TransportBackendKind::Mock
    }

    fn is_active(&self) -> bool {
        self.is_active
    }

    fn as_raw_fd(&self) -> Option<i32> {
        None
    }
}
