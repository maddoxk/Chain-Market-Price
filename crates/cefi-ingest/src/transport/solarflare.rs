//! # Solarflare EF_VI Transport Adapter (Tier 3 Bare Metal)
//!
//! Provides zero-syscall direct userspace DMA MMIO packet ingress
//! leveraging Solarflare `ef_vi` and DPDK kernel-bypass primitives.

use core_engine::kernel_bypass::{
    BypassBackendKind, BypassConfig, KernelBypassBridge, DEFAULT_RING_CAPACITY,
};
use std::time::SystemTime;

use super::{NetworkTransport, TimestampNs, TransportBackendKind, TransportError, TransportStats};

/// Tier 3: Solarflare EF_VI Kernel-Bypass Direct Userspace DMA Transport
pub struct SolarflareEfViTransport {
    bridge: KernelBypassBridge<DEFAULT_RING_CAPACITY>,
    interface_name: String,
    bind_addr: String,
    stats: TransportStats,
    is_active: bool,
}

impl SolarflareEfViTransport {
    /// Initializes a new Solarflare EF_VI transport adapter
    pub fn new(interface_name: &str, bind_addr: &str) -> Self {
        let config = BypassConfig {
            backend: BypassBackendKind::SolarflareEfVi,
            ring_capacity: DEFAULT_RING_CAPACITY,
            interface_id: 0,
            enable_hardware_timestamping: true,
            promiscuous_mode: false,
        };

        Self {
            bridge: KernelBypassBridge::new(config),
            interface_name: interface_name.to_string(),
            bind_addr: bind_addr.to_string(),
            stats: TransportStats::default(),
            is_active: true,
        }
    }

    /// Injects a packet into the DMA RX ring (used in testing and synthetic replay)
    pub fn inject_packet(
        &mut self,
        payload: &[u8],
        hw_timestamp_ns: TimestampNs,
    ) -> Result<(), TransportError> {
        self.bridge
            .inject_packet(payload, hw_timestamp_ns)
            .map_err(|e| TransportError::BufferOverflow(e.to_string()))
    }

    pub fn interface_name(&self) -> &str {
        &self.interface_name
    }

    pub fn bind_addr(&self) -> &str {
        &self.bind_addr
    }
}

impl NetworkTransport for SolarflareEfViTransport {
    #[inline(always)]
    fn poll_batch(
        &mut self,
        max_batch: usize,
        handler: &mut dyn FnMut(&[u8], TimestampNs),
    ) -> Result<usize, TransportError> {
        if !self.is_active {
            return Err(TransportError::NotConnected);
        }

        self.stats.poll_calls += 1;
        let mut batch_packets = 0;
        let mut batch_bytes = 0;

        let polled = self.bridge.poll_rx_burst(max_batch, |payload, hw_ts| {
            let ts = if hw_ts > 0 {
                hw_ts
            } else {
                SystemTime::now()
                    .duration_since(SystemTime::UNIX_EPOCH)
                    .unwrap_or_default()
                    .as_nanos() as u64
            };
            batch_packets += 1;
            batch_bytes += payload.len() as u64;
            handler(payload, ts);
        });

        if polled == 0 {
            self.stats.empty_polls += 1;
        } else {
            self.stats.rx_packets += batch_packets;
            self.stats.rx_bytes += batch_bytes;
        }

        Ok(polled)
    }

    fn stats(&self) -> TransportStats {
        self.stats.clone()
    }

    fn backend_kind(&self) -> TransportBackendKind {
        TransportBackendKind::SolarflareEfVi
    }

    fn is_active(&self) -> bool {
        self.is_active && self.bridge.is_active()
    }

    fn as_raw_fd(&self) -> Option<i32> {
        None // Userspace MMIO direct DMA ring has no kernel file descriptor
    }
}
