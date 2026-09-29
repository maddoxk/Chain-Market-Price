//! # Modular Network Transport Layer
//!
//! Provides a zero-cost polymorphic network ingress abstraction:
//! - **Tier 3 (Enterprise Bare Metal):** `SolarflareEfViTransport` with zero-syscall userspace MMIO and DMA.
//! - **Tier 2 (Cloud Virtualized):** `IoUringTransport` with batched SQ/CQ rings and fixed buffer pools.
//! - **Tier 1 (Mid-Range Workstation):** `StandardSocketTransport` with non-blocking BSD UDP sockets and `SO_BUSY_POLL`.
//! - **Deterministic Simulation:** `MockNetworkTransport` for high-speed deterministic unit testing and playback.

pub mod io_uring;
pub mod mock;
pub mod socket;
pub mod solarflare;

use core_engine::topology::{HardwareProbe, HardwareTier};
use std::fmt;

pub use self::io_uring::IoUringTransport;
pub use self::mock::MockNetworkTransport;
pub use self::socket::StandardSocketTransport;
pub use self::solarflare::SolarflareEfViTransport;

/// Nanosecond ingress hardware/kernel timestamp ($T_1$)
pub type TimestampNs = u64;

/// Supported network transport backends
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum TransportBackendKind {
    /// Solarflare EF_VI / DPDK kernel-bypass direct userspace DMA
    SolarflareEfVi,
    /// Linux io_uring asynchronous batched completion rings
    IoUring,
    /// Cross-platform standard non-blocking BSD sockets
    StandardSocket,
    /// In-memory mock transport for deterministic testing
    Mock,
}

impl TransportBackendKind {
    pub fn as_str(&self) -> &'static str {
        match self {
            TransportBackendKind::SolarflareEfVi => "Solarflare_EF_VI",
            TransportBackendKind::IoUring => "Linux_io_uring",
            TransportBackendKind::StandardSocket => "Standard_BSD_Socket",
            TransportBackendKind::Mock => "Mock_Synthetic",
        }
    }
}

impl fmt::Display for TransportBackendKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.as_str())
    }
}

/// Real-time telemetry statistics for network transport ingress
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct TransportStats {
    pub rx_packets: u64,
    pub rx_bytes: u64,
    pub rx_dropped: u64,
    pub ring_overruns: u64,
    pub poll_calls: u64,
    pub empty_polls: u64,
}

/// Transport operation errors
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TransportError {
    Io(String),
    BufferOverflow(String),
    UnsupportedPlatform(String),
    DeviceNotFound(String),
    NotConnected,
    PermissionDenied,
}

impl fmt::Display for TransportError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            TransportError::Io(msg) => write!(f, "I/O Error: {}", msg),
            TransportError::BufferOverflow(msg) => write!(f, "Buffer Overflow: {}", msg),
            TransportError::UnsupportedPlatform(msg) => write!(f, "Unsupported Platform: {}", msg),
            TransportError::DeviceNotFound(msg) => write!(f, "Device Not Found: {}", msg),
            TransportError::NotConnected => write!(f, "Transport is not connected or active"),
            TransportError::PermissionDenied => {
                write!(f, "Permission denied for raw/bypass socket")
            }
        }
    }
}

impl std::error::Error for TransportError {}

/// Zero-cost polymorphic abstraction for market data network ingress
pub trait NetworkTransport: Send {
    /// Polls a batch of incoming datagrams/packets directly into pre-allocated memory.
    /// Passes payload slices and nanosecond timestamps directly to the handler without heap allocations.
    fn poll_batch(
        &mut self,
        max_batch: usize,
        handler: &mut dyn FnMut(&[u8], TimestampNs),
    ) -> Result<usize, TransportError>;

    /// Returns cumulative telemetry statistics
    fn stats(&self) -> TransportStats;

    /// Identifies the underlying concrete transport backend
    fn backend_kind(&self) -> TransportBackendKind;

    /// Queries whether the transport interface is currently active
    fn is_active(&self) -> bool;

    /// Returns the underlying raw file descriptor if supported by the OS/backend
    fn as_raw_fd(&self) -> Option<i32>;
}

/// Configuration options for configuring network transport ingestion
#[derive(Debug, Clone)]
pub struct TransportConfig {
    pub bind_addr: String,
    pub interface: String,
    pub preferred_backend: Option<TransportBackendKind>,
    pub batch_size: usize,
    pub enable_busy_poll: bool,
    pub ring_capacity: usize,
}

impl Default for TransportConfig {
    fn default() -> Self {
        Self {
            bind_addr: "0.0.0.0:0".to_string(),
            interface: "eth0".to_string(),
            preferred_backend: None,
            batch_size: 64,
            enable_busy_poll: false,
            ring_capacity: 2048,
        }
    }
}

/// Factory for auto-negotiating and instantiating the optimal network transport
pub struct TransportFactory;

impl TransportFactory {
    /// Auto-detects hardware topology and instantiates the fastest available transport
    pub fn create_optimal_transport(
        config: &TransportConfig,
        probe: &dyn HardwareProbe,
    ) -> Result<Box<dyn NetworkTransport>, TransportError> {
        // 1. If explicit preferred backend is configured, attempt initialization
        if let Some(preferred) = config.preferred_backend {
            return Self::create_explicit(preferred, config);
        }

        // 2. Hardware profile auto-negotiation based on probed tier
        let profile = probe.negotiate_profile(None);

        match profile.tier {
            HardwareTier::Tier3EnterpriseBareMetal => {
                // Tier 3: Attempt Solarflare EF_VI kernel bypass
                Ok(Box::new(SolarflareEfViTransport::new(
                    &config.interface,
                    &config.bind_addr,
                )))
            }
            HardwareTier::Tier2CloudVirtualized => {
                // Tier 2: Prefer Linux io_uring asynchronous batched ring
                match IoUringTransport::bind(&config.bind_addr, config.ring_capacity) {
                    Ok(transport) => Ok(Box::new(transport)),
                    Err(_) => {
                        // Fallback to cross-platform standard socket
                        let socket_transport = StandardSocketTransport::bind(
                            &config.bind_addr,
                            config.enable_busy_poll,
                        )?;
                        Ok(Box::new(socket_transport))
                    }
                }
            }
            HardwareTier::Tier1MidRange => {
                // Tier 1: Cross-platform non-blocking UDP socket
                let socket_transport =
                    StandardSocketTransport::bind(&config.bind_addr, config.enable_busy_poll)?;
                Ok(Box::new(socket_transport))
            }
        }
    }

    /// Explicitly instantiates a requested transport backend
    pub fn create_explicit(
        backend: TransportBackendKind,
        config: &TransportConfig,
    ) -> Result<Box<dyn NetworkTransport>, TransportError> {
        match backend {
            TransportBackendKind::SolarflareEfVi => Ok(Box::new(SolarflareEfViTransport::new(
                &config.interface,
                &config.bind_addr,
            ))),
            TransportBackendKind::IoUring => {
                let transport = IoUringTransport::bind(&config.bind_addr, config.ring_capacity)?;
                Ok(Box::new(transport))
            }
            TransportBackendKind::StandardSocket => {
                let transport =
                    StandardSocketTransport::bind(&config.bind_addr, config.enable_busy_poll)?;
                Ok(Box::new(transport))
            }
            TransportBackendKind::Mock => Ok(Box::new(MockNetworkTransport::new())),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::CefiParser;
    use core_engine::topology::MockHardwareProbe;
    use ingest_models::VenueId;

    #[test]
    fn test_solarflare_ef_vi_transport_ingress() {
        let mut transport = SolarflareEfViTransport::new("sfc0", "239.1.1.1:12345");
        assert_eq!(
            transport.backend_kind(),
            TransportBackendKind::SolarflareEfVi
        );
        assert!(transport.is_active());
        assert_eq!(transport.as_raw_fd(), None);

        let packet1 = br#"{"u":100,"s":"BTCUSDT","b":"65000.50","B":"1.5","a":"65001.00","A":"2.0","E":1672531199000}"#;
        let packet2 = br#"{"u":101,"s":"BTCUSDT","b":"65001.00","B":"1.8","a":"65001.50","A":"1.2","E":1672531200000}"#;

        transport.inject_packet(packet1, 100_000).unwrap();
        transport.inject_packet(packet2, 200_000).unwrap();

        let mut received = Vec::new();
        let count = transport
            .poll_batch(10, &mut |payload, ts| {
                received.push((payload.to_vec(), ts));
            })
            .unwrap();

        assert_eq!(count, 2);
        assert_eq!(received.len(), 2);
        assert_eq!(received[0].1, 100_000);
        assert_eq!(received[1].1, 200_000);

        let stats = transport.stats();
        assert_eq!(stats.rx_packets, 2);
        assert_eq!(stats.rx_bytes, (packet1.len() + packet2.len()) as u64);
        assert_eq!(stats.poll_calls, 1);
        assert_eq!(stats.empty_polls, 0);

        // Next poll should be empty
        let empty_count = transport.poll_batch(10, &mut |_, _| {}).unwrap();
        assert_eq!(empty_count, 0);
        assert_eq!(transport.stats().empty_polls, 1);
    }

    #[test]
    fn test_io_uring_transport_ingress() {
        let mut transport = IoUringTransport::bind("mock", 128).unwrap();
        assert_eq!(transport.backend_kind(), TransportBackendKind::IoUring);
        assert!(transport.is_active());

        let payload = br#"{"u":200,"s":"ETHUSDT","b":"3500.00","B":"10.0","a":"3500.50","A":"8.5","E":1672531201000}"#;
        transport.inject_packet(payload, 500_000).unwrap();

        let mut captured_bbo = None;
        let count = transport
            .poll_batch(10, &mut |bytes, ts| {
                if let Ok(bbo) = CefiParser::parse_binance_bbo(1, bytes, ts) {
                    captured_bbo = Some(bbo);
                }
            })
            .unwrap();

        assert_eq!(count, 1);
        let bbo = captured_bbo.expect("Should have parsed BBO from io_uring packet");
        assert_eq!(bbo.venue, VenueId::Binance as u16);
        assert_eq!(bbo.bid_price, 3500_00000000);
        assert_eq!(bbo.ask_price, 3500_50000000);
        assert_eq!(bbo.telemetry.t1_ingest_nic_ns, 500_000);

        assert_eq!(transport.stats().rx_packets, 1);
    }

    #[test]
    fn test_standard_socket_transport_loopback() {
        // Bind receiver to an ephemeral loopback port
        let mut receiver = StandardSocketTransport::bind("127.0.0.1:0", false).unwrap();
        assert_eq!(
            receiver.backend_kind(),
            TransportBackendKind::StandardSocket
        );
        let target_addr = receiver.local_addr();

        // Bind sender to loopback
        let sender = std::net::UdpSocket::bind("127.0.0.1:0").unwrap();
        let payload = br#"{"u":300,"s":"SOLUSDT","b":"150.25","B":"50.0","a":"150.30","A":"40.0","E":1672531202000}"#;
        sender.send_to(payload, target_addr).unwrap();

        // Small sleep or retry to allow kernel socket delivery
        std::thread::sleep(std::time::Duration::from_millis(10));

        let mut received = Vec::new();
        let count = receiver
            .poll_batch(10, &mut |bytes, ts| {
                received.push((bytes.to_vec(), ts));
            })
            .unwrap();

        assert!(count >= 1);
        assert_eq!(&received[0].0[..payload.len()], payload);
        assert!(received[0].1 > 0);
        assert_eq!(receiver.stats().rx_packets, 1);
    }

    #[test]
    fn test_mock_network_transport() {
        let mut mock = MockNetworkTransport::new();
        assert_eq!(mock.backend_kind(), TransportBackendKind::Mock);

        mock.push_packet(b"TEST_PACKET_A", 100);
        mock.push_packet(b"TEST_PACKET_B", 200);
        assert_eq!(mock.queue_len(), 2);

        let mut items = Vec::new();
        let count = mock
            .poll_batch(5, &mut |bytes, ts| {
                items.push((bytes.to_vec(), ts));
            })
            .unwrap();

        assert_eq!(count, 2);
        assert_eq!(items[0].0, b"TEST_PACKET_A");
        assert_eq!(items[0].1, 100);
        assert_eq!(items[1].0, b"TEST_PACKET_B");
        assert_eq!(items[1].1, 200);
        assert_eq!(mock.queue_len(), 0);
    }

    #[test]
    fn test_transport_factory_tier_negotiation() {
        let config = TransportConfig {
            bind_addr: "127.0.0.1:0".to_string(),
            interface: "eth0".to_string(),
            preferred_backend: None,
            batch_size: 32,
            enable_busy_poll: false,
            ring_capacity: 512,
        };

        // 1. Tier 3 probe -> Solarflare EF_VI
        let tier3_probe = MockHardwareProbe::new_tier3_bare_metal();
        let t3 = TransportFactory::create_optimal_transport(&config, &tier3_probe).unwrap();
        assert_eq!(t3.backend_kind(), TransportBackendKind::SolarflareEfVi);

        // 2. Tier 2 probe -> io_uring / socket fallback
        let tier2_probe = MockHardwareProbe::new_tier2_aws_nitro();
        let t2 = TransportFactory::create_optimal_transport(&config, &tier2_probe).unwrap();
        assert!(
            t2.backend_kind() == TransportBackendKind::IoUring
                || t2.backend_kind() == TransportBackendKind::StandardSocket
        );

        // 3. Tier 1 probe -> StandardSocket
        let tier1_probe = MockHardwareProbe::new_tier1_laptop();
        let t1 = TransportFactory::create_optimal_transport(&config, &tier1_probe).unwrap();
        assert_eq!(t1.backend_kind(), TransportBackendKind::StandardSocket);

        // 4. Explicit preferred backend override
        let mut explicit_cfg = config.clone();
        explicit_cfg.preferred_backend = Some(TransportBackendKind::Mock);
        let explicit =
            TransportFactory::create_optimal_transport(&explicit_cfg, &tier3_probe).unwrap();
        assert_eq!(explicit.backend_kind(), TransportBackendKind::Mock);
    }
}
