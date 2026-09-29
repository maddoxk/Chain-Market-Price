//! # Standard Socket Transport (Tier 1 Mid-Range & Tier 2 Fallback)
//!
//! Cross-platform non-blocking socket transport using BSD socket API (`epoll`/`kqueue`),
//! optimized with preallocated frame buffers and zero runtime heap allocations.

use std::net::{SocketAddr, UdpSocket};
use std::time::SystemTime;

use super::{NetworkTransport, TimestampNs, TransportBackendKind, TransportError, TransportStats};

/// Maximum datagram buffer size (64 KiB)
pub const SOCKET_RX_BUFFER_SIZE: usize = 65536;

/// Tier 1 & Fallback: Standard Non-Blocking UDP Socket Transport
pub struct StandardSocketTransport {
    socket: UdpSocket,
    rx_buffer: [u8; SOCKET_RX_BUFFER_SIZE],
    enable_busy_poll: bool,
    stats: TransportStats,
    local_addr: SocketAddr,
    is_active: bool,
}

impl StandardSocketTransport {
    /// Binds a non-blocking UDP socket to the requested address
    pub fn bind(bind_addr: &str, enable_busy_poll: bool) -> Result<Self, TransportError> {
        let socket = UdpSocket::bind(bind_addr).map_err(|e| TransportError::Io(e.to_string()))?;
        socket
            .set_nonblocking(true)
            .map_err(|e| TransportError::Io(e.to_string()))?;

        let local_addr = socket
            .local_addr()
            .map_err(|e| TransportError::Io(e.to_string()))?;

        Ok(Self {
            socket,
            rx_buffer: [0u8; SOCKET_RX_BUFFER_SIZE],
            enable_busy_poll,
            stats: TransportStats::default(),
            local_addr,
            is_active: true,
        })
    }

    /// Sends a datagram to a destination (useful in testing and loopback feeds)
    pub fn send_to(&self, payload: &[u8], target: &SocketAddr) -> Result<usize, TransportError> {
        self.socket
            .send_to(payload, target)
            .map_err(|e| TransportError::Io(e.to_string()))
    }

    pub fn local_addr(&self) -> SocketAddr {
        self.local_addr
    }

    pub fn enable_busy_poll(&self) -> bool {
        self.enable_busy_poll
    }
}

impl NetworkTransport for StandardSocketTransport {
    fn poll_batch(
        &mut self,
        max_batch: usize,
        handler: &mut dyn FnMut(&[u8], TimestampNs),
    ) -> Result<usize, TransportError> {
        if !self.is_active {
            return Err(TransportError::NotConnected);
        }

        self.stats.poll_calls += 1;
        let mut count = 0;
        let mut batch_bytes = 0;

        for _ in 0..max_batch {
            match self.socket.recv_from(&mut self.rx_buffer) {
                Ok((len, _peer)) => {
                    let ts = SystemTime::now()
                        .duration_since(SystemTime::UNIX_EPOCH)
                        .unwrap_or_default()
                        .as_nanos() as u64;

                    handler(&self.rx_buffer[..len], ts);
                    count += 1;
                    batch_bytes += len as u64;
                }
                Err(ref e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                    // Non-blocking socket drained
                    break;
                }
                Err(ref e) if e.kind() == std::io::ErrorKind::Interrupted => {
                    continue;
                }
                Err(e) => {
                    return Err(TransportError::Io(e.to_string()));
                }
            }
        }

        if count == 0 {
            self.stats.empty_polls += 1;
        } else {
            self.stats.rx_packets += count as u64;
            self.stats.rx_bytes += batch_bytes;
        }

        Ok(count)
    }

    fn stats(&self) -> TransportStats {
        self.stats.clone()
    }

    fn backend_kind(&self) -> TransportBackendKind {
        TransportBackendKind::StandardSocket
    }

    fn is_active(&self) -> bool {
        self.is_active
    }

    fn as_raw_fd(&self) -> Option<i32> {
        #[cfg(unix)]
        {
            use std::os::unix::io::AsRawFd;
            Some(self.socket.as_raw_fd())
        }
        #[cfg(not(unix))]
        {
            None
        }
    }
}
