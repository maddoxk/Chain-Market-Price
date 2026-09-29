//! # Cloud-Native Health & Telemetry Probes
//!
//! Provides zero-dependency HTTP endpoints for cloud container orchestrators:
//! - `/healthz`: Liveness probe (checks process responsiveness).
//! - `/readyz`: Readiness probe (verifies that market data pipelines and rings are initialized).
//! - `/metrics`: Prometheus-compatible plaintext telemetry metrics.

use std::io::{Read, Write};
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::thread;
use std::time::{Duration, Instant};

/// Operational status and telemetry counters for cloud health probes
pub struct HealthStatus {
    pub is_ready: AtomicBool,
    pub ticks_processed: AtomicU64,
    pub start_time: Instant,
    pub tier_code: AtomicU64,
}

impl HealthStatus {
    pub fn new() -> Self {
        Self {
            is_ready: AtomicBool::new(false),
            ticks_processed: AtomicU64::new(0),
            start_time: Instant::now(),
            tier_code: AtomicU64::new(1),
        }
    }

    pub fn mark_ready(&self) {
        self.is_ready.store(true, Ordering::Release);
    }

    pub fn set_tier(&self, tier: u64) {
        self.tier_code.store(tier, Ordering::Release);
    }

    pub fn inc_ticks(&self, count: u64) {
        self.ticks_processed.fetch_add(count, Ordering::Relaxed);
    }
}

impl Default for HealthStatus {
    fn default() -> Self {
        Self::new()
    }
}

/// Lightweight HTTP probe server running without external web frameworks
pub struct HealthServer {
    listener: TcpListener,
    status: Arc<HealthStatus>,
    stop_signal: Arc<AtomicBool>,
    handle: Option<thread::JoinHandle<()>>,
}

impl HealthServer {
    /// Binds the health probe server to a local TCP address (e.g. "0.0.0.0:9001")
    pub fn bind(bind_addr: &str, status: Arc<HealthStatus>) -> Result<Self, std::io::Error> {
        let listener = TcpListener::bind(bind_addr)?;
        listener.set_nonblocking(true)?;

        let stop_signal = Arc::new(AtomicBool::new(false));
        let stop_clone = Arc::clone(&stop_signal);
        let status_clone = Arc::clone(&status);
        let listener_clone = listener.try_clone()?;

        let handle = thread::spawn(move || {
            let mut buf = [0u8; 1024];
            while !stop_clone.load(Ordering::Relaxed) {
                match listener_clone.accept() {
                    Ok((mut stream, _addr)) => {
                        let _ = stream.set_read_timeout(Some(Duration::from_millis(500)));
                        let _ = stream.set_write_timeout(Some(Duration::from_millis(500)));

                        if let Ok(n) = stream.read(&mut buf) {
                            if n > 0 {
                                Self::handle_request(&buf[..n], &mut stream, &status_clone);
                            }
                        }
                    }
                    Err(ref e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                        thread::sleep(Duration::from_millis(10));
                    }
                    Err(_) => {
                        thread::sleep(Duration::from_millis(20));
                    }
                }
            }
        });

        Ok(Self {
            listener,
            status,
            stop_signal,
            handle: Some(handle),
        })
    }

    /// Dispatches incoming HTTP GET requests
    fn handle_request(req_bytes: &[u8], stream: &mut TcpStream, status: &HealthStatus) {
        let req_str = String::from_utf8_lossy(req_bytes);
        let first_line = req_str.lines().next().unwrap_or("");

        if first_line.starts_with("GET /healthz") {
            let body = "OK\n";
            let resp = format!(
                "HTTP/1.1 200 OK\r\nContent-Type: text/plain\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
                body.len(),
                body
            );
            let _ = stream.write_all(resp.as_bytes());
        } else if first_line.starts_with("GET /readyz") {
            let ready = status.is_ready.load(Ordering::Acquire);
            if ready {
                let body = "READY\n";
                let resp = format!(
                    "HTTP/1.1 200 OK\r\nContent-Type: text/plain\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
                    body.len(),
                    body
                );
                let _ = stream.write_all(resp.as_bytes());
            } else {
                let body = "INITIALIZING\n";
                let resp = format!(
                    "HTTP/1.1 503 Service Unavailable\r\nContent-Type: text/plain\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
                    body.len(),
                    body
                );
                let _ = stream.write_all(resp.as_bytes());
            }
        } else if first_line.starts_with("GET /metrics") {
            let uptime = status.start_time.elapsed().as_secs();
            let ticks = status.ticks_processed.load(Ordering::Relaxed);
            let tier = status.tier_code.load(Ordering::Relaxed);

            let body = format!(
                "# HELP cmp_engine_uptime_seconds Engine uptime in seconds\n\
                 # TYPE cmp_engine_uptime_seconds gauge\n\
                 cmp_engine_uptime_seconds {}\n\
                 # HELP cmp_engine_ticks_processed_total Total market data ticks processed\n\
                 # TYPE cmp_engine_ticks_processed_total counter\n\
                 cmp_engine_ticks_processed_total {}\n\
                 # HELP cmp_engine_active_tier Negotiated operational hardware tier (1, 2, or 3)\n\
                 # TYPE cmp_engine_active_tier gauge\n\
                 cmp_engine_active_tier {}\n",
                uptime, ticks, tier
            );

            let resp = format!(
                "HTTP/1.1 200 OK\r\nContent-Type: text/plain; version=0.0.4\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
                body.len(),
                body
            );
            let _ = stream.write_all(resp.as_bytes());
        } else {
            let body = "Not Found\n";
            let resp = format!(
                "HTTP/1.1 404 Not Found\r\nContent-Type: text/plain\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
                body.len(),
                body
            );
            let _ = stream.write_all(resp.as_bytes());
        }
        let _ = stream.flush();
    }

    pub fn local_addr(&self) -> Result<SocketAddr, std::io::Error> {
        self.listener.local_addr()
    }

    pub fn status(&self) -> &Arc<HealthStatus> {
        &self.status
    }
}

impl Drop for HealthServer {
    fn drop(&mut self) {
        self.stop_signal.store(true, Ordering::Relaxed);
        if let Some(handle) = self.handle.take() {
            let _ = handle.join();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_health_server_endpoints() {
        let status = Arc::new(HealthStatus::new());
        status.set_tier(2);
        status.inc_ticks(42_000);

        let server = HealthServer::bind("127.0.0.1:0", Arc::clone(&status)).unwrap();
        let addr = server.local_addr().unwrap();

        // 1. Test /healthz
        {
            let mut client = TcpStream::connect(addr).unwrap();
            client
                .write_all(b"GET /healthz HTTP/1.1\r\nHost: localhost\r\n\r\n")
                .unwrap();
            let mut resp = String::new();
            client.read_to_string(&mut resp).unwrap();
            assert!(resp.starts_with("HTTP/1.1 200 OK"));
            assert!(resp.contains("OK\n"));
        }

        // 2. Test /readyz before ready
        {
            let mut client = TcpStream::connect(addr).unwrap();
            client
                .write_all(b"GET /readyz HTTP/1.1\r\nHost: localhost\r\n\r\n")
                .unwrap();
            let mut resp = String::new();
            client.read_to_string(&mut resp).unwrap();
            assert!(resp.starts_with("HTTP/1.1 503 Service Unavailable"));
        }

        // Mark ready
        status.mark_ready();

        // 3. Test /readyz after ready
        {
            let mut client = TcpStream::connect(addr).unwrap();
            client
                .write_all(b"GET /readyz HTTP/1.1\r\nHost: localhost\r\n\r\n")
                .unwrap();
            let mut resp = String::new();
            client.read_to_string(&mut resp).unwrap();
            assert!(resp.starts_with("HTTP/1.1 200 OK"));
            assert!(resp.contains("READY\n"));
        }

        // 4. Test /metrics
        {
            let mut client = TcpStream::connect(addr).unwrap();
            client
                .write_all(b"GET /metrics HTTP/1.1\r\nHost: localhost\r\n\r\n")
                .unwrap();
            let mut resp = String::new();
            client.read_to_string(&mut resp).unwrap();
            assert!(resp.starts_with("HTTP/1.1 200 OK"));
            assert!(resp.contains("cmp_engine_ticks_processed_total 42000"));
            assert!(resp.contains("cmp_engine_active_tier 2"));
        }
    }
}
