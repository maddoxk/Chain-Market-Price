---
id: kernel-bypass
title: Solarflare ef_vi & DPDK Kernel-Bypass Ingestion
sidebar_label: Kernel Bypass
---

# Solarflare ef_vi & DPDK Kernel-Bypass Ingestion

Standard Linux OS kernel network processing incurs socket buffer copying, interrupt processing latency (SoftIRQs), and scheduler context switches, adding 5 to 15 µs of jitter. `Chain-Market-Price` integrates kernel-bypass networking architectures for colocation deployments.

---

## 1. Kernel-Bypass Architecture

```
                       TRADITIONAL LINUX KERNEL NETWORKING
[NIC Hardware] ==> [Hard IRQ] ==> [ksoftirqd] ==> [Socket Buffer] ==> [Context Switch] ==> [App]
                  (Latency: ~5 to 15 us | Significant Jitter)

                         SOLARFLARE EF_VI DIRECT MMIO
[NIC Hardware] ================== Direct DMA Mapping ==================> [Ring Buffer] ==> [App]
                  (Latency: < 0.45 us | Deterministic Hardware PTP)
```

---

## 2. Solarflare `ef_vi` Ingestion Bridge

- **Zero-Copy DMA:** Network packets are DMA-transferred directly into pre-allocated memory pools (`posted_descriptors`) mapped to user-space.
- **Hardware PTP Timestamps ($T_1$):** Every packet header contains nanosecond hardware timestamps captured at the physical PHY layer by the Solarflare NIC oscillator (IEEE 1588).
- **Zero-Syscall Polling:** The ingress thread executes a busy-wait loop (`poll_rx_burst`), reading ring descriptors without making OS system calls.

---

## 3. Modular Network Transport Layer (`NetworkTransport`)

In addition to bare-metal kernel bypass, the market data ingestion engine is abstracted behind the zero-cost polymorphic `NetworkTransport` trait in `cefi-ingest`:

```rust
pub trait NetworkTransport: Send {
    fn poll_batch(
        &mut self,
        max_batch: usize,
        handler: &mut dyn FnMut(&[u8], TimestampNs),
    ) -> Result<usize, TransportError>;

    fn stats(&self) -> TransportStats;
    fn backend_kind(&self) -> TransportBackendKind;
    fn is_active(&self) -> bool;
    fn as_raw_fd(&self) -> Option<i32>;
}
```

### Transport Backends by Hardware Tier

| Transport Backend | Hardware Tier | Mechanism | Steady-State Syscalls | Throughput |
| :--- | :--- | :--- | :--- | :--- |
| **`SolarflareEfViTransport`** | **Tier 3 (Enterprise Bare Metal)** | Direct userspace MMIO & NIC DMA ring | **0** | > 12,000,000 pkts/s |
| **`IoUringTransport`** | **Tier 2 (Cloud Virtualized)** | Batched SQ/CQ rings with pre-allocated buffer pools | **0 (with SQPOLL)** | ~3,500,000 pkts/s |
| **`StandardSocketTransport`** | **Tier 1 (Mid-Range & Fallback)** | Non-blocking BSD UDP sockets (`epoll` / `kqueue`) | 1 per batch | ~500,000 pkts/s |
| **`MockNetworkTransport`** | **Testing / Simulation** | Deterministic in-memory ring queue | 0 | In-Memory Bandwidth |

---

## 4. Hardware Auto-Negotiation (`TransportFactory`)

At system startup, `TransportFactory::create_optimal_transport()` inspects the runtime hardware profile negotiated by `HardwareProbe` and dynamically instantiates the highest performing transport viable on the host:

```rust
let config = TransportConfig {
    bind_addr: "0.0.0.0:12345".to_string(),
    interface: "eth0".to_string(),
    preferred_backend: None, // Auto-negotiate based on detected hardware
    batch_size: 64,
    enable_busy_poll: true,
    ring_capacity: 2048,
};

let probe = SystemHardwareProbe::new();
let mut transport = TransportFactory::create_optimal_transport(&config, &probe)?;
println!("Active Transport Backend: {}", transport.backend_kind());
```

