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

## 3. Host Compatibility & Fallback Mode

On development machines (e.g. macOS / Windows) or cloud instances lacking Solarflare Onload / DPDK PCIe devices, the engine automatically activates its high-performance POSIX fallback mode:
- Resolves timestamps via hardware Time Stamp Counter (TSC).
- Emulates packet arrival through memory-mapped SPSC queues.
- Guarantees binary and structural compatibility across all platforms.
