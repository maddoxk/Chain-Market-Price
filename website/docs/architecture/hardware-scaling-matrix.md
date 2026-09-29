---
id: hardware-scaling-matrix
title: Multi-Tier Hardware Scaling Matrix & Adaptive Synchronization
sidebar_label: Hardware Scaling Matrix
---

# Multi-Tier Hardware Scaling Matrix & Adaptive Synchronization

While `Chain-Market-Price` delivers sub-microsecond tick processing in high-grade bare-metal colocation (Equinix NY4/LD4/TY3), production trading architectures require frictionless execution across heterogeneous infrastructure: developer laptops (Apple Silicon / Intel / AMD), cloud virtualized instances (AWS EC2, GCP Compute Engine, Docker, Kubernetes), and ultra-enterprise colocation hardware.

---

## 1. The Three Operational Tiers

```text
+-------------------------------------------------------------------------------------------------------------+
|                                    HARDWARE PROFILE SPECIFICATION MATRIX                                    |
+--------------------------+-------------------------------+------------------------------+-------------------+
| Feature Dimension        | Tier 1: Mid-Range Workstation | Tier 2: Cloud Virtualized    | Tier 3: Bare Metal|
+--------------------------+-------------------------------+------------------------------+-------------------+
| Typical Hardware Target  | 4-8 Cores (Apple M-series,    | AWS EC2 (c6i, c7g, m6i),     | Dual AMD EPYC 9654|
|                          | Intel Core i7, AMD Ryzen 7),  | GCP C3, Azure D-series,      | / Xeon Platinum,  |
|                          | 16-32 GB RAM, macOS / Linux   | Kubernetes Pods (4-16 vCPUs) | 128GB+ DDR5 NUMA  |
+--------------------------+-------------------------------+------------------------------+-------------------+
| Concurrency & Threading  | Dynamic Cooperative Pool:     | Multiplexed Worker Pool:     | Dedicated 1:1 Core|
|                          | 2-4 threads; pipeline stages  | 4-8 pinned vCPUs; Stage      | Isolation; strict |
|                          | share worker execution loop.  | multiplexing with affinity.  | nohz_full pinning.|
+--------------------------+-------------------------------+------------------------------+-------------------+
| Polling & Backoff Policy | Adaptive Hybrid:              | Steal-Time Aware Backoff:    | Pure Zero-Sleep:  |
|                          | Spin 32 iters -> Rapid sleep; | Spin 256 iters -> Yield ->   | Unconditional     |
|                          | < 2% idle CPU utilization.    | 50us timed pause (no steal). | _mm_pause() spin. |
+--------------------------+-------------------------------+------------------------------+-------------------+
| Network Transport        | Standard Non-blocking Sockets | Tokio epoll / Linux io_uring | Solarflare ef_vi  |
|                          | (TCP/TLS / WebSocket / kqueue)| with vectorized TCP buffers  | / DPDK bypass     |
+--------------------------+-------------------------------+------------------------------+-------------------+
| Shared Memory (SHM)      | 8,192 slots (512 KB);         | 65,536 slots (4 MB);         | 1,048,576 slots   |
|                          | Fallback to /tmp or UDS socket| Auto-detects /dev/shm limits | (64 MB) HugeTLB   |
+--------------------------+-------------------------------+------------------------------+-------------------+
| Target Median Latency    | ~15 to 45 microseconds        | ~3.5 to 8.5 microseconds     | < 2.16 microsecs  |
+--------------------------+-------------------------------+------------------------------+-------------------+
```

---

## 2. Runtime Topology Auto-Detection Engine

At startup, before allocating worker rings or mapping memory, the engine executes `SystemHardwareProbe::negotiate_profile(override)` in `crates/core-engine/src/topology/`:

1. **Hypervisor & Container Detection:**
   - Probes `/.dockerenv`, `/run/.containerenv`.
   - Reads Linux DMI product strings (`/sys/class/dmi/id/product_name` and `sys_vendor`) identifying AWS Nitro, GCP Compute Engine, KVM, Xen, Hyper-V, or bare-metal.
   - On macOS, checks `kern.hv_vmm_present` via `sysctl`.
2. **CPU & NUMA Topology:**
   - Detects logical and physical cores.
   - Detects hybrid CPU topologies (e.g., Apple Silicon Performance vs Efficiency cores).
   - Counts NUMA nodes via `/sys/devices/system/node/`.
3. **Privilege & Limit Probes:**
   - Non-fatal probe of `MAP_HUGETLB` via `/sys/kernel/mm/hugepages/`.
   - Non-fatal test of real-time `SCHED_FIFO` capability.
   - Reads cgroup v2 CPU quota limits from `/sys/fs/cgroup/cpu.max`.
4. **Negotiation Rules & Operator Override:**
   - Operators can explicitly override the active tier using the `CMP_HARDWARE_PROFILE` environment variable (`tier1`, `tier2`, `tier3`, or `auto`).

---

## 3. Adaptive Backoff Synchronization (`WaitStrategy`)

Unbounded busy-spinning (`loop { core::hint::spin_loop(); }`) pins developer laptops at 100% CPU, causing thermal throttling and battery exhaustion. In the cloud, it triggers hypervisor vCPU descheduling and catastrophic `%steal` tail latency.

`crates/core-engine/src/sync/wait_strategy.rs` provides hardware-adaptive polling:

### 1. `BusySpinStrategy` (Tier 3: Enterprise Bare Metal)
Pure unthrottled `core::hint::spin_loop()` (`_mm_pause` on x86_64, `YIELD` on aarch64). Zero syscall overhead, sub-50ns wake-up.

### 2. `HybridAdaptiveStrategy` (Tier 2: Cloud Virtualized)
A 3-stage backoff ladder:
- **Phase 1 (Spins 1–256):** `core::hint::spin_loop()`
- **Phase 2 (Spins 257–1280):** `std::thread::yield_now()`
- **Phase 3 (Escalation):** Exponential timed sleep (`1µs` to `50µs`), preventing vCPU credit exhaustion.

### 3. `PowerEfficientStrategy` (Tier 1: Developer Workstations)
Rapid escalation to sleep after 32 spins, dropping idle CPU utilization to **under 2%**.

### 4. Dynamic Burst Promotion (`AdaptiveBurstWaitStrategy`)
When a market volatility spike occurs (incoming tick velocity exceeds `5,000 ticks/sec`), the wait strategy automatically promotes to pure `BusySpinStrategy` for a minimum decay window (50ms). This ensures zero latency degradation during critical arbitrage opportunities, smoothly returning to power-efficient backoff once volume normalizes.

---

## 4. Zero-Allocation SPSC Integration

Ring buffer consumers pop data using hardware-adaptive strategies without dynamic dispatch overhead:

```rust
use core_engine::sync::{DynamicWaitStrategy, WaitStrategy};
use core_engine::topology::HardwareTier;
use core_engine::SpscRingBuffer;

let ring: SpscRingBuffer<u64, 1024> = SpscRingBuffer::new();
let mut strategy = DynamicWaitStrategy::for_tier(HardwareTier::Tier2CloudVirtualized);

// Block adaptively until data arrives
let item = ring.pop_blocking(&mut strategy);
```
