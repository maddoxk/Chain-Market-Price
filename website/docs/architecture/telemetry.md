---
id: telemetry
title: 4-Point Nanosecond Telemetry Latency Accounting
sidebar_label: Nanosecond Telemetry
---

# 4-Point Nanosecond Telemetry Latency Accounting

In quantitative high-frequency trading and cross-venue arbitrage, measuring latency is not an afterthought—it is a core alpha signal. Every market event dispatched by `Chain-Market-Price` carries an immutable, 32-byte telemetry record containing four hardware-synchronized nanosecond timestamps ($T_0 \dots T_3$).

---

## 1. Timestamp Definitions

```
       Exchange Engine                       Ingress NIC                        Engine Core                       Wire Egress
        [Matching]                         [Hardware DMA]                     [Discretization]                   [Socket / SHM]
            |                                     |                                  |                                  |
            |------------ WAN Packet ------------>|                                  |                                  |
           (T0)                                  (T1)                                |                                  |
            |                                     |------- SPSC Queue Ingest ------->|                                  |
            |                                     |                                 (T2)                                |
            |                                     |                                  |------- Lock-Free Fanout -------->|
            |                                     |                                  |                                 (T3)
```

| Timestamp | Source | Precision | Description |
| :--- | :--- | :--- | :--- |
| **T0** | Exchange / Block Timestamp | Millisecond or Microsecond | Timestamp assigned by exchange matching engine (CeFi) or block header (DeFi) |
| **T1** | Solarflare NIC / Ingress Bridge | Nanosecond (IEEE 1588 PTP) | Hardware timestamp captured upon packet DMA arrival at physical network interface |
| **T2** | Engine Pipeline Core | Nanosecond (Invariant TSC) | Timestamp when message normalization and order book reconstruction completes |
| **T3** | Distribution Layer | Nanosecond (Invariant TSC) | Timestamp immediately prior to SBE wire serialization or SHM ring commit |

---

## 2. Derived Metrics & Latency Equations

Clients use these timestamps to decompose their execution latency:

### Internal Engine Pipeline Latency (Delta_engine)
```text
Delta_engine = T2 - T1
```
Measures the duration required for packet ingestion, JSON/binary parsing, sequence tracking, and virtual order book updating. Target SLA: `<= 1.80 µs`.

### Wire-to-Egress Latency (Delta_egress)
```text
Delta_egress = T3 - T1
```
Total duration from hardware NIC packet reception to broadcast wire availability. Target SLA: `<= 2.90 µs`.

### Network Transit Latency (Delta_network)
```text
Delta_network = T1 - T0
```
Transit time from the external exchange matching engine across the WAN to the colocation facility.

---

## 3. Data Structure Definition

```rust
#[repr(C, align(32))]
#[derive(Clone, Copy, Default, Debug, PartialEq, Eq)]
pub struct TelemetryTimestamps {
    pub t0_exchange_ns: u64,
    pub t1_ingest_nic_ns: u64,
    pub t2_engine_proc_ns: u64,
    pub t3_egress_ns: u64,
}

impl TelemetryTimestamps {
    #[inline(always)]
    pub fn internal_latency_ns(&self) -> u64 {
        self.t2_engine_proc_ns.saturating_sub(self.t1_ingest_nic_ns)
    }

    #[inline(always)]
    pub fn wire_to_egress_latency_ns(&self) -> u64 {
        self.t3_egress_ns.saturating_sub(self.t1_ingest_nic_ns)
    }
}
```
