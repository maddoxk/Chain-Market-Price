---
id: bare-metal-hardware
title: Bare-Metal Hardware & Colocation Architecture
sidebar_label: Hardware Architecture
---

# Bare-Metal Hardware & Colocation Architecture

To guarantee sub-microsecond tick processing under market micro-bursts, the engine is optimized for high-frequency financial colocation deployments.

---

## 1. Enterprise Hardware Bill of Materials (BOM)

| Component | Minimum Specification | Recommended Enterprise Reference |
| :--- | :--- | :--- |
| **Server Chassis** | 1U Rackmount Server | **Supermicro Ultra SuperServer** / Dell PowerEdge R660 |
| **Processor (CPU)** | 16+ Cores @ &ge; 3.8 GHz Base Clock | **AMD EPYC 9654** (Zen 4, 96C/192T) or **Intel Xeon Platinum 8480+** |
| **Memory (RAM)** | 64 GB DDR5-4800 ECC Registered | **128 GB Quad-Channel** per NUMA node |
| **Network Interface (NIC)** | Dual-Port 10/25GbE PCIe Gen4 | **Solarflare XtremeScale X2522 / SFN8522** or **NVIDIA Mellanox ConnectX-6 Dx** |
| **Solid-State Storage** | Enterprise NVMe SSD | **KIOXIA CM6** / Samsung PM9A3 (Direct PCIe Gen4 x4) |
| **Colocation Facility** | Tier-3+ Financial DC | **Equinix NY4** (Secaucus), **LD4** (Slough), or **TY3** (Tokyo) |

---

## 2. BIOS & Firmware Tuning Checklist

Prior to booting the operating system, enforce these settings in server UEFI/BIOS:

1. **SMT / Hyper-Threading:** `Disabled` (Prevents thread contention for shared L1/L2 execution units).
2. **CPU Power Management:** `Maximum Performance` (Never enter idle low-power states).
3. **Intel SpeedStep / AMD Cool'n'Quiet:** `Disabled`.
4. **Processor C-States & C1E:** `Disabled` (Eliminates &gt; 10 µs sleep wake-up penalties).
5. **Memory Frequency:** Locked to maximum rated frequency.
6. **PCIe ASPM (Active State Power Management):** `Disabled`.
