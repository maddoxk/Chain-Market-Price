---
id: kernel-tuning
title: Linux Kernel & Sysctl Low-Latency Tuning Profile
sidebar_label: Kernel & Sysctl Tuning
---

# Linux Kernel & Sysctl Low-Latency Tuning Profile

Standard Linux distributions prioritize fair sharing and throughput over latency determinism. `Chain-Market-Price` provides pre-tuned kernel boot parameters and sysctl profiles.

---

## 1. Kernel Boot Command Line (`/etc/default/grub`)

```text
GRUB_CMDLINE_LINUX_DEFAULT="quiet splash isolcpus=1-8 nohz_full=1-8 rcu_nocbs=1-8 intel_idle.max_cstate=0 processor.max_cstate=0 idle=poll transparent_hugepage=never default_hugepagesz=2M hugepagesz=2M hugepages=2048 tsc=reliable clocksource=tsc audit=0 nosoftlockup mce=ignore_ce"
```

### Parameter Breakdown
- **`isolcpus=1-8`**: Removes CPU cores 1–8 from the OS process scheduler.
- **`nohz_full=1-8`**: Disables the 1000Hz scheduling timer tick on isolated cores.
- **`rcu_nocbs=1-8`**: Offloads RCU callbacks to core 0.
- **`idle=poll`**: Forces idle CPU cores into a busy loop instead of halting.
- **`hugepages=2048`**: Pre-allocates 4GB of contiguous 2MB physical RAM pages.
- **`tsc=reliable clocksource=tsc`**: Enforces invariant hardware TSC timestamps.

---

## 2. Production Sysctl Profile (`deploy/sysctl/99-hft-low-latency.conf`)

Install to `/etc/sysctl.d/99-hft-low-latency.conf`:

```ini
# Network socket buffer expansion (64MB)
net.core.rmem_max = 67108864
net.core.wmem_max = 67108864
net.ipv4.tcp_rmem = 4096 87380 67108864
net.ipv4.tcp_wmem = 4096 65536 67108864

# Kernel socket busy-polling (bypasses OS interrupt latency)
net.core.busy_poll = 50
net.core.busy_read = 50

# TCP Latency Optimizations
net.ipv4.tcp_low_latency = 1
net.ipv4.tcp_timestamps = 0
net.ipv4.tcp_sack = 1

# Completely eliminate swapping hot trading pages
vm.swappiness = 0
vm.dirty_ratio = 10
vm.dirty_background_ratio = 5

# Static HugePages allocation
vm.nr_hugepages = 2048
vm.max_map_count = 1048576

# IPC Shared Memory Limits (64GB max segment)
kernel.shmmax = 68719476736
kernel.shmall = 4294967296
```

Reload sysctl parameters:
```bash
sudo sysctl --system
```
