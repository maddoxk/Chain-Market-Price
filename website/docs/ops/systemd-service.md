---
id: systemd-service
title: Systemd Real-Time Service & NUMA Core Pinning
sidebar_label: Systemd & NUMA
---

# Systemd Real-Time Service & NUMA Core Pinning

To guarantee execution on dedicated CPU cores without OS migration, `Chain-Market-Price` provides systemd unit files and NUMA node pinning scripts.

---

## 1. NUMA Node Pinning Architecture

```
                          NUMA NODE 0 (Socket 0)
+-------------------------------------------------------------------------+
| Core 0: Linux OS Housekeeping / SSH / Monitoring / Cron                 |
+-------------------------------------------------------------------------+
| Cores 1-8: Dedicated Isolated Trading Cores (Real-Time FIFO 99)         |
|  - Core 1: Solarflare ef_vi / DPDK Kernel-Bypass Ingress                |
|  - Core 2: CeFi Normalization (Binance, OKX, Bybit)                     |
|  - Core 3: On-Chain Ingestion (EVM Reth IPC & Solana Geyser)            |
|  - Core 4: AMM Virtualizer (Uniswap v3/v4 & Curve Math)                 |
|  - Core 5: Shared Memory Ring Buffer Writer (/dev/shm)                  |
|  - Core 6: WebSocket SBE & JSON Fan-out Event Loop                      |
|  - Cores 7-8: Telemetry Aggregation & Health Watchdog                   |
+-------------------------------------------------------------------------+
```

### Launch Script (`scripts/launch_pinned.sh`)
```bash
numactl --cpunodebind=0 --membind=0 \
        taskset -c 1,2,3,4,5,6,7,8 \
        ./target/release/chain-market-price
```

---

## 2. Systemd Real-Time Unit (`deploy/systemd/chain-market-price.service`)

```ini
[Unit]
Description=Chain-Market-Price Ultra-Low Latency HFT Feed Engine
After=network-online.target
DefaultDependencies=no

[Service]
Type=simple
User=hft
Group=hft
WorkingDirectory=/opt/chain-market-price
ExecStart=/opt/chain-market-price/scripts/launch_pinned.sh

# Real-Time Scheduling & Resource Invariants
LimitMEMLOCK=infinity
Nice=-20
CPUSchedulingPolicy=fifo
CPUSchedulingPriority=99
CPUAffinity=1-8

# Sandboxing
ProtectSystem=full
ProtectHome=read-only
PrivateTmp=true
ReadWritePaths=/dev/shm /var/log/chain-market-price

# Watchdog & Restart
Restart=always
RestartSec=2s
WatchdogSec=5s

[Install]
WantedBy=multi-user.target
```

### Service Installation
```bash
sudo cp deploy/systemd/chain-market-price.service /etc/systemd/system/
sudo systemctl daemon-reload
sudo systemctl enable --now chain-market-price
```
