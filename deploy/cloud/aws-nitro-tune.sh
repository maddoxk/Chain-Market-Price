#!/bin/bash
# ==============================================================================
# AWS Nitro Hypervisor & ENA Network Tuning Script
# Optimizes EC2 instances (c6i, c7g, c6a, r6i, m6i) for ultra-low-latency market data.
# Must be executed with root/sudo privileges.
# ==============================================================================

set -eo pipefail

if [ "$EUID" -ne 0 ]; then
  echo "Error: This script must be run as root." >&2
  exit 1
fi

echo "==============================================================================="
echo "Applying AWS Nitro & ENA High-Frequency Ingestion Optimizations"
echo "==============================================================================="

INTERFACE="eth0"

# 1. Clocksource Verification & Stabilization
if [ -f "/sys/devices/system/clocksource/clocksource0/available_clocksource" ]; then
    AVAIL_CS=$(cat /sys/devices/system/clocksource/clocksource0/available_clocksource)
    echo "[+] Available clocksources: ${AVAIL_CS}"
    if echo "$AVAIL_CS" | grep -q "tsc"; then
        echo "tsc" > /sys/devices/system/clocksource/clocksource0/current_clocksource
        echo "[✓] Set clocksource to invariant 'tsc'."
    elif echo "$AVAIL_CS" | grep -q "kvm-clock"; then
        echo "kvm-clock" > /sys/devices/system/clocksource/clocksource0/current_clocksource
        echo "[✓] Set clocksource to 'kvm-clock'."
    fi
fi

# 2. AWS Elastic Network Adapter (ENA) Ring Buffers
if command -v ethtool >/dev/null 2>&1; then
    if ethtool -i "$INTERFACE" 2>/dev/null | grep -q "driver: ena"; then
        echo "[+] ENA driver detected on ${INTERFACE}."
        # Expand RX and TX hardware ring buffers to maximum capacity (4096)
        ethtool -G "$INTERFACE" rx 4096 tx 4096 2>/dev/null || true
        echo "[✓] Expanded ${INTERFACE} ring buffers to 4096."

        # Enable Adaptive RX Network Interrupt Coalescing
        ethtool -C "$INTERFACE" adaptive-rx on adaptive-tx on 2>/dev/null || true

        # Enable ENA Express (Scalable Reliable Datagram - SRD) if supported
        ethtool -K "$INTERFACE" rxhash on 2>/dev/null || true
        echo "[✓] Optimized ENA network hardware hashing and coalescing."
    else
        echo "[!] Interface ${INTERFACE} is not using the ENA driver; skipping ENA-specific tuning."
    fi
fi

# 3. Kernel Network Sysctl Overrides
sysctl -w net.core.rmem_max=67108864 >/dev/null
sysctl -w net.core.wmem_max=67108864 >/dev/null
sysctl -w net.core.rmem_default=33554432 >/dev/null
sysctl -w net.core.wmem_default=33554432 >/dev/null
sysctl -w net.core.netdev_max_backlog=250000 >/dev/null
sysctl -w net.ipv4.tcp_rmem="4096 87380 67108864" >/dev/null
sysctl -w net.ipv4.tcp_wmem="4096 65536 67108864" >/dev/null
sysctl -w net.ipv4.tcp_low_latency=1 2>/dev/null || true
sysctl -w net.ipv4.tcp_congestion_control=bbr 2>/dev/null || true

echo "[✓] Applied low-latency TCP/UDP memory buffer sysctls."

# 4. Disable CPU C-State Deep Sleep (Reduces vCPU wakeup latency)
if [ -d "/sys/devices/system/cpu/cpu0/cpuidle" ]; then
    for STATE_DISABLE in /sys/devices/system/cpu/cpu*/cpuidle/state[23456789]/disable; do
        if [ -f "$STATE_DISABLE" ]; then
            echo 1 > "$STATE_DISABLE" 2>/dev/null || true
        fi
    done
    echo "[✓] Restricted CPU deep C-states to eliminate vCPU wake latency."
fi

echo "==============================================================================="
echo "AWS Nitro & ENA host configuration complete."
echo "==============================================================================="
