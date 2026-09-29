#!/bin/bash
# ==============================================================================
# Google Cloud Platform (GCP) Compute Engine & gVNIC Tuning Script
# Optimizes GCP instances (C3, C3D, C2, N2) for ultra-low-latency market data.
# Must be executed with root/sudo privileges.
# ==============================================================================

set -eo pipefail

if [ "$EUID" -ne 0 ]; then
  echo "Error: This script must be run as root." >&2
  exit 1
fi

echo "==============================================================================="
echo "Applying GCP Compute Engine & gVNIC Low-Latency Optimizations"
echo "==============================================================================="

INTERFACE="eth0"

# 1. Google Virtual NIC (gVNIC) Driver Tuning
if command -v ethtool >/dev/null 2>&1; then
    if ethtool -i "$INTERFACE" 2>/dev/null | grep -q "driver: gve"; then
        echo "[+] Google Virtual Ethernet (gve) driver detected on ${INTERFACE}."
        ethtool -G "$INTERFACE" rx 2048 tx 2048 2>/dev/null || true
        ethtool -K "$INTERFACE" tso on gso on gro on 2>/dev/null || true
        echo "[✓] Configured gVNIC ring descriptors and offloading."
    fi
fi

# 2. Receive Packet Steering (RPS) & Receive Flow Steering (RFS)
CORES=$(nproc)
RPS_CPUS=$(printf '%x' $(( (1 << CORES) - 1 )))

for RXQ in /sys/class/net/"$INTERFACE"/queues/rx-*; do
    if [ -f "$RXQ/rps_cpus" ]; then
        echo "$RPS_CPUS" > "$RXQ/rps_cpus" 2>/dev/null || true
    fi
done

if [ -f "/proc/sys/net/core/rps_sock_flow_entries" ]; then
    echo 32768 > /proc/sys/net/core/rps_sock_flow_entries
fi

for RXQ in /sys/class/net/"$INTERFACE"/queues/rx-*; do
    if [ -f "$RXQ/rps_flow_cnt" ]; then
        echo 4096 > "$RXQ/rps_flow_cnt" 2>/dev/null || true
    fi
done
echo "[✓] Configured multi-core RPS/RFS on ${INTERFACE} (CPU mask: ${RPS_CPUS})."

# 3. Kernel TCP BBR Congestion Control & Sockets
sysctl -w net.core.rmem_max=67108864 >/dev/null
sysctl -w net.core.wmem_max=67108864 >/dev/null
sysctl -w net.core.busy_poll=50 >/dev/null
sysctl -w net.core.busy_read=50 >/dev/null
sysctl -w net.ipv4.tcp_congestion_control=bbr 2>/dev/null || true
sysctl -w net.ipv4.tcp_fastopen=3 2>/dev/null || true

echo "[✓] Applied TCP BBR and low-latency busy-poll sysctls."

echo "==============================================================================="
echo "GCP C3 & gVNIC host configuration complete."
echo "==============================================================================="
