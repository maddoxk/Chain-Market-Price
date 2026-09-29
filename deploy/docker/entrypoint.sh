#!/bin/bash
set -eo pipefail

echo "==============================================================================="
echo "Chain-Market-Price Container Runtime Initializer"
echo "==============================================================================="

# 1. Probing Container Shared Memory (/dev/shm)
if [ -d "/dev/shm" ]; then
    SHM_AVAIL_MB=$(df -m /dev/shm | awk 'NR==2 {print $2}')
    echo "[+] Detected /dev/shm capacity: ${SHM_AVAIL_MB} MB"
    if [ "$SHM_AVAIL_MB" -lt 256 ]; then
        echo "[!] WARNING: /dev/shm is below recommended size (256 MB+)."
        echo "    For optimal zero-copy IPC throughput, run Docker with: --shm-size=512m"
        echo "    Or in Kubernetes, mount an emptyDir volume with medium: Memory at /dev/shm."
    fi
else
    echo "[!] /dev/shm not present. In-memory /tmp cascading fallback will be used."
fi

# 2. Inspecting Cgroups v2 CPU Quotas
if [ -f "/sys/fs/cgroup/cpu.max" ]; then
    read -r QUOTA PERIOD < /sys/fs/cgroup/cpu.max
    if [ "$QUOTA" != "max" ] && [ -n "$PERIOD" ] && [ "$PERIOD" -gt 0 ]; then
        EFFECTIVE_CORES=$(awk "BEGIN {print $QUOTA / $PERIOD}")
        echo "[+] Cgroups v2 CPU limit detected: ${EFFECTIVE_CORES} cores (quota=${QUOTA}, period=${PERIOD})"
        export CMP_CGROUP_QUOTA="${EFFECTIVE_CORES}"
    else
        echo "[+] No CFS CPU quota throttling detected on container."
    fi
fi

# 3. Environment Profile Defaulting
if [ -z "$CMP_HARDWARE_PROFILE" ]; then
    echo "[+] Defaulting CMP_HARDWARE_PROFILE to 'tier2' (Cloud Virtualized / Container)."
    export CMP_HARDWARE_PROFILE="tier2"
else
    echo "[+] Active operational profile override: ${CMP_HARDWARE_PROFILE}"
fi

echo "==============================================================================="
echo "[+] Launching service: $@"
echo "==============================================================================="

exec "$@"
