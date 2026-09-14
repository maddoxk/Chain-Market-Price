#!/usr/bin/env bash
# ==============================================================================
# Chain-Market-Price: Bare-Metal HFT Environment Pre-Flight Validator
# Inspects hardware, kernel parameters, CPU governors, HugePages, and clocksource
# ensuring guaranteed zero-jitter, determinism, and sub-microsecond latency SLAs.
# ==============================================================================

set -uo pipefail

TOTAL_CHECKS=0
PASSED_CHECKS=0
WARNINGS=0
CRITICAL_FAILS=0

report_pass() {
    local desc="$1"
    echo -e "  \033[32m[PASS]\033[0m ${desc}"
    ((PASSED_CHECKS++))
    ((TOTAL_CHECKS++))
}

report_warn() {
    local desc="$1"
    local advice="$2"
    echo -e "  \033[33m[WARN]\033[0m ${desc} -> \033[90m(${advice})\033[0m"
    ((WARNINGS++))
    ((TOTAL_CHECKS++))
}

report_fail() {
    local desc="$1"
    local advice="$2"
    echo -e "  \033[31m[FAIL]\033[0m ${desc} -> \033[90m(${advice})\033[0m"
    ((CRITICAL_FAILS++))
    ((TOTAL_CHECKS++))
}

echo "================================================================================"
echo "   CHAIN-MARKET-PRICE: PRODUCTION PRE-FLIGHT SYSTEM VALIDATOR                  "
echo "================================================================================"
echo "Timestamp: $(date -u +"%Y-%m-%dT%H:%M:%SZ")"
echo "Hostname:  $(hostname 2>/dev/null || echo 'localhost')"
echo "OS / Arch: $(uname -s) $(uname -m) $(uname -r 2>/dev/null || true)"
echo ""

# ------------------------------------------------------------------------------
# 1. Operating System & Kernel Platform
# ------------------------------------------------------------------------------
echo "--- [1/6] Operating System & Hardware Invariants ---"
if [[ "$(uname -s)" == "Linux" ]]; then
    report_pass "Host OS is Linux bare-metal / enterprise distribution."
else
    report_warn "Host OS is $(uname -s) (Non-Linux development environment)." \
                "Deploy on Linux AMD EPYC / Intel Xeon Platinum for production HFT."
fi

# ------------------------------------------------------------------------------
# 2. CPU Frequency Governor & C-States
# ------------------------------------------------------------------------------
echo "--- [2/6] CPU Frequency & Power Management ---"
if [[ -f "/sys/devices/system/cpu/cpu0/cpufreq/scaling_governor" ]]; then
    GOV=$(cat /sys/devices/system/cpu/cpu0/cpufreq/scaling_governor 2>/dev/null || echo "unknown")
    if [[ "${GOV}" == "performance" ]]; then
        report_pass "CPU scaling governor is set to 'performance'."
    else
        report_fail "CPU governor is '${GOV}' (expected 'performance')." \
                    "Run: cpupower frequency-set -g performance"
    fi
else
    report_warn "cpufreq scaling_governor interface unavailable." \
                "Verify host BIOS has Disabled Intel SpeedStep / AMD Cool'n'Quiet."
fi

if [[ -f "/sys/module/intel_idle/parameters/max_cstate" ]]; then
    CSTATE=$(cat /sys/module/intel_idle/parameters/max_cstate 2>/dev/null || echo "1")
    if [[ "${CSTATE}" == "0" ]]; then
        report_pass "Intel Idle C-States disabled (max_cstate=0)."
    else
        report_warn "Intel Idle max_cstate is ${CSTATE}." \
                    "Boot kernel with intel_idle.max_cstate=0 processor.max_cstate=0."
    fi
fi

# ------------------------------------------------------------------------------
# 3. CPU Core Isolation & Tickless Kernel
# ------------------------------------------------------------------------------
echo "--- [3/6] CPU Core Isolation (isolcpus & nohz_full) ---"
if [[ -f "/sys/devices/system/cpu/isolated" ]]; then
    ISOL=$(cat /sys/devices/system/cpu/isolated 2>/dev/null || true)
    if [[ -n "${ISOL}" ]]; then
        report_pass "Isolated CPU cores detected: ${ISOL}."
    else
        report_warn "No isolated CPU cores detected." \
                    "Boot kernel with isolcpus=1-8 nohz_full=1-8 rcu_nocbs=1-8."
    fi
else
    report_warn "/sys/devices/system/cpu/isolated not present." \
                "Verify isolcpus kernel boot parameters on target production chassis."
fi

# ------------------------------------------------------------------------------
# 4. Invariant Time Stamp Counter (TSC) Clocksource
# ------------------------------------------------------------------------------
echo "--- [4/6] High-Resolution TSC Hardware Clocksource ---"
if [[ -f "/sys/devices/system/clocksource/clocksource0/current_clocksource" ]]; then
    CLOCKSRC=$(cat /sys/devices/system/clocksource/clocksource0/current_clocksource 2>/dev/null || echo "unknown")
    if [[ "${CLOCKSRC}" == "tsc" ]]; then
        report_pass "High-resolution Invariant TSC clocksource active."
    else
        report_warn "Current clocksource is '${CLOCKSRC}' (recommended 'tsc')." \
                    "Verify hypervisor/kernel supports constant_tsc and nonstop_tsc."
    fi
else
    report_warn "Current clocksource sysfs node not found (macOS / container)." \
                "Ensure production bare-metal host uses calibrated invariant TSC."
fi

# ------------------------------------------------------------------------------
# 5. HugePages & Memory Swapping Invariants
# ------------------------------------------------------------------------------
echo "--- [5/6] Virtual Memory, Swappiness & HugePages ---"
if [[ -f "/proc/meminfo" ]]; then
    SWAPPINESS=$(cat /proc/sys/vm/swappiness 2>/dev/null || echo "unknown")
    if [[ "${SWAPPINESS}" == "0" ]]; then
        report_pass "vm.swappiness is set to 0 (swapping disabled)."
    else
        report_warn "vm.swappiness is ${SWAPPINESS} (expected 0)." \
                    "Run: sysctl -w vm.swappiness=0"
    fi

    HUGEPAGES_TOTAL=$(grep -i "HugePages_Total:" /proc/meminfo | awk '{print $2}' || echo "0")
    if [[ "${HUGEPAGES_TOTAL}" -ge 2048 ]]; then
        report_pass "Static HugePages pool configured: ${HUGEPAGES_TOTAL} x 2MB pages."
    else
        report_warn "HugePages_Total is ${HUGEPAGES_TOTAL} (recommended >= 2048)." \
                    "Run: sysctl -w vm.nr_hugepages=2048"
    fi
fi

# ------------------------------------------------------------------------------
# 6. Shared Memory (/dev/shm) IPC Mount & Permissions
# ------------------------------------------------------------------------------
echo "--- [6/6] POSIX Shared Memory Ring Buffer Mount ---"
SHM_DIR="/dev/shm"
if [[ -d "${SHM_DIR}" ]]; then
    if [[ -w "${SHM_DIR}" ]]; then
        report_pass "/dev/shm is mounted and writable."
    else
        report_fail "/dev/shm exists but is not writable by current user." \
                    "Check chmod/chown on /dev/shm."
    fi
else
    report_warn "/dev/shm directory does not exist on this OS." \
                "Ensure tmpfs is mounted at /dev/shm on Linux production server."
fi

echo ""
echo "================================================================================"
echo "   PRE-FLIGHT VALIDATION SUMMARY                                                "
echo "================================================================================"
echo "Total Checks:    ${TOTAL_CHECKS}"
echo "Passed Checks:   ${PASSED_CHECKS}"
echo "Warnings:        ${WARNINGS}"
echo "Critical Errors: ${CRITICAL_FAILS}"
echo ""

if [[ ${CRITICAL_FAILS} -eq 0 ]]; then
    echo -e "\033[32m>>> SYSTEM STATUS: OPERATIONAL & COMPLIANT WITH LOW-LATENCY SLA <<<\033[0m"
    exit 0
else
    echo -e "\033[31m>>> SYSTEM STATUS: NON-COMPLIANT WITH HFT SLA (Review Critical Errors) <<<\033[0m"
    exit 1
fi
