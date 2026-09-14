---
id: preflight-validation
title: Production Pre-Flight Validator Runbook
sidebar_label: Pre-Flight Validator
---

# Production Pre-Flight Validator Runbook

Prior to opening market connectivity or committing trading capital, operations engineers execute the pre-flight verification script [`scripts/validate_environment.sh`](https://github.com/maddoxk/Chain-Market-Price/blob/main/scripts/validate_environment.sh).

---

## 1. Running the Pre-Flight Check

```bash
./scripts/validate_environment.sh
```

---

## 2. Invariants Inspected

1. **Operating System Invariant:** Validates Linux bare-metal kernel.
2. **CPU Scaling Governor:** Confirms all trading cores are set to `performance` (no dynamic down-clocking).
3. **C-States / Sleep:** Confirms Intel/AMD idle sleep states are disabled (`max_cstate=0` or `idle=poll`).
4. **CPU Core Isolation:** Confirms `isolcpus` covers cores 1–8.
5. **Hardware Clocksource:** Confirms current clocksource is invariant `tsc`.
6. **Virtual Memory & Swapping:** Verifies `vm.swappiness = 0` and HugePages pool $\ge 2048$.
7. **POSIX Shared Memory Mount:** Confirms `/dev/shm` is mounted with write permissions.

---

## 3. Sample Output on Certified Production Host

```text
================================================================================
   CHAIN-MARKET-PRICE: PRODUCTION PRE-FLIGHT SYSTEM VALIDATOR                  
================================================================================
Timestamp: 2026-09-14T21:00:00Z
Hostname:  ny4-hft-prod-01.firm.internal
OS / Arch: Linux x86_64 5.15.0-100-generic

--- [1/6] Operating System & Hardware Invariants ---
  [PASS] Host OS is Linux bare-metal / enterprise distribution.
--- [2/6] CPU Frequency & Power Management ---
  [PASS] CPU scaling governor is set to 'performance'.
  [PASS] Intel Idle C-States disabled (max_cstate=0).
--- [3/6] CPU Core Isolation (isolcpus & nohz_full) ---
  [PASS] Isolated CPU cores detected: 1-8.
--- [4/6] High-Resolution TSC Hardware Clocksource ---
  [PASS] High-resolution Invariant TSC clocksource active.
--- [5/6] Virtual Memory, Swappiness & HugePages ---
  [PASS] vm.swappiness is set to 0 (swapping disabled).
  [PASS] Static HugePages pool configured: 2048 x 2MB pages.
--- [6/6] POSIX Shared Memory Ring Buffer Mount ---
  [PASS] /dev/shm is mounted and writable.

================================================================================
   PRE-FLIGHT VALIDATION SUMMARY                                                
================================================================================
Total Checks:    7
Passed Checks:   7
Warnings:        0
Critical Errors: 0

>>> SYSTEM STATUS: OPERATIONAL & COMPLIANT WITH LOW-LATENCY SLA <<<
```
