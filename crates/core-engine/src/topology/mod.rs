//! # Hardware & Hypervisor Topology Auto-Detection Engine
//!
//! Provides runtime discovery of host CPU architecture, cache hierarchy,
//! hypervisor virtualization, container constraints, and OS privileges.
//! Dynamically negotiates optimal execution profiles:
//! - Tier 1: Mid-Range Workstation (laptops, developer machines, power-aware)
//! - Tier 2: Cloud Virtualized (AWS EC2, GCP, Docker/K8s, vCPU-sharing aware)
//! - Tier 3: Enterprise Bare Metal (colocation, dual EPYC/Xeon, HugeTLB, isolcpus)

use std::fmt;
#[cfg(target_os = "linux")]
use std::fs;
use std::path::Path;

/// Operational hardware tiers supported by the engine
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum HardwareTier {
    /// Tier 1: Mid-range laptops, developer workstations (macOS / Linux, 4-8 cores, unprivileged)
    Tier1MidRange = 1,
    /// Tier 2: Cloud virtualized environments (AWS EC2, GCP GCE, Azure, Docker, Kubernetes)
    Tier2CloudVirtualized = 2,
    /// Tier 3: Enterprise bare-metal colocation (Dual AMD EPYC / Xeon, 16+ cores, isolcpus, HugeTLB)
    Tier3EnterpriseBareMetal = 3,
}

impl HardwareTier {
    pub fn as_str(&self) -> &'static str {
        match self {
            HardwareTier::Tier1MidRange => "Tier1_MidRange",
            HardwareTier::Tier2CloudVirtualized => "Tier2_CloudVirtualized",
            HardwareTier::Tier3EnterpriseBareMetal => "Tier3_EnterpriseBareMetal",
        }
    }
}

impl fmt::Display for HardwareTier {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.as_str())
    }
}

/// Detected virtualization or container runtime environment
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum HypervisorKind {
    None,
    AwsNitro,
    Kvm,
    Xen,
    HyperV,
    GcpCompute,
    DockerContainer,
    Unknown(String),
}

impl HypervisorKind {
    pub fn is_virtualized(&self) -> bool {
        !matches!(self, HypervisorKind::None)
    }

    pub fn as_str(&self) -> &str {
        match self {
            HypervisorKind::None => "BareMetal(None)",
            HypervisorKind::AwsNitro => "AwsNitro",
            HypervisorKind::Kvm => "KVM",
            HypervisorKind::Xen => "Xen",
            HypervisorKind::HyperV => "HyperV",
            HypervisorKind::GcpCompute => "GcpCompute",
            HypervisorKind::DockerContainer => "DockerContainer",
            HypervisorKind::Unknown(name) => name.as_str(),
        }
    }
}

/// Host CPU architecture and cache hierarchy
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CpuTopology {
    pub total_logical_cores: usize,
    pub total_physical_cores: usize,
    pub numa_nodes: usize,
    pub is_hybrid: bool,
    pub performance_cores: Vec<usize>,
    pub efficiency_cores: Vec<usize>,
    pub l1d_cache_bytes: usize,
    pub l2_cache_bytes: usize,
    pub l3_cache_bytes: usize,
    pub has_invariant_tsc: bool,
}

impl Default for CpuTopology {
    fn default() -> Self {
        Self {
            total_logical_cores: 4,
            total_physical_cores: 4,
            numa_nodes: 1,
            is_hybrid: false,
            performance_cores: (0..4).collect(),
            efficiency_cores: Vec::new(),
            l1d_cache_bytes: 32 * 1024,
            l2_cache_bytes: 512 * 1024,
            l3_cache_bytes: 8 * 1024 * 1024,
            has_invariant_tsc: false,
        }
    }
}

/// Non-fatal probed OS capabilities and kernel limits
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EnvironmentCapabilities {
    pub has_hugetlb: bool,
    pub has_mlock: bool,
    pub has_sched_fifo: bool,
    pub has_raw_sockets: bool,
    pub cgroup_cpu_quota_us: Option<u64>,
    pub cgroup_cpu_period_us: Option<u64>,
    pub max_locked_memory_bytes: u64,
}

impl Default for EnvironmentCapabilities {
    fn default() -> Self {
        Self {
            has_hugetlb: false,
            has_mlock: false,
            has_sched_fifo: false,
            has_raw_sockets: false,
            cgroup_cpu_quota_us: None,
            cgroup_cpu_period_us: None,
            max_locked_memory_bytes: 0,
        }
    }
}

/// Negotiated runtime operational profile
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RuntimeProfile {
    pub tier: HardwareTier,
    pub hypervisor: HypervisorKind,
    pub cpu: CpuTopology,
    pub caps: EnvironmentCapabilities,
    pub auto_detected: bool,
    pub override_reason: Option<String>,
}

impl RuntimeProfile {
    pub fn summary(&self) -> String {
        format!(
            "Tier: {} | Hypervisor: {} | Cores: {}P/{}L | NUMA: {} | InvariantTSC: {} | HugeTLB: {} | FIFO: {}",
            self.tier,
            self.hypervisor.as_str(),
            self.cpu.total_physical_cores,
            self.cpu.total_logical_cores,
            self.cpu.numa_nodes,
            self.cpu.has_invariant_tsc,
            self.caps.has_hugetlb,
            self.caps.has_sched_fifo
        )
    }
}

/// Hardware and environment probing interface
pub trait HardwareProbe: Send + Sync {
    /// Detect if running in a hypervisor or container
    fn detect_hypervisor(&self) -> HypervisorKind;

    /// Discover CPU core topology and cache line properties
    fn detect_cpu_topology(&self) -> CpuTopology;

    /// Non-fatal probe of available OS privileges
    fn detect_capabilities(&self) -> EnvironmentCapabilities;

    /// Negotiate the optimal profile
    fn negotiate_profile(&self, override_tier: Option<HardwareTier>) -> RuntimeProfile {
        let hypervisor = self.detect_hypervisor();
        let cpu = self.detect_cpu_topology();
        let caps = self.detect_capabilities();

        // 1. Check explicit override (argument or env var)
        let env_override = std::env::var("CMP_HARDWARE_PROFILE")
            .or_else(|_| std::env::var("CMP_PROFILE"))
            .ok()
            .and_then(|val| match val.to_lowercase().as_str() {
                "tier1" | "midrange" | "1" => Some(HardwareTier::Tier1MidRange),
                "tier2" | "cloud" | "virtualized" | "2" => {
                    Some(HardwareTier::Tier2CloudVirtualized)
                }
                "tier3" | "baremetal" | "enterprise" | "3" => {
                    Some(HardwareTier::Tier3EnterpriseBareMetal)
                }
                _ => None,
            });

        let chosen_override = override_tier.or(env_override);

        if let Some(tier) = chosen_override {
            return RuntimeProfile {
                tier,
                hypervisor,
                cpu,
                caps,
                auto_detected: false,
                override_reason: Some(format!("Operator override set to {}", tier)),
            };
        }

        // 2. Automated Negotiation Rules
        let tier = if !hypervisor.is_virtualized()
            && cpu.total_physical_cores >= 16
            && caps.has_hugetlb
            && caps.has_sched_fifo
        {
            // Tier 3: Bare metal enterprise server with real-time capability and >= 16 cores
            HardwareTier::Tier3EnterpriseBareMetal
        } else if hypervisor.is_virtualized()
            || caps.cgroup_cpu_quota_us.is_some()
            || (cpu.total_logical_cores >= 8 && !cfg!(target_os = "macos"))
        {
            // Tier 2: Cloud virtual machine, container, or server without bare-metal privileges
            HardwareTier::Tier2CloudVirtualized
        } else {
            // Tier 1: Mid-range computer, developer laptop, or low core count
            HardwareTier::Tier1MidRange
        };

        RuntimeProfile {
            tier,
            hypervisor,
            cpu,
            caps,
            auto_detected: true,
            override_reason: None,
        }
    }
}

/// Concrete runtime hardware prober
pub struct SystemHardwareProbe;

impl Default for SystemHardwareProbe {
    fn default() -> Self {
        Self
    }
}

impl HardwareProbe for SystemHardwareProbe {
    fn detect_hypervisor(&self) -> HypervisorKind {
        // 1. Container check
        if Path::new("/.dockerenv").exists() || Path::new("/run/.containerenv").exists() {
            return HypervisorKind::DockerContainer;
        }

        // 2. Linux DMI inspection
        #[cfg(target_os = "linux")]
        {
            if let Ok(product) = fs::read_to_string("/sys/class/dmi/id/product_name") {
                let p = product.trim().to_lowercase();
                if p.contains("amazon ec2") || p.contains("nitro") {
                    return HypervisorKind::AwsNitro;
                }
                if p.contains("google") || p.contains("compute engine") {
                    return HypervisorKind::GcpCompute;
                }
                if p.contains("kvm") || p.contains("qemu") {
                    return HypervisorKind::Kvm;
                }
                if p.contains("vmware") {
                    return HypervisorKind::Unknown("VMware".to_string());
                }
                if p.contains("hyper-v") || p.contains("virtual machine") {
                    return HypervisorKind::HyperV;
                }
            }

            if let Ok(vendor) = fs::read_to_string("/sys/class/dmi/id/sys_vendor") {
                let v = vendor.trim().to_lowercase();
                if v.contains("amazon") {
                    return HypervisorKind::AwsNitro;
                }
                if v.contains("google") {
                    return HypervisorKind::GcpCompute;
                }
                if v.contains("qemu") {
                    return HypervisorKind::Kvm;
                }
                if v.contains("xen") {
                    return HypervisorKind::Xen;
                }
            }

            // Check /proc/cpuinfo hypervisor flag
            if let Ok(cpuinfo) = fs::read_to_string("/proc/cpuinfo") {
                if cpuinfo.contains("hypervisor") {
                    return HypervisorKind::Kvm; // Generic KVM/Xen fallback
                }
            }
        }

        // 3. macOS / Darwin inspection
        #[cfg(target_os = "macos")]
        {
            // On macOS, developers run either locally on Apple Silicon / Intel Mac,
            // or inside a virtualization guest (Parallels/UTM/Virtualization.framework)
            if let Ok(output) = std::process::Command::new("sysctl")
                .arg("-n")
                .arg("kern.hv_vmm_present")
                .output()
            {
                let text = String::from_utf8_lossy(&output.stdout).trim().to_string();
                if text == "1" {
                    return HypervisorKind::Unknown("AppleHypervisorGuest".to_string());
                }
            }
        }

        HypervisorKind::None
    }

    fn detect_cpu_topology(&self) -> CpuTopology {
        let mut logical_cores = 4;

        #[cfg(target_os = "macos")]
        {
            if let Ok(output) = std::process::Command::new("sysctl")
                .arg("-n")
                .arg("hw.logicalcpu")
                .output()
            {
                if let Ok(val) = String::from_utf8_lossy(&output.stdout)
                    .trim()
                    .parse::<usize>()
                {
                    logical_cores = val;
                }
            }
        }

        #[cfg(target_os = "linux")]
        {
            if let Ok(cpuinfo) = fs::read_to_string("/proc/cpuinfo") {
                let count = cpuinfo
                    .lines()
                    .filter(|l| l.starts_with("processor"))
                    .count();
                if count > 0 {
                    logical_cores = count;
                }
            }
        }

        let mut physical_cores = logical_cores;
        #[allow(unused_mut)]
        let mut numa_nodes = 1;
        let mut is_hybrid = false;
        let mut perf_cores = Vec::new();
        let mut eff_cores = Vec::new();
        #[allow(unused_mut)]
        let mut has_invariant_tsc = false;

        #[cfg(target_os = "linux")]
        {
            // Detect NUMA nodes via /sys/devices/system/node/
            if let Ok(entries) = fs::read_dir("/sys/devices/system/node") {
                let count = entries
                    .filter_map(|e| e.ok())
                    .filter(|e| e.file_name().to_string_lossy().starts_with("node"))
                    .count();
                if count > 0 {
                    numa_nodes = count;
                }
            }

            // Detect invariant TSC
            if let Ok(cpuinfo) = fs::read_to_string("/proc/cpuinfo") {
                if cpuinfo.contains("constant_tsc") && cpuinfo.contains("nonstop_tsc") {
                    has_invariant_tsc = true;
                }
                // Physical core heuristic
                let mut core_ids = std::collections::HashSet::new();
                for line in cpuinfo.lines() {
                    if line.starts_with("core id") {
                        if let Some(id) = line.split(':').nth(1) {
                            core_ids.insert(id.trim());
                        }
                    }
                }
                if !core_ids.is_empty() {
                    physical_cores = core_ids.len() * numa_nodes.max(1);
                }
            }
        }

        #[cfg(target_os = "macos")]
        {
            if let Ok(output) = std::process::Command::new("sysctl")
                .arg("-n")
                .arg("hw.physicalcpu")
                .output()
            {
                if let Ok(val) = String::from_utf8_lossy(&output.stdout)
                    .trim()
                    .parse::<usize>()
                {
                    physical_cores = val;
                }
            }

            // Apple Silicon hybrid topology (Performance vs Efficiency cores)
            let p_count = std::process::Command::new("sysctl")
                .arg("-n")
                .arg("hw.perflevel0.physicalcpu")
                .output()
                .ok()
                .and_then(|o| {
                    String::from_utf8_lossy(&o.stdout)
                        .trim()
                        .parse::<usize>()
                        .ok()
                })
                .unwrap_or(0);

            let e_count = std::process::Command::new("sysctl")
                .arg("-n")
                .arg("hw.perflevel1.physicalcpu")
                .output()
                .ok()
                .and_then(|o| {
                    String::from_utf8_lossy(&o.stdout)
                        .trim()
                        .parse::<usize>()
                        .ok()
                })
                .unwrap_or(0);

            if p_count > 0 && e_count > 0 {
                is_hybrid = true;
                // E-cores mapped first on Darwin scheduler, then P-cores
                for i in 0..e_count {
                    eff_cores.push(i);
                }
                for i in 0..p_count {
                    perf_cores.push(e_count + i);
                }
            }
        }

        if perf_cores.is_empty() {
            perf_cores = (0..logical_cores).collect();
        }

        CpuTopology {
            total_logical_cores: logical_cores,
            total_physical_cores: physical_cores.max(1),
            numa_nodes: numa_nodes.max(1),
            is_hybrid,
            performance_cores: perf_cores,
            efficiency_cores: eff_cores,
            l1d_cache_bytes: 32 * 1024,
            l2_cache_bytes: 512 * 1024,
            l3_cache_bytes: 16 * 1024 * 1024,
            has_invariant_tsc,
        }
    }

    fn detect_capabilities(&self) -> EnvironmentCapabilities {
        #[allow(unused_mut)]
        let mut caps = EnvironmentCapabilities::default();

        #[cfg(target_os = "linux")]
        {
            // Probe cgroup v2 CPU quota
            if let Ok(quota_str) = fs::read_to_string("/sys/fs/cgroup/cpu.max") {
                let parts: Vec<&str> = quota_str.split_whitespace().collect();
                if parts.len() == 2 && parts[0] != "max" {
                    caps.cgroup_cpu_quota_us = parts[0].parse().ok();
                    caps.cgroup_cpu_period_us = parts[1].parse().ok();
                }
            }

            // Probe HugeTLB availability via sysfs
            if Path::new("/sys/kernel/mm/hugepages/hugepages-2048kB").exists() {
                if let Ok(nr) =
                    fs::read_to_string("/sys/kernel/mm/hugepages/hugepages-2048kB/nr_hugepages")
                {
                    if let Ok(count) = nr.trim().parse::<usize>() {
                        if count > 0 {
                            caps.has_hugetlb = true;
                        }
                    }
                }
            }

            // Check non-fatal SCHED_FIFO privilege
            let param = 1i32;
            let res = unsafe {
                libc::sched_setscheduler(
                    0,
                    libc::SCHED_FIFO,
                    &param as *const _ as *const libc::sched_param,
                )
            };
            if res == 0 {
                caps.has_sched_fifo = true;
                // Restore standard round-robin scheduler
                let zero = 0i32;
                unsafe {
                    libc::sched_setscheduler(
                        0,
                        libc::SCHED_OTHER,
                        &zero as *const _ as *const libc::sched_param,
                    );
                }
            }
        }

        caps
    }
}

/// Mock hardware probe for testing arbitrary hardware matrices
#[derive(Debug, Clone)]
pub struct MockHardwareProbe {
    pub mock_hypervisor: HypervisorKind,
    pub mock_cpu: CpuTopology,
    pub mock_caps: EnvironmentCapabilities,
}

impl MockHardwareProbe {
    /// Creates a mock Tier 1 Developer Laptop (4 physical cores, macOS/Linux, unprivileged)
    pub fn new_tier1_laptop() -> Self {
        Self {
            mock_hypervisor: HypervisorKind::None,
            mock_cpu: CpuTopology {
                total_logical_cores: 8,
                total_physical_cores: 4,
                numa_nodes: 1,
                is_hybrid: false,
                performance_cores: vec![0, 1, 2, 3],
                efficiency_cores: vec![],
                l1d_cache_bytes: 32 * 1024,
                l2_cache_bytes: 512 * 1024,
                l3_cache_bytes: 8 * 1024 * 1024,
                has_invariant_tsc: false,
            },
            mock_caps: EnvironmentCapabilities::default(),
        }
    }

    /// Creates a mock Tier 2 AWS Nitro Cloud VM (8 vCPUs, cgroup limits)
    pub fn new_tier2_aws_nitro() -> Self {
        Self {
            mock_hypervisor: HypervisorKind::AwsNitro,
            mock_cpu: CpuTopology {
                total_logical_cores: 8,
                total_physical_cores: 4,
                numa_nodes: 1,
                is_hybrid: false,
                performance_cores: (0..8).collect(),
                efficiency_cores: vec![],
                l1d_cache_bytes: 32 * 1024,
                l2_cache_bytes: 1024 * 1024,
                l3_cache_bytes: 32 * 1024 * 1024,
                has_invariant_tsc: true,
            },
            mock_caps: EnvironmentCapabilities {
                has_hugetlb: true,
                has_mlock: true,
                has_sched_fifo: false,
                has_raw_sockets: false,
                cgroup_cpu_quota_us: Some(400_000),
                cgroup_cpu_period_us: Some(100_000),
                max_locked_memory_bytes: 64 * 1024 * 1024,
            },
        }
    }

    /// Creates a mock Tier 3 Bare-Metal Enterprise Server (64 cores, isolcpus, HugeTLB, SCHED_FIFO)
    pub fn new_tier3_bare_metal() -> Self {
        Self {
            mock_hypervisor: HypervisorKind::None,
            mock_cpu: CpuTopology {
                total_logical_cores: 128,
                total_physical_cores: 64,
                numa_nodes: 2,
                is_hybrid: false,
                performance_cores: (0..128).collect(),
                efficiency_cores: vec![],
                l1d_cache_bytes: 32 * 1024,
                l2_cache_bytes: 1024 * 1024,
                l3_cache_bytes: 256 * 1024 * 1024,
                has_invariant_tsc: true,
            },
            mock_caps: EnvironmentCapabilities {
                has_hugetlb: true,
                has_mlock: true,
                has_sched_fifo: true,
                has_raw_sockets: true,
                cgroup_cpu_quota_us: None,
                cgroup_cpu_period_us: None,
                max_locked_memory_bytes: 128 * 1024 * 1024 * 1024,
            },
        }
    }
}

impl HardwareProbe for MockHardwareProbe {
    fn detect_hypervisor(&self) -> HypervisorKind {
        self.mock_hypervisor.clone()
    }

    fn detect_cpu_topology(&self) -> CpuTopology {
        self.mock_cpu.clone()
    }

    fn detect_capabilities(&self) -> EnvironmentCapabilities {
        self.mock_caps.clone()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_mock_tier1_laptop_negotiation() {
        let probe = MockHardwareProbe::new_tier1_laptop();
        let profile = probe.negotiate_profile(None);
        assert_eq!(profile.tier, HardwareTier::Tier1MidRange);
        assert!(!profile.hypervisor.is_virtualized());
        assert_eq!(profile.cpu.total_physical_cores, 4);
        assert!(profile.auto_detected);
    }

    #[test]
    fn test_mock_tier2_aws_nitro_negotiation() {
        let probe = MockHardwareProbe::new_tier2_aws_nitro();
        let profile = probe.negotiate_profile(None);
        assert_eq!(profile.tier, HardwareTier::Tier2CloudVirtualized);
        assert_eq!(profile.hypervisor, HypervisorKind::AwsNitro);
        assert!(profile.caps.cgroup_cpu_quota_us.is_some());
    }

    #[test]
    fn test_mock_tier3_bare_metal_negotiation() {
        let probe = MockHardwareProbe::new_tier3_bare_metal();
        let profile = probe.negotiate_profile(None);
        assert_eq!(profile.tier, HardwareTier::Tier3EnterpriseBareMetal);
        assert!(!profile.hypervisor.is_virtualized());
        assert_eq!(profile.cpu.total_physical_cores, 64);
        assert!(profile.caps.has_sched_fifo);
        assert!(profile.caps.has_hugetlb);
    }

    #[test]
    fn test_explicit_operator_override() {
        let probe = MockHardwareProbe::new_tier1_laptop();
        // Override Tier 1 machine to run in Tier 3 mode
        let profile = probe.negotiate_profile(Some(HardwareTier::Tier3EnterpriseBareMetal));
        assert_eq!(profile.tier, HardwareTier::Tier3EnterpriseBareMetal);
        assert!(!profile.auto_detected);
        assert!(profile.override_reason.is_some());
    }

    #[test]
    fn test_system_hardware_probe_live_execution() {
        let probe = SystemHardwareProbe;
        let profile = probe.negotiate_profile(None);
        assert!(profile.cpu.total_logical_cores >= 1);
        println!("Live Detected Profile: {}", profile.summary());
    }
}
