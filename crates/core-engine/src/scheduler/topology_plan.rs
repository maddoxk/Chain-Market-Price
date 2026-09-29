//! # Execution Topology Plan & Worker Models
//!
//! Defines execution planning abstractions for matching pipeline worker stages
//! to host hardware characteristics, CPU core count, and cgroup quotas.

use std::collections::HashMap;
use std::fmt;

/// Distinct operational stages of the institutional trading engine
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum WorkerRole {
    /// Ingress network/hardware packet reception (NIC / socket)
    Ingestion,
    /// Fast JSON / binary parsing and normalization to NormalizedBbo
    ParserNormalizer,
    /// High-frequency order book sequencing and synthetic quote generation
    SequencerOrderBook,
    /// Ultra-low-latency Shared Memory ring distribution
    ShmEgress,
    /// External network distribution (WebSocket / multicast)
    NetworkEgress,
    /// Metrics collection, hardware timestamp logging, and telemetry
    Telemetry,
}

impl WorkerRole {
    pub fn as_str(&self) -> &'static str {
        match self {
            WorkerRole::Ingestion => "Ingestion",
            WorkerRole::ParserNormalizer => "ParserNormalizer",
            WorkerRole::SequencerOrderBook => "SequencerOrderBook",
            WorkerRole::ShmEgress => "ShmEgress",
            WorkerRole::NetworkEgress => "NetworkEgress",
            WorkerRole::Telemetry => "Telemetry",
        }
    }
}

impl fmt::Display for WorkerRole {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.as_str())
    }
}

/// A group of worker roles assigned to an execution thread
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WorkerGroup {
    pub name: String,
    pub roles: Vec<WorkerRole>,
    pub assigned_core: Option<usize>,
    pub real_time_priority: Option<i32>,
}

/// Negotiated hardware-matched thread and execution plan
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ExecutionTopologyPlan {
    /// Tier 3: 1:1 dedicated core pinning for each pipeline stage
    DedicatedPinned {
        core_mapping: HashMap<WorkerRole, usize>,
    },
    /// Tier 2: Fused stages mapped to available vCPUs
    GroupedAffinity { groups: Vec<WorkerGroup> },
    /// Tier 1: Single/dual cooperative event loop (macOS, unprivileged, or <= 2 cores)
    CooperativeMultiplexed { worker_threads: usize },
}

impl ExecutionTopologyPlan {
    pub fn summary(&self) -> String {
        match self {
            ExecutionTopologyPlan::DedicatedPinned { core_mapping } => {
                let mut mappings: Vec<String> = core_mapping
                    .iter()
                    .map(|(role, core)| format!("{}:Core{}", role, core))
                    .collect();
                mappings.sort();
                format!("DedicatedPinned [{}]", mappings.join(", "))
            }
            ExecutionTopologyPlan::GroupedAffinity { groups } => {
                let desc: Vec<String> = groups
                    .iter()
                    .map(|g| {
                        let roles: Vec<&'static str> = g.roles.iter().map(|r| r.as_str()).collect();
                        format!(
                            "{}(core={:?}): [{}]",
                            g.name,
                            g.assigned_core,
                            roles.join("+")
                        )
                    })
                    .collect();
                format!(
                    "GroupedAffinity [{} groups: {}]",
                    groups.len(),
                    desc.join("; ")
                )
            }
            ExecutionTopologyPlan::CooperativeMultiplexed { worker_threads } => {
                format!("CooperativeMultiplexed [threads: {}]", worker_threads)
            }
        }
    }

    /// Returns the total number of OS threads spawned by this plan
    pub fn thread_count(&self) -> usize {
        match self {
            ExecutionTopologyPlan::DedicatedPinned { core_mapping } => core_mapping.len(),
            ExecutionTopologyPlan::GroupedAffinity { groups } => groups.len(),
            ExecutionTopologyPlan::CooperativeMultiplexed { worker_threads } => *worker_threads,
        }
    }
}
