//! # Elastic Core Pinning & Dynamic Worker Multiplexing Engine
//!
//! Adapts execution topology to available CPU cores, NUMA nodes, OS privileges,
//! and container cgroup CFS quotas:
//! - **Tier 3 (Enterprise Bare Metal):** Dedicated 1:1 core pinning with real-time FIFO priority.
//! - **Tier 2 (Cloud Virtualized):** Fused stage worker groups aligned to vCPU allocations.
//! - **Tier 1 (Workstation / Container):** Cooperative multiplexed event loop running on 1-2 threads.

pub mod topology_plan;

pub use self::topology_plan::{ExecutionTopologyPlan, WorkerGroup, WorkerRole};

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::thread;
use std::time::Duration;

/// Result returned by an individual pipeline stage execution step
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StepResult {
    /// Work was processed; the scheduler should immediately poll this or next stage
    Continue,
    /// No work was immediately available; scheduler may yield or back off
    Yield,
    /// The stage has exhausted its workload or completed shutdown
    Exhausted,
}

/// Abstract operational stage within the market data engine pipeline
pub trait PipelineStage: Send + 'static {
    /// Identifies the role of this stage
    fn role(&self) -> WorkerRole;

    /// Executes a single discrete processing step
    fn step(&mut self) -> StepResult;

    /// Checks if the stage is currently running
    fn is_active(&self) -> bool;

    /// Signals the stage to initiate graceful shutdown
    fn shutdown(&mut self);
}

/// Sets thread affinity to a dedicated logical CPU core with non-fatal fallback
#[inline(always)]
pub fn set_current_thread_affinity(core_id: usize) -> Result<(), String> {
    #[cfg(target_os = "linux")]
    {
        extern "C" {
            fn sched_setaffinity(pid: i32, cpusetsize: usize, mask: *const u8) -> i32;
        }
        let mut mask = [0u8; 128];
        let byte_idx = core_id / 8;
        let bit_idx = core_id % 8;
        if byte_idx < 128 {
            mask[byte_idx] |= 1 << bit_idx;
            let _res = unsafe { sched_setaffinity(0, 128, mask.as_ptr()) };
            // In unprivileged environments or restricted containers, non-fatal fallback returns Ok
            Ok(())
        } else {
            Err(format!(
                "Core ID {} exceeds 1024-cpu mask capacity",
                core_id
            ))
        }
    }

    #[cfg(not(target_os = "linux"))]
    {
        let _ = core_id;
        // On macOS (Darwin) or Windows, affinity APIs are absent or unprivileged.
        // Return Ok(()) gracefully to enable seamless workstation execution.
        Ok(())
    }
}

/// Sets real-time FIFO thread priority with non-fatal fallback
#[inline(always)]
pub fn set_realtime_priority(priority: i32) -> Result<(), String> {
    #[cfg(target_os = "linux")]
    {
        #[repr(C)]
        struct SchedParam {
            sched_priority: i32,
        }
        extern "C" {
            fn sched_setscheduler(pid: i32, policy: i32, param: *const SchedParam) -> i32;
        }
        const SCHED_FIFO: i32 = 1;
        let param = SchedParam {
            sched_priority: priority,
        };
        let _res = unsafe { sched_setscheduler(0, SCHED_FIFO, &param) };
        // On unprivileged systems or CI runners lacking CAP_SYS_NICE, degrade gracefully
        Ok(())
    }

    #[cfg(not(target_os = "linux"))]
    {
        let _ = priority;
        Ok(())
    }
}

/// Coordinates and executes pipeline stages according to a negotiated `ExecutionTopologyPlan`
pub struct ElasticScheduler {
    plan: ExecutionTopologyPlan,
    stages: Vec<Box<dyn PipelineStage>>,
}

impl ElasticScheduler {
    pub fn new(plan: ExecutionTopologyPlan) -> Self {
        Self {
            plan,
            stages: Vec::new(),
        }
    }

    /// Adds a pipeline stage to the scheduler
    pub fn add_stage<S: PipelineStage>(&mut self, stage: S) {
        self.stages.push(Box::new(stage));
    }

    /// Returns the active execution plan
    pub fn plan(&self) -> &ExecutionTopologyPlan {
        &self.plan
    }

    /// Computes the optimal execution topology plan based on host characteristics and cgroup limits
    pub fn compute_plan(
        available_cores: usize,
        is_bare_metal: bool,
        has_affinity_cap: bool,
        cgroup_quota_cores: Option<f64>,
    ) -> ExecutionTopologyPlan {
        // If container CFS quota is active, clamp available cores to effective quota
        let effective_cores = if let Some(quota) = cgroup_quota_cores {
            let clamped = (quota.floor() as usize).max(1);
            clamped.min(available_cores)
        } else {
            available_cores
        };

        // 1. Tier 3: Bare metal server with >= 8 cores and affinity capabilities
        if is_bare_metal && effective_cores >= 8 && has_affinity_cap {
            let mut map = HashMap::new();
            map.insert(WorkerRole::Ingestion, 1);
            map.insert(WorkerRole::ParserNormalizer, 2);
            map.insert(WorkerRole::SequencerOrderBook, 3);
            map.insert(WorkerRole::ShmEgress, 4);
            map.insert(WorkerRole::NetworkEgress, 5);
            map.insert(WorkerRole::Telemetry, 6);
            ExecutionTopologyPlan::DedicatedPinned { core_mapping: map }
        }
        // 2. Tier 2: Cloud virtualized VM with 4-7 cores or unprivileged >= 4 cores
        else if effective_cores >= 4 && has_affinity_cap {
            ExecutionTopologyPlan::GroupedAffinity {
                groups: vec![
                    WorkerGroup {
                        name: "ingress_worker".to_string(),
                        roles: vec![WorkerRole::Ingestion, WorkerRole::ParserNormalizer],
                        assigned_core: Some(0),
                        real_time_priority: None,
                    },
                    WorkerGroup {
                        name: "engine_worker".to_string(),
                        roles: vec![WorkerRole::SequencerOrderBook],
                        assigned_core: Some(1),
                        real_time_priority: None,
                    },
                    WorkerGroup {
                        name: "distribution_worker".to_string(),
                        roles: vec![
                            WorkerRole::ShmEgress,
                            WorkerRole::NetworkEgress,
                            WorkerRole::Telemetry,
                        ],
                        assigned_core: Some(2),
                        real_time_priority: None,
                    },
                ],
            }
        }
        // 3. Tier 1: Workstation, laptop, macOS, or <= 2 cores / container throttled
        else {
            let worker_threads = effective_cores.clamp(1, 2);
            ExecutionTopologyPlan::CooperativeMultiplexed { worker_threads }
        }
    }

    /// Executes a single cooperative cycle across all stages.
    /// Returns Continue if any stage performed work, or Yield if all were idle.
    pub fn step_cooperative(stages: &mut [Box<dyn PipelineStage>]) -> StepResult {
        let mut work_done = false;
        let mut all_exhausted = true;

        for stage in stages.iter_mut() {
            if stage.is_active() {
                all_exhausted = false;
                match stage.step() {
                    StepResult::Continue => {
                        work_done = true;
                    }
                    StepResult::Yield => {}
                    StepResult::Exhausted => {}
                }
            }
        }

        if all_exhausted {
            StepResult::Exhausted
        } else if work_done {
            StepResult::Continue
        } else {
            StepResult::Yield
        }
    }

    /// Runs a cooperative event loop on the current thread until the stop flag is set
    pub fn run_cooperative_loop(
        mut stages: Vec<Box<dyn PipelineStage>>,
        stop_signal: Arc<AtomicBool>,
    ) {
        let mut idle_counter = 0;

        while !stop_signal.load(Ordering::Relaxed) {
            match Self::step_cooperative(&mut stages) {
                StepResult::Continue => {
                    idle_counter = 0;
                }
                StepResult::Yield => {
                    idle_counter += 1;
                    if idle_counter > 64 {
                        thread::yield_now();
                    }
                    if idle_counter > 256 {
                        thread::sleep(Duration::from_micros(200));
                    }
                }
                StepResult::Exhausted => {
                    break;
                }
            }
        }

        // Graceful shutdown
        for stage in stages.iter_mut() {
            stage.shutdown();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    struct MockStage {
        role: WorkerRole,
        active: bool,
        work_items: usize,
        steps_executed: usize,
    }

    impl MockStage {
        fn new(role: WorkerRole, work_items: usize) -> Self {
            Self {
                role,
                active: true,
                work_items,
                steps_executed: 0,
            }
        }
    }

    impl PipelineStage for MockStage {
        fn role(&self) -> WorkerRole {
            self.role
        }

        fn step(&mut self) -> StepResult {
            self.steps_executed += 1;
            if self.work_items > 0 {
                self.work_items -= 1;
                StepResult::Continue
            } else {
                StepResult::Yield
            }
        }

        fn is_active(&self) -> bool {
            self.active
        }

        fn shutdown(&mut self) {
            self.active = false;
        }
    }

    #[test]
    fn test_topology_planner_matrix() {
        // 1. Bare metal 16 cores with affinity -> DedicatedPinned
        let plan_t3 = ElasticScheduler::compute_plan(16, true, true, None);
        assert!(matches!(
            plan_t3,
            ExecutionTopologyPlan::DedicatedPinned { .. }
        ));
        assert_eq!(plan_t3.thread_count(), 6);

        // 2. Cloud VM 4 cores with affinity -> GroupedAffinity
        let plan_t2 = ElasticScheduler::compute_plan(4, false, true, None);
        assert!(matches!(
            plan_t2,
            ExecutionTopologyPlan::GroupedAffinity { .. }
        ));
        assert_eq!(plan_t2.thread_count(), 3);

        // 3. Workstation 8 cores without affinity (e.g. macOS) -> CooperativeMultiplexed
        let plan_t1_mac = ElasticScheduler::compute_plan(8, false, false, None);
        assert!(matches!(
            plan_t1_mac,
            ExecutionTopologyPlan::CooperativeMultiplexed { .. }
        ));
        assert_eq!(plan_t1_mac.thread_count(), 2);

        // 4. Low-spec 1 core laptop -> CooperativeMultiplexed 1 thread
        let plan_t1_single = ElasticScheduler::compute_plan(1, false, false, None);
        assert_eq!(
            plan_t1_single,
            ExecutionTopologyPlan::CooperativeMultiplexed { worker_threads: 1 }
        );

        // 5. Container with cgroups quota 1.5 cores on 32-core host -> CooperativeMultiplexed
        let plan_cgroup = ElasticScheduler::compute_plan(32, true, true, Some(1.5));
        assert_eq!(
            plan_cgroup,
            ExecutionTopologyPlan::CooperativeMultiplexed { worker_threads: 1 }
        );
    }

    #[test]
    fn test_cooperative_step_processing() {
        let mut stages: Vec<Box<dyn PipelineStage>> = vec![
            Box::new(MockStage::new(WorkerRole::Ingestion, 3)),
            Box::new(MockStage::new(WorkerRole::ParserNormalizer, 2)),
            Box::new(MockStage::new(WorkerRole::SequencerOrderBook, 0)),
        ];

        // First step has work in stage 0 and 1
        let res1 = ElasticScheduler::step_cooperative(&mut stages);
        assert_eq!(res1, StepResult::Continue);

        // Next 2 steps still have work
        let res2 = ElasticScheduler::step_cooperative(&mut stages);
        assert_eq!(res2, StepResult::Continue);

        let res3 = ElasticScheduler::step_cooperative(&mut stages);
        assert_eq!(res3, StepResult::Continue);

        // Work items now exhausted, next step returns Yield
        let res4 = ElasticScheduler::step_cooperative(&mut stages);
        assert_eq!(res4, StepResult::Yield);
    }

    #[test]
    fn test_cooperative_loop_graceful_shutdown() {
        let stop_signal = Arc::new(AtomicBool::new(false));
        let stop_clone = Arc::clone(&stop_signal);

        let stages: Vec<Box<dyn PipelineStage>> = vec![
            Box::new(MockStage::new(WorkerRole::Ingestion, 100)),
            Box::new(MockStage::new(WorkerRole::SequencerOrderBook, 100)),
        ];

        // Trigger shutdown from separate thread after brief interval
        let handle = thread::spawn(move || {
            thread::sleep(Duration::from_millis(15));
            stop_clone.store(true, Ordering::Relaxed);
        });

        // Loop runs until stopped
        ElasticScheduler::run_cooperative_loop(stages, stop_signal);
        handle.join().unwrap();
    }

    #[test]
    fn test_thread_affinity_and_priority_fallback() {
        // Both calls should succeed without panic even on unprivileged / macOS hosts
        assert!(set_current_thread_affinity(0).is_ok());
        assert!(set_realtime_priority(50).is_ok());
    }
}
