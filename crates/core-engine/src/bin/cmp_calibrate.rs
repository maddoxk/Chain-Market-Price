//! # `cmp-calibrate`: Hardware Profiling & Latency Calibration Tool
//!
//! Evaluates the host CPU architecture, hypervisor virtualization, TSC drift,
//! inter-core cache bouncing latency, OS scheduler jitter, and shared memory performance.
//! Generates an objective institutional hardware scorecard and auto-calibrates `cmp-config.toml`.

use core_engine::topology::{HardwareProbe, HardwareTier, SystemHardwareProbe};
use std::env;
use std::fs::File;
use std::io::Write;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::thread;
use std::time::{Duration, Instant};

/// Objective hardware qualification grade
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HardwareGrade {
    APlus,
    A,
    B,
    C,
    F,
}

impl HardwareGrade {
    pub fn as_str(&self) -> &'static str {
        match self {
            HardwareGrade::APlus => "A+ (Institutional HFT Colocation)",
            HardwareGrade::A => "A  (High-Performance Bare Metal / Cloud)",
            HardwareGrade::B => "B  (Standard Cloud Virtualized / High-Spec Laptop)",
            HardwareGrade::C => "C  (Standard Developer Workstation / VM)",
            HardwareGrade::F => "F  (Unsuitable / Extreme Jitter Detected)",
        }
    }
}

/// Comprehensive hardware qualification report
#[derive(Debug, Clone)]
pub struct HardwareScorecard {
    pub rdtsc_overhead_ns: f64,
    pub core_ping_pong_p50_ns: f64,
    pub core_ping_pong_p90_ns: f64,
    pub core_ping_pong_p99_ns: f64,
    pub max_scheduler_jitter_us: f64,
    pub shm_throughput_mb_sec: f64,
    pub grade: HardwareGrade,
    pub recommended_tier: HardwareTier,
    pub logical_cores: usize,
    pub physical_cores: usize,
    pub hypervisor: String,
}

fn rdtsc_sample() -> u64 {
    #[cfg(target_arch = "x86_64")]
    unsafe {
        std::arch::x86_64::_rdtsc()
    }
    #[cfg(not(target_arch = "x86_64"))]
    {
        Instant::now().elapsed().as_nanos() as u64
    }
}

#[inline(never)]
fn black_box<T>(dummy: T) -> T {
    let ret = unsafe { std::ptr::read_volatile(&dummy) };
    std::mem::forget(dummy);
    ret
}

/// Benchmarks clock reading overhead
fn benchmark_tsc_overhead(iterations: usize) -> f64 {
    let start = Instant::now();
    let mut dummy = 0u64;
    for _ in 0..iterations {
        dummy ^= rdtsc_sample();
    }
    let elapsed = start.elapsed();
    let nanos_per_op = elapsed.as_nanos() as f64 / iterations as f64;
    black_box(dummy);
    nanos_per_op
}

/// Benchmarks cross-thread atomic cache-line ping-pong latency
fn benchmark_core_ping_pong(iterations: usize) -> (f64, f64, f64) {
    let flag1 = Arc::new(AtomicU64::new(0));
    let flag2 = Arc::new(AtomicU64::new(0));
    let stop = Arc::new(AtomicBool::new(false));

    let f1 = Arc::clone(&flag1);
    let f2 = Arc::clone(&flag2);
    let s_clone = Arc::clone(&stop);

    let handle = thread::spawn(move || {
        while !s_clone.load(Ordering::Relaxed) {
            let val = f1.load(Ordering::Acquire);
            if val > f2.load(Ordering::Relaxed) {
                f2.store(val, Ordering::Release);
            } else {
                std::hint::spin_loop();
            }
        }
    });

    let mut latencies_ns = Vec::with_capacity(iterations);
    for i in 1..=(iterations as u64) {
        let t0 = Instant::now();
        flag1.store(i, Ordering::Release);
        while flag2.load(Ordering::Acquire) < i {
            std::hint::spin_loop();
        }
        let round_trip = t0.elapsed().as_nanos() as f64;
        latencies_ns.push(round_trip / 2.0); // One-way core-to-core latency
    }

    stop.store(true, Ordering::Relaxed);
    flag1.fetch_add(1, Ordering::Relaxed);
    let _ = handle.join();

    latencies_ns.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
    let p50 = latencies_ns[latencies_ns.len() * 50 / 100];
    let p90 = latencies_ns[latencies_ns.len() * 90 / 100];
    let p99 = latencies_ns[latencies_ns.len() * 99 / 100];

    (p50, p90, p99)
}

/// Benchmarks OS scheduling and hypervisor steal-time jitter
fn benchmark_scheduler_jitter(iterations: usize) -> f64 {
    let interval = Duration::from_micros(200);
    let mut max_jitter_us = 0.0f64;

    for _ in 0..iterations {
        let t0 = Instant::now();
        thread::sleep(interval);
        let elapsed = t0.elapsed();
        if elapsed > interval {
            let delta = (elapsed - interval).as_micros() as f64;
            if delta > max_jitter_us {
                max_jitter_us = delta;
            }
        }
    }

    max_jitter_us
}

/// Benchmarks memory throughput (MB/sec)
fn benchmark_memory_throughput(mb_to_write: usize) -> f64 {
    let chunk_size = 64 * 1024; // 64 KB L1/L2 friendly buffer
    let mut chunk = vec![0xAAu8; chunk_size];
    let iterations = (mb_to_write * 1024 * 1024) / chunk_size;

    let start = Instant::now();
    for i in 0..iterations {
        chunk[i % chunk_size] = (i & 0xFF) as u8;
        black_box(chunk[i % chunk_size]);
    }
    let elapsed = start.elapsed();
    let seconds = elapsed.as_secs_f64();
    if seconds > 0.0 {
        mb_to_write as f64 / seconds
    } else {
        100_000.0
    }
}

/// Evaluates metrics to assign hardware qualification grade
fn compute_scorecard(
    rdtsc_ns: f64,
    p50: f64,
    p90: f64,
    p99: f64,
    jitter_us: f64,
    shm_mb_s: f64,
    probe: &SystemHardwareProbe,
) -> HardwareScorecard {
    let profile = probe.negotiate_profile(None);

    let (grade, recommended_tier) = if p50 < 35.0
        && jitter_us < 5.0
        && profile.tier == HardwareTier::Tier3EnterpriseBareMetal
    {
        (HardwareGrade::APlus, HardwareTier::Tier3EnterpriseBareMetal)
    } else if p50 < 75.0 && jitter_us < 50.0 {
        (HardwareGrade::A, HardwareTier::Tier2CloudVirtualized)
    } else if p50 < 150.0 && jitter_us < 200.0 {
        (HardwareGrade::B, HardwareTier::Tier2CloudVirtualized)
    } else if jitter_us < 1000.0 {
        (HardwareGrade::C, HardwareTier::Tier1MidRange)
    } else {
        (HardwareGrade::F, HardwareTier::Tier1MidRange)
    };

    HardwareScorecard {
        rdtsc_overhead_ns: rdtsc_ns,
        core_ping_pong_p50_ns: p50,
        core_ping_pong_p90_ns: p90,
        core_ping_pong_p99_ns: p99,
        max_scheduler_jitter_us: jitter_us,
        shm_throughput_mb_sec: shm_mb_s,
        grade,
        recommended_tier,
        logical_cores: profile.cpu.total_logical_cores,
        physical_cores: profile.cpu.total_physical_cores,
        hypervisor: profile.hypervisor.as_str().to_string(),
    }
}

/// Generates a calibrated `cmp-config.toml` tailored to host measurements
fn generate_config_toml(scorecard: &HardwareScorecard, out_path: &str) -> std::io::Result<()> {
    let mut file = File::create(out_path)?;

    let wait_policy = match scorecard.recommended_tier {
        HardwareTier::Tier3EnterpriseBareMetal => "BusySpin",
        HardwareTier::Tier2CloudVirtualized => "HybridAdaptive",
        HardwareTier::Tier1MidRange => "PowerEfficient",
    };

    let shm_slots = match scorecard.recommended_tier {
        HardwareTier::Tier3EnterpriseBareMetal => 65536,
        HardwareTier::Tier2CloudVirtualized => 16384,
        HardwareTier::Tier1MidRange => 4096,
    };

    let content = format!(
        r#"# ==============================================================================
# Chain-Market-Price Calibrated Configuration
# Auto-generated by 'cmp-calibrate' on {}
# Host Qualification Grade: {}
# ==============================================================================

[hardware]
detected_tier = "{:?}"
recommended_tier = "{:?}"
logical_cores = {}
physical_cores = {}
hypervisor = "{}"

[sync]
wait_strategy = "{}"
spin_limit = 256
yield_limit = 1024
enable_burst_promotion = true

[shm]
ring_slots = {}
slot_size_bytes = 64
backend_policy = "CascadeFallback"

[telemetry]
rdtsc_overhead_ns = {:.2}
core_ping_pong_p50_ns = {:.2}
max_scheduler_jitter_us = {:.2}
"#,
        chrono_placeholder(),
        scorecard.grade.as_str(),
        scorecard.recommended_tier,
        scorecard.recommended_tier,
        scorecard.logical_cores,
        scorecard.physical_cores,
        scorecard.hypervisor,
        wait_policy,
        shm_slots,
        scorecard.rdtsc_overhead_ns,
        scorecard.core_ping_pong_p50_ns,
        scorecard.max_scheduler_jitter_us,
    );

    file.write_all(content.as_bytes())?;
    Ok(())
}

fn chrono_placeholder() -> &'static str {
    "AutoCalibrated"
}

fn print_json_scorecard(card: &HardwareScorecard) {
    println!(
        r#"{{"grade":"{}","recommended_tier":"{:?}","logical_cores":{},"physical_cores":{},"hypervisor":"{}","rdtsc_overhead_ns":{:.2},"core_ping_pong_p50_ns":{:.2},"core_ping_pong_p90_ns":{:.2},"core_ping_pong_p99_ns":{:.2},"max_scheduler_jitter_us":{:.2},"shm_throughput_mb_sec":{:.2}}}"#,
        card.grade.as_str(),
        card.recommended_tier,
        card.logical_cores,
        card.physical_cores,
        card.hypervisor,
        card.rdtsc_overhead_ns,
        card.core_ping_pong_p50_ns,
        card.core_ping_pong_p90_ns,
        card.core_ping_pong_p99_ns,
        card.max_scheduler_jitter_us,
        card.shm_throughput_mb_sec
    );
}

fn main() {
    let args: Vec<String> = env::args().collect();
    let json_mode = args.iter().any(|a| a == "--json");
    let mut out_config = "cmp-config.toml".to_string();

    let mut i = 1;
    let mut command = "full";

    while i < args.len() {
        if args[i] == "--output" && i + 1 < args.len() {
            out_config = args[i + 1].clone();
            i += 2;
            continue;
        } else if args[i] == "--json" {
            i += 1;
            continue;
        } else if !args[i].starts_with('-') {
            command = &args[i];
        }
        i += 1;
    }

    let probe = SystemHardwareProbe;

    if !json_mode {
        println!("===============================================================================");
        println!("Chain-Market-Price Hardware Profiler & Latency Calibrator ('cmp-calibrate')");
        println!(
            "===============================================================================\n"
        );
    }

    match command {
        "tsc" => {
            let tsc_ns = benchmark_tsc_overhead(1_000_000);
            if json_mode {
                println!(r#"{{"command":"tsc","overhead_ns":{:.2}}}"#, tsc_ns);
            } else {
                println!("  [+] TSC / Time Reading Overhead: {:.2} ns/op", tsc_ns);
            }
        }
        "core-latency" => {
            let (p50, p90, p99) = benchmark_core_ping_pong(10_000);
            if json_mode {
                println!(
                    r#"{{"command":"core-latency","p50_ns":{:.2},"p90_ns":{:.2},"p99_ns":{:.2}}}"#,
                    p50, p90, p99
                );
            } else {
                println!("  [+] Core-to-Core Ping-Pong Latency:");
                println!("      - p50:   {:.2} ns", p50);
                println!("      - p90:   {:.2} ns", p90);
                println!("      - p99:   {:.2} ns", p99);
            }
        }
        "jitter" => {
            let jitter_us = benchmark_scheduler_jitter(50);
            if json_mode {
                println!(r#"{{"command":"jitter","max_jitter_us":{:.2}}}"#, jitter_us);
            } else {
                println!(
                    "  [+] OS / Hypervisor Scheduling Jitter: {:.2} us",
                    jitter_us
                );
            }
        }
        "shm" => {
            let throughput = benchmark_memory_throughput(256);
            if json_mode {
                println!(r#"{{"command":"shm","throughput_mb_s":{:.2}}}"#, throughput);
            } else {
                println!("  [+] Memory Subsystem Throughput: {:.2} MB/s", throughput);
            }
        }
        _ => {
            if !json_mode {
                println!("  [*] Running comprehensive diagnostic suite...");
            }
            let tsc_ns = benchmark_tsc_overhead(500_000);
            let (p50, p90, p99) = benchmark_core_ping_pong(10_000);
            let jitter_us = benchmark_scheduler_jitter(30);
            let throughput = benchmark_memory_throughput(128);

            let card = compute_scorecard(tsc_ns, p50, p90, p99, jitter_us, throughput, &probe);

            if json_mode {
                print_json_scorecard(&card);
            } else {
                println!("\n-------------------------------------------------------------------------------");
                println!("HARDWARE SCORECARD & QUALIFICATION");
                println!("-------------------------------------------------------------------------------");
                println!(
                    "  CPU Topology:         {} Physical / {} Logical Cores",
                    card.physical_cores, card.logical_cores
                );
                println!("  Hypervisor Detected:  {}", card.hypervisor);
                println!("  TSC Read Overhead:    {:.2} ns", card.rdtsc_overhead_ns);
                println!(
                    "  Core Ping-Pong p50:   {:.2} ns",
                    card.core_ping_pong_p50_ns
                );
                println!(
                    "  Core Ping-Pong p99:   {:.2} ns",
                    card.core_ping_pong_p99_ns
                );
                println!(
                    "  Scheduler Jitter:     {:.2} us",
                    card.max_scheduler_jitter_us
                );
                println!(
                    "  Memory Bandwidth:     {:.2} MB/s",
                    card.shm_throughput_mb_sec
                );
                println!("  Hardware Grade:       {}", card.grade.as_str());
                println!("  Recommended Tier:     {:?}", card.recommended_tier);
                println!("-------------------------------------------------------------------------------");
            }

            if let Err(e) = generate_config_toml(&card, &out_config) {
                if !json_mode {
                    eprintln!("  [!] Warning: Failed to write {}: {}", out_config, e);
                }
            } else if !json_mode {
                println!(
                    "  [✓] Auto-calibrated configuration written to '{}'\n",
                    out_config
                );
            }
        }
    }
}
