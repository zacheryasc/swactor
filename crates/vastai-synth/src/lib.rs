//! Synthetic scenario generator for the vastai node telemetry stream.
//!
//! This crate produces realistic, *reproducible* telemetry for a pipeline of
//! rented vast.ai GPU nodes without touching a real cloud. [`generate`] is a pure,
//! deterministic function of [`ScenarioParams`]: given a seed it returns the exact
//! same ordered list of [`PlannedRecord`]s every time. The companion binary
//! (`vastai-synth`) replays that list through the real [`VastaiShipper`] producers
//! into a running collector; the records themselves are independent of transport.
//!
//! The generated story mirrors what the two real producers emit:
//!   * the **external poller** (orchestrator-side) — [`InstanceObservation`]s and
//!     [`LifecycleEvent`]s per contract;
//!   * the **in-VM monitor** (one per rented container) — [`HostSample`]s and
//!     [`LogBatch`]es.
//!
//! [`VastaiShipper`]: distribution::diagnostics::vastai::VastaiShipper

use distribution::diagnostics::vastai::record::{
    CgroupSample, CpuSample, DiskSample, GpuSample, HostSample, InstanceObservation, LifecycleEvent,
    LogBatch, LogLine, LogStream, MemSample, NetSample, VastaiBody, VastaiNodeRef,
};

pub use distribution::diagnostics::vastai::record;

/// Which producer a record belongs to — selects the shipper the driver routes it
/// through. `External` is the single orchestrator-side poller; `Stage(i)` is the
/// in-VM monitor of the i-th rented container.
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize)]
#[serde(rename_all = "snake_case")]
pub enum Producer {
    External,
    Stage(usize),
}

/// One scheduled record: what to ship, who ships it, the node it's about, and when
/// (relative to the start of the run) to emit it.
#[derive(Debug, Clone, serde::Serialize)]
pub struct PlannedRecord {
    /// Emission offset from run start, in milliseconds. Records are returned sorted
    /// by this; the driver paces real or compressed wall-clock against it.
    pub at_ms: u64,
    pub producer: Producer,
    pub node: VastaiNodeRef,
    pub body: VastaiBody,
}

/// Inputs to [`generate`]. Construct with [`ScenarioParams::new`] and tune via the
/// `with_*` setters.
#[derive(Debug, Clone)]
pub struct ScenarioParams {
    /// Number of in-VM stage nodes (1 external poller is always added on top).
    pub stages: usize,
    /// Total scenario length in milliseconds.
    pub duration_ms: u64,
    /// Seed for the deterministic measurement noise.
    pub seed: u64,
    /// Absolute epoch-ms for scenario t=0. Stamped into `LogLine.wall_ms` and
    /// `InstanceObservation.start_date` so timestamps are coherent for a consumer.
    pub base_wall_ms: u64,
    /// In-VM sampler cadence.
    pub tick_ms: u64,
    /// External poller cadence.
    pub poll_ms: u64,
    /// Run/label prefix; per-stage labels are `{label}-stage-{i}`.
    pub label: String,
}

impl ScenarioParams {
    pub fn new(stages: usize, duration_ms: u64, seed: u64) -> Self {
        Self {
            stages,
            duration_ms,
            seed,
            base_wall_ms: 1_761_000_000_000,
            tick_ms: 1_000,
            poll_ms: 5_000,
            label: "pp-synth".to_string(),
        }
    }

    pub fn with_base_wall_ms(mut self, ms: u64) -> Self {
        self.base_wall_ms = ms;
        self
    }

    pub fn with_cadence(mut self, tick_ms: u64, poll_ms: u64) -> Self {
        self.tick_ms = tick_ms.max(1);
        self.poll_ms = poll_ms.max(1);
        self
    }

    pub fn with_label(mut self, label: impl Into<String>) -> Self {
        self.label = label.into();
        self
    }
}

/// splitmix64 — small, fast, fully deterministic across platforms. Used only for
/// measurement noise so a fixed seed reproduces identical record bodies.
struct Rng(u64);

impl Rng {
    fn new(seed: u64) -> Self {
        Rng(seed)
    }

    fn next_u64(&mut self) -> u64 {
        self.0 = self.0.wrapping_add(0x9E37_79B9_7F4A_7C15);
        let mut z = self.0;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        z ^ (z >> 31)
    }

    fn unit(&mut self) -> f64 {
        (self.next_u64() >> 11) as f64 / (1u64 << 53) as f64
    }

    fn range(&mut self, lo: f64, hi: f64) -> f64 {
        lo + (hi - lo) * self.unit()
    }
}

/// The character of a stage's running container — what it does well, or how it
/// misbehaves. Each variant shapes a different corner of the telemetry so the
/// stream exercises every metric we collect.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Profile {
    /// Textbook run: GPU saturated, temps nominal, everything in band.
    Healthy,
    /// VRAM creeps toward the ceiling until the container is OOM-killed (137).
    Oom,
    /// A transient thermal excursion drives GPU temp up and clocks down.
    TempSpike,
    /// Dataset streaming pins the NVMe: sustained multi-GB/s disk throughput.
    DiskBound,
    /// Weights download slowly over the network; GPU sits data-starved.
    NetBound,
    /// Misconfigured batch size leaves an expensive GPU near-idle the whole run.
    Underutilized,
    /// CPU-side preprocessing saturates the cores and starves the GPU.
    CpuBound,
}

#[derive(Debug, Clone, Copy)]
struct GpuSpec {
    name: &'static str,
    vram_gb: f64,
    vram_mb: f64,
    power_limit_w: f64,
    dph: f64,
    host_ram_kb: u64,
    num_cpus: u32,
    geo: &'static str,
}

const GPUS: [GpuSpec; 8] = [
    // 0 — healthy baseline
    GpuSpec {
        name: "RTX 4090",
        vram_gb: 24.0,
        vram_mb: 24564.0,
        power_limit_w: 450.0,
        dph: 0.41,
        host_ram_kb: 64 * 1024 * 1024,
        num_cpus: 16,
        geo: "US-CA",
    },
    // 1 — stalled image pull, replaced
    GpuSpec {
        name: "A100 SXM4 80GB",
        vram_gb: 80.0,
        vram_mb: 81920.0,
        power_limit_w: 400.0,
        dph: 1.18,
        host_ram_kb: 128 * 1024 * 1024,
        num_cpus: 32,
        geo: "DE-HE",
    },
    // 2 — OOM crash
    GpuSpec {
        name: "RTX 4090",
        vram_gb: 24.0,
        vram_mb: 24564.0,
        power_limit_w: 450.0,
        dph: 0.39,
        host_ram_kb: 64 * 1024 * 1024,
        num_cpus: 16,
        geo: "US-TX",
    },
    // 3 — thermal spike / throttle
    GpuSpec {
        name: "H100 SXM 80GB",
        vram_gb: 80.0,
        vram_mb: 81920.0,
        power_limit_w: 700.0,
        dph: 2.54,
        host_ram_kb: 128 * 1024 * 1024,
        num_cpus: 48,
        geo: "SG-SG",
    },
    // 4 — disk-bound dataset streaming
    GpuSpec {
        name: "L40S",
        vram_gb: 48.0,
        vram_mb: 49140.0,
        power_limit_w: 350.0,
        dph: 0.84,
        host_ram_kb: 96 * 1024 * 1024,
        num_cpus: 24,
        geo: "US-NY",
    },
    // 5 — network-bound weight download
    GpuSpec {
        name: "RTX A6000",
        vram_gb: 48.0,
        vram_mb: 49140.0,
        power_limit_w: 300.0,
        dph: 0.55,
        host_ram_kb: 64 * 1024 * 1024,
        num_cpus: 16,
        geo: "FR-IDF",
    },
    // 6 — chronically underutilized (wasted spend)
    GpuSpec {
        name: "MI300X",
        vram_gb: 192.0,
        vram_mb: 196608.0,
        power_limit_w: 750.0,
        dph: 3.10,
        host_ram_kb: 256 * 1024 * 1024,
        num_cpus: 48,
        geo: "CA-QC",
    },
    // 7 — CPU-bound preprocessing starving the GPU
    GpuSpec {
        name: "RTX 5090",
        vram_gb: 32.0,
        vram_mb: 32768.0,
        power_limit_w: 575.0,
        dph: 0.78,
        host_ram_kb: 96 * 1024 * 1024,
        num_cpus: 24,
        geo: "JP-13",
    },
];

const CID_BASE: u64 = 41_000;
const OFFER_BASE: u64 = 9_000_000;
const REPLACEMENT_OFFSET: u64 = 500;

/// One leased contract's lifetime on a stage.
#[derive(Debug, Clone)]
struct ContractSpan {
    cid: u64,
    offer: u64,
    lease_ms: u64,
    /// When the app inside reaches `running`. `None` ⇒ it never did (stalled).
    running_ms: Option<u64>,
    /// When the contract ends (teardown or self-exit).
    end_ms: u64,
    /// Terminal `actual_status` reported at/after `end_ms` (e.g. `"exited"`).
    terminal_status: Option<&'static str>,
    teardown_reason: &'static str,
    status_msg: Option<&'static str>,
}

#[derive(Debug, Clone)]
struct StagePlan {
    stage: usize,
    gpu: GpuSpec,
    label: String,
    contracts: Vec<ContractSpan>,
    /// Index into `contracts` of the one that runs the app (`None` if none does).
    run_idx: Option<usize>,
    profile: Profile,
}

/// Behavior assigned to each stage slot. The first eight slots each tell a
/// distinct story; beyond that we fall back to healthy runs.
fn stage_profile(stage: usize, stages: usize) -> Profile {
    match stage {
        2 if stages > 2 => Profile::Oom,
        3 if stages > 3 => Profile::TempSpike,
        4 if stages > 4 => Profile::DiskBound,
        5 if stages > 5 => Profile::NetBound,
        6 if stages > 6 => Profile::Underutilized,
        7 if stages > 7 => Profile::CpuBound,
        _ => Profile::Healthy,
    }
}

fn is_stall_stage(stage: usize, stages: usize) -> bool {
    stage == 1 && stages > 1
}

fn plan_stage(stage: usize, p: &ScenarioParams) -> StagePlan {
    let gpu = GPUS[stage % GPUS.len()];
    let label = format!("{}-stage-{}", p.label, stage);
    let lease = 500 + (stage as u64) * 400;
    let warm = 14_000 + (stage as u64) * 1_500;
    let duration = p.duration_ms;

    if is_stall_stage(stage, p.stages) {
        let t_replace = (duration / 4).max(8_000);
        let repl_warm = 12_000;
        let repl_running = t_replace + repl_warm;
        let primary = ContractSpan {
            cid: CID_BASE + stage as u64,
            offer: OFFER_BASE + stage as u64,
            lease_ms: lease,
            running_ms: None,
            end_ms: t_replace,
            terminal_status: Some("offline"),
            teardown_reason: "stalled image pull",
            status_msg: Some("pulling image (layer 7/12) — no progress"),
        };
        let replacement = ContractSpan {
            cid: CID_BASE + stage as u64 + REPLACEMENT_OFFSET,
            offer: OFFER_BASE + stage as u64 + REPLACEMENT_OFFSET,
            lease_ms: t_replace,
            running_ms: Some(repl_running),
            end_ms: duration,
            terminal_status: None,
            teardown_reason: "deploy complete",
            status_msg: None,
        };
        return StagePlan {
            stage,
            gpu,
            label,
            contracts: vec![primary, replacement],
            run_idx: Some(1),
            profile: Profile::Healthy,
        };
    }

    let profile = stage_profile(stage, p.stages);
    // The net-bound node takes much longer to come up: weights trickle in.
    let warm = if profile == Profile::NetBound { warm + 40_000 } else { warm };
    let running = lease + warm;
    let (end_ms, terminal_status, reason, status_msg) = match profile {
        Profile::Oom => {
            let oom = (duration * 2 / 3).max(running + 5_000).min(duration);
            (
                oom,
                Some("exited"),
                "worker exited (OOM)",
                Some("container exited 137 (OOMKilled)"),
            )
        }
        _ => (duration, None, "deploy complete", None),
    };
    let contract = ContractSpan {
        cid: CID_BASE + stage as u64,
        offer: OFFER_BASE + stage as u64,
        lease_ms: lease,
        running_ms: Some(running),
        end_ms,
        terminal_status,
        teardown_reason: reason,
        status_msg,
    };
    StagePlan {
        stage,
        gpu,
        label,
        contracts: vec![contract],
        run_idx: Some(0),
        profile,
    }
}

/// Generate the full deterministic scenario as an `at_ms`-ordered list of records.
pub fn generate(p: &ScenarioParams) -> Vec<PlannedRecord> {
    let mut rng = Rng::new(p.seed);
    let mut out: Vec<PlannedRecord> = Vec::new();
    let plans: Vec<StagePlan> = (0..p.stages).map(|s| plan_stage(s, p)).collect();

    // Run-level: deploy start.
    out.push(PlannedRecord {
        at_ms: 0,
        producer: Producer::External,
        node: VastaiNodeRef {
            label: Some(p.label.clone()),
            ..Default::default()
        },
        body: VastaiBody::Lifecycle(LifecycleEvent::DeployStart {
            num_stages: Some(p.stages as u32),
        }),
    });

    // External view: lifecycle markers + polled instance observations per contract.
    for plan in &plans {
        for c in &plan.contracts {
            let node = VastaiNodeRef {
                contract_id: Some(c.cid),
                stage_index: Some(plan.stage as u32),
                label: Some(plan.label.clone()),
            };
            out.push(PlannedRecord {
                at_ms: c.lease_ms,
                producer: Producer::External,
                node: node.clone(),
                body: VastaiBody::Lifecycle(LifecycleEvent::ContractLeased {
                    contract_id: c.cid,
                    offer_id: Some(c.offer),
                }),
            });

            let mut now = c.lease_ms;
            loop {
                out.push(PlannedRecord {
                    at_ms: now,
                    producer: Producer::External,
                    node: node.clone(),
                    body: VastaiBody::Instance(instance_obs(plan, c, now, p)),
                });
                if now >= c.end_ms {
                    break;
                }
                now = (now + p.poll_ms).min(c.end_ms);
            }

            out.push(PlannedRecord {
                at_ms: c.end_ms,
                producer: Producer::External,
                node,
                body: VastaiBody::Lifecycle(LifecycleEvent::Teardown {
                    contract_id: c.cid,
                    reason: Some(c.teardown_reason.to_string()),
                }),
            });
        }
    }

    // In-VM view: host samples + logs from each stage's running container.
    for plan in &plans {
        push_host_samples(plan, p, &mut rng, &mut out);
        push_logs(plan, p, &mut rng, &mut out);
    }

    out.sort_by_key(|r| r.at_ms);
    out
}

fn smoothstep(x: f64) -> f64 {
    let x = x.clamp(0.0, 1.0);
    x * x * (3.0 - 2.0 * x)
}

fn gpu_util_true(sec_running: f64, profile: Profile) -> f64 {
    let ramp = smoothstep(sec_running / 20.0);
    let util = match profile {
        // Near-idle the whole run: the GPU we're paying a premium for barely works.
        Profile::Underutilized => 10.0 + 4.0 * (sec_running / 7.0).sin(),
        // Sawtooth: bursts of compute punctuated by long waits on the network.
        Profile::NetBound => {
            let phase = (sec_running / 6.0).sin();
            35.0 + 30.0 * phase.max(0.0)
        }
        // GPU is chronically starved while the CPU does the real work.
        Profile::CpuBound => 28.0 + 8.0 * (sec_running / 13.0).sin(),
        _ => 86.0 + 4.0 * (sec_running / 11.0).sin(),
    };
    (util * ramp).clamp(0.0, 100.0)
}

fn vram_frac_true(sec_running: f64, profile: Profile, run_total_sec: f64) -> f64 {
    match profile {
        Profile::Oom => (0.5 + 0.49 * smoothstep(sec_running / run_total_sec.max(1.0))).min(0.995),
        // Tiny working set — the GPU's huge VRAM goes mostly unused.
        Profile::Underutilized => 0.06 + 0.02 * smoothstep(sec_running / 20.0),
        Profile::NetBound => 0.20 + 0.30 * smoothstep(sec_running / 40.0),
        _ => 0.45 + 0.4 * smoothstep(sec_running / 25.0),
    }
}

fn gpu_temp_true(util: f64, t_sec: f64, profile: Profile, spike_center_sec: f64) -> f64 {
    let mut temp = 56.0 + 0.18 * util;
    if let Profile::TempSpike = profile {
        let d = (t_sec - spike_center_sec) / 12.0;
        temp += 16.0 * (-(d * d)).exp();
    }
    temp
}

fn instance_obs(
    plan: &StagePlan,
    c: &ContractSpan,
    now: u64,
    p: &ScenarioParams,
) -> InstanceObservation {
    let g = plan.gpu;
    let spike_center_sec = (p.duration_ms / 2) as f64 / 1000.0;
    let status = status_at(c, now);
    let running = matches!(status, "running");
    let sec_running = c
        .running_ms
        .filter(|_| running)
        .map(|r| (now.saturating_sub(r)) as f64 / 1000.0);

    let (gpu_util, gpu_temp, cpu_util, mem_usage) = if let Some(sec) = sec_running {
        let util = gpu_util_true(sec, plan.profile);
        let temp = gpu_temp_true(util, now as f64 / 1000.0, plan.profile, spike_center_sec);
        let host_used_mb = (g.host_ram_kb as f64 / 1024.0) * (0.3 + 0.25 * smoothstep(sec / 30.0));
        let cpu = match plan.profile {
            Profile::CpuBound => 90.0 + 5.0 * (sec / 9.0).sin(),
            _ => 22.0 + 6.0 * (sec / 9.0).sin(),
        };
        (Some(util), Some(temp), Some(cpu), Some(host_used_mb))
    } else {
        (None, None, None, None)
    };

    let accumulated_cost = g.dph * (now.saturating_sub(c.lease_ms)) as f64 / 3_600_000.0;
    let start_secs = (p.base_wall_ms + c.lease_ms) as f64 / 1000.0;

    InstanceObservation {
        id: c.cid,
        gpu_name: Some(g.name.to_string()),
        gpu_ram: Some(g.vram_gb),
        num_gpus: Some(1),
        geolocation: Some(g.geo.to_string()),
        host_id: Some(7_000 + plan.stage as u64),
        machine_id: Some(33_000 + plan.stage as u64),
        dph_total: Some(g.dph),
        accumulated_cost: Some(accumulated_cost),
        start_date: Some(start_secs),
        actual_status: Some(status.to_string()),
        intended_status: Some("running".to_string()),
        status_msg: status_msg_at(c, now).map(|s| s.to_string()),
        disk_usage: Some(18.0 + (now.saturating_sub(c.lease_ms)) as f64 / 1000.0 * 0.02),
        disk_space: Some(200.0),
        public_ipaddr: Some(format!("203.0.113.{}", 10 + plan.stage)),
        ssh_host: Some(format!("ssh{}.vast.ai", 4 + plan.stage)),
        ssh_port: Some(40_000 + plan.stage as u16),
        gpu_util,
        gpu_temp,
        cpu_util,
        mem_usage,
        inet_up: Some(120.0),
        inet_down: Some(880.0),
        raw: serde_json::json!({
            "id": c.cid,
            "actual_status": status,
            "cur_state": if running { "running" } else { "loading" },
            "gpu_frac": 1.0,
            "ssh_idx": plan.stage,
            "rentable": false,
            "template_hash_id": "synthtmpl",
        }),
    }
}

fn status_at(c: &ContractSpan, now: u64) -> &'static str {
    if now >= c.end_ms {
        if let Some(t) = c.terminal_status {
            return t;
        }
    }
    match c.running_ms {
        Some(r) if now >= r => "running",
        _ if now < c.lease_ms + 2_000 => "created",
        _ => "loading",
    }
}

fn status_msg_at(c: &ContractSpan, now: u64) -> Option<&'static str> {
    if now >= c.end_ms {
        return c.status_msg.or_else(|| match c.terminal_status {
            Some("exited") => Some("container exited 137 (OOMKilled)"),
            _ => None,
        });
    }
    match c.running_ms {
        Some(r) if now >= r => None,
        _ => c.status_msg.or(Some("loading container image")),
    }
}

fn push_host_samples(
    plan: &StagePlan,
    p: &ScenarioParams,
    rng: &mut Rng,
    out: &mut Vec<PlannedRecord>,
) {
    let Some(run_idx) = plan.run_idx else { return };
    let c = &plan.contracts[run_idx];
    let Some(running_ms) = c.running_ms else {
        return;
    };
    let g = plan.gpu;
    let spike_center_sec = (p.duration_ms / 2) as f64 / 1000.0;
    let run_total = (c.end_ms.saturating_sub(running_ms)) as f64 / 1000.0;
    let node = VastaiNodeRef {
        contract_id: Some(c.cid),
        stage_index: Some(plan.stage as u32),
        label: Some(plan.label.clone()),
    };

    let mut now = running_ms;
    while now <= c.end_ms {
        let sec = (now - running_ms) as f64 / 1000.0;
        let t_sec = now as f64 / 1000.0;
        let util = (gpu_util_true(sec, plan.profile) + rng.range(-3.0, 3.0)).clamp(0.0, 100.0);
        let temp = gpu_temp_true(util, t_sec, plan.profile, spike_center_sec) + rng.range(-1.5, 1.5);
        let power = (g.power_limit_w * util / 100.0) * rng.range(0.92, 1.0);
        let vram_used = g.vram_mb * vram_frac_true(sec, plan.profile, run_total);

        let used_frac = match plan.profile {
            Profile::Oom => 0.45 + 0.5 * smoothstep(sec / run_total.max(1.0)),
            Profile::Underutilized => 0.12 + 0.04 * smoothstep(sec / 30.0),
            _ => 0.30 + 0.22 * smoothstep(sec / 30.0),
        };
        let used_kb = (g.host_ram_kb as f64 * used_frac) as u64;

        let cpu_util = match plan.profile {
            // Cores pinned: this is what's bottlenecking the run.
            Profile::CpuBound => 88.0 + 7.0 * (sec / 9.0).sin() + rng.range(-2.0, 2.0),
            _ => 20.0 + 8.0 * (sec / 9.0).sin() + rng.range(-3.0, 3.0),
        }
        .clamp(0.0, 100.0);
        // load1 tracks utilization; a saturated box runs hot above 1.0/core.
        let load_factor = match plan.profile {
            Profile::CpuBound => rng.range(1.05, 1.35),
            _ => rng.range(0.30, 0.55),
        };

        // Disk and network throughput. Most runs do a heavy read burst while
        // warming up then settle; the disk- and net-bound nodes never settle.
        let warming = sec < 10.0;
        let (read_bps, write_bps) = match plan.profile {
            // Streaming a dataset from NVMe for the whole run: multi-GB/s, steady.
            Profile::DiskBound => (rng.range(1.8e9, 3.2e9), rng.range(4.0e8, 9.0e8)),
            _ if warming => (rng.range(3.0e8, 6.0e8), rng.range(1.0e6, 8.0e6)),
            _ => (rng.range(2.0e6, 2.0e7), rng.range(1.0e6, 8.0e6)),
        };
        let (rx_bps, tx_bps) = match plan.profile {
            // Weights still trickling in: download saturates the NIC.
            Profile::NetBound => (rng.range(9.0e8, 1.18e9), rng.range(2.0e6, 1.5e7)),
            _ if warming => (rng.range(5.0e8, 9.0e8), rng.range(5.0e5, 3.0e6)),
            _ => (rng.range(8.0e5, 4.0e6), rng.range(5.0e5, 3.0e6)),
        };
        // The disk-bound node's filesystem fills fast as shards land on local SSD.
        let fs_growth = match plan.profile {
            Profile::DiskBound => sec * 1.5e8,
            _ => sec * 4.0e6,
        } as u64;

        out.push(PlannedRecord {
            at_ms: now,
            producer: Producer::Stage(plan.stage),
            node: node.clone(),
            body: VastaiBody::HostSample(HostSample {
                gpus: vec![GpuSample {
                    index: 0,
                    name: Some(g.name.to_string()),
                    util_pct: Some(util),
                    mem_used_mb: Some(vram_used),
                    mem_total_mb: Some(g.vram_mb),
                    temp_c: Some(temp),
                    power_w: Some(power),
                    power_limit_w: Some(g.power_limit_w),
                }],
                cpu: CpuSample {
                    util_pct: Some(cpu_util),
                    load1: Some(g.num_cpus as f64 * load_factor),
                    load5: Some(g.num_cpus as f64 * load_factor * 0.85),
                    load15: Some(g.num_cpus as f64 * load_factor * 0.7),
                    num_cpus: Some(g.num_cpus),
                },
                mem: MemSample {
                    total_kb: Some(g.host_ram_kb),
                    available_kb: Some(g.host_ram_kb.saturating_sub(used_kb)),
                    used_kb: Some(used_kb),
                    swap_total_kb: Some(8 * 1024 * 1024),
                    swap_free_kb: Some((8 * 1024 * 1024) - (used_kb / 32).min(8 * 1024 * 1024)),
                },
                disk: vec![DiskSample {
                    device: "nvme0n1".to_string(),
                    read_bytes_per_s: Some(read_bps),
                    write_bytes_per_s: Some(write_bps),
                    fs_total_bytes: Some(200 * 1_000_000_000),
                    fs_used_bytes: Some(20_000_000_000 + fs_growth),
                }],
                net: vec![NetSample {
                    iface: "eth0".to_string(),
                    rx_bytes_per_s: Some(rx_bps),
                    tx_bytes_per_s: Some(tx_bps),
                    rx_bytes_total: Some(50_000_000 + (rx_bps * sec) as u64),
                    tx_bytes_total: Some(20_000_000 + (tx_bps * sec) as u64),
                }],
                cgroup: Some(CgroupSample {
                    mem_limit_bytes: Some(g.host_ram_kb * 1024),
                    mem_current_bytes: Some(used_kb * 1024),
                    cpu_quota_cores: Some(g.num_cpus as f64),
                }),
            }),
        });

        if now == c.end_ms {
            break;
        }
        now = (now + p.tick_ms).min(c.end_ms);
    }
}

/// Per-(stage, stream) monotonic line counters.
#[derive(Default)]
struct LineCounters {
    stdout: u64,
    stderr: u64,
}

impl LineCounters {
    fn batch(
        &mut self,
        stream: LogStream,
        at_ms: u64,
        base_wall_ms: u64,
        lines: &[String],
    ) -> LogBatch {
        let counter = match stream {
            LogStream::Stderr => &mut self.stderr,
            _ => &mut self.stdout,
        };
        let framed = lines
            .iter()
            .map(|text| {
                let line = *counter;
                *counter += 1;
                LogLine {
                    wall_ms: base_wall_ms + at_ms,
                    line,
                    text: text.clone(),
                }
            })
            .collect();
        LogBatch {
            stream,
            lines: framed,
        }
    }
}

fn push_logs(plan: &StagePlan, p: &ScenarioParams, rng: &mut Rng, out: &mut Vec<PlannedRecord>) {
    let mut counters = LineCounters::default();
    for (idx, c) in plan.contracts.iter().enumerate() {
        let node = VastaiNodeRef {
            contract_id: Some(c.cid),
            stage_index: Some(plan.stage as u32),
            label: Some(plan.label.clone()),
        };
        let is_run = plan.run_idx == Some(idx);

        let mut emit = |at_ms: u64, stream: LogStream, lines: Vec<String>, counters: &mut LineCounters| {
            out.push(PlannedRecord {
                at_ms,
                producer: Producer::Stage(plan.stage),
                node: node.clone(),
                body: VastaiBody::Logs(counters.batch(stream, at_ms, p.base_wall_ms, &lines)),
            });
        };

        emit(
            c.lease_ms + 2_000,
            LogStream::Stdout,
            vec![
                "[boot] container up; starting entrypoint".to_string(),
                "[boot] pulling image ghcr.io/acme/pp-worker:latest".to_string(),
            ],
            &mut counters,
        );

        match c.running_ms {
            Some(r) => {
                for shard in 1..=4u32 {
                    let at = r.saturating_sub((4 - shard as u64 + 1) * 3_000);
                    emit(
                        at.max(c.lease_ms + 2_500),
                        LogStream::Stdout,
                        vec![format!("loading weights shard {shard}/4")],
                        &mut counters,
                    );
                }
                emit(
                    r,
                    LogStream::Stdout,
                    vec![
                        format!("stage {} ready (rank {}/{})", plan.stage, plan.stage, p.stages),
                        "listening on 0.0.0.0:7000".to_string(),
                    ],
                    &mut counters,
                );

                // A characteristic line that hints at this node's personality.
                if is_run {
                    if let Some((stream, line)) = match plan.profile {
                        Profile::DiskBound => Some((
                            LogStream::Stdout,
                            "[data] streaming shards from /mnt/dataset (nvme0n1 ~2.4 GB/s)".to_string(),
                        )),
                        Profile::NetBound => Some((
                            LogStream::Stderr,
                            "WARN input queue starved; waiting on upstream activations".to_string(),
                        )),
                        Profile::Underutilized => Some((
                            LogStream::Stderr,
                            "WARN batch_size=1 — GPU utilization ~11%; consider larger batches".to_string(),
                        )),
                        Profile::CpuBound => Some((
                            LogStream::Stderr,
                            "WARN tokenizer running on CPU (24 threads pinned); GPU idle waiting".to_string(),
                        )),
                        _ => None,
                    } {
                        emit(r + 4_000, stream, vec![line], &mut counters);
                    }
                }

                let mut t = r + 20_000;
                while t < c.end_ms {
                    let tokens = (t - r) / 1000 * 34;
                    let rate = match plan.profile {
                        // Bottlenecked nodes crawl along at a fraction of the rate.
                        Profile::NetBound | Profile::CpuBound => 4.0 + rng.range(0.0, 3.0),
                        Profile::Underutilized => 8.0 + rng.range(0.0, 3.0),
                        _ => 30.0 + rng.range(0.0, 12.0),
                    };
                    emit(
                        t,
                        LogStream::Stdout,
                        vec![format!("processed {tokens} tokens ({rate:.1} tok/s)")],
                        &mut counters,
                    );
                    t += 20_000;
                }

                if is_run && plan.profile == Profile::Oom {
                    emit(
                        c.end_ms.saturating_sub(800),
                        LogStream::Stderr,
                        vec![
                            "Traceback (most recent call last):".to_string(),
                            "  File \"/app/worker.py\", line 212, in forward".to_string(),
                            "torch.cuda.OutOfMemoryError: CUDA out of memory.".to_string(),
                            "Tried to allocate 2.10 GiB (GPU 0; 79.15 GiB total)".to_string(),
                        ],
                        &mut counters,
                    );
                }
            }
            None => {
                // Stalled container: keeps "pulling" forever, then the pull times out.
                let mut t = c.lease_ms + 8_000;
                while t < c.end_ms {
                    let mb = 180 + ((t - c.lease_ms) / 1000) * 3;
                    emit(
                        t,
                        LogStream::Stdout,
                        vec![format!("still pulling image... ({mb} MB, no progress)")],
                        &mut counters,
                    );
                    t += 8_000;
                }
                emit(
                    c.end_ms,
                    LogStream::Stderr,
                    vec!["image pull timed out after 40s; aborting".to_string()],
                    &mut counters,
                );
            }
        }
    }
}
