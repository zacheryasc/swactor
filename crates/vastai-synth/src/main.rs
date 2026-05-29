//! `vastai-synth` — synthetic telemetry generator for the vastai node stream.
//!
//! Impersonates the real producers (one external poller + one in-VM monitor per
//! stage), each its own [`VastaiShipper`], and replays a deterministic scenario
//! (see [`vastai_synth::generate`]) into a separately-running collector. The
//! collector is the production `swactor-diag-collector`; point this at it with
//! `--collector-url`. Use the data via the collector's `/diag/runs`,
//! `/diag/stream/{run_id}` (SSE) and `/diag/bundle/{run_id}` endpoints.
//!
//! [`VastaiShipper`]: distribution::diagnostics::vastai::VastaiShipper

use std::process::ExitCode;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

use distribution::diagnostics::vastai::record::{Source, VastaiBody, VastaiNodeRef};
use distribution::diagnostics::vastai::{VastaiShipper, VastaiShipperConfig, VastaiShipperHandle};
use vastai_synth::{Producer, ScenarioParams, generate};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Mode {
    Live,
    Backfill,
    Dump,
}

struct Config {
    collector_url: String,
    mode: Mode,
    stages: usize,
    duration_ms: u64,
    seed: u64,
    tick_ms: u64,
    poll_ms: u64,
    speed: f64,
    run_id: String,
    label: String,
    out: Option<String>,
}

const USAGE: &str = "Usage:
  vastai-synth [--collector-url URL] [--mode live|backfill|dump] [--stages N]
               [--duration SECS] [--seed N] [--tick MS] [--poll MS]
               [--speed N] [--run-id ID] [--label NAME] [--out PATH]

Options:
  --collector-url URL  Base URL of a running swactor-diag-collector
                       (default http://localhost:9080). Ignored in dump mode.
  --mode MODE          live: real-time ticks into a collector (SSE looks live);
                       backfill: compressed wall-clock into a collector + finalize → bundle;
                       dump: no collector — stream the scenario as NDJSON
                       forever, paced in real time (Ctrl-C to stop)
                       (default live)
  --stages N           In-VM stage nodes; 1 external poller is added (default 4)
  --duration SECS      Scenario length in seconds (default 180)
  --seed N             Seed for reproducible measurement noise (default 42)
  --tick MS            In-VM sampler cadence (default 1000)
  --poll MS            External poller cadence (default 5000)
  --speed N            Time-compression factor: backfill delivery and dump
                       pacing (default 60; use a small value like 1 for dump)
  --run-id ID          Collector run id (default vastai-synth-{seed}-{epoch})
  --label NAME         Run/label prefix (default pp-synth)
  --out PATH           Dump-mode output file; '-' or omitted means stdout
  -h, --help           Show this help
";

fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

impl Config {
    fn parse(args: &[String]) -> Result<Self, String> {
        let mut collector_url = "http://localhost:9080".to_string();
        let mut mode = Mode::Live;
        let mut stages = 4usize;
        let mut duration_secs = 180u64;
        let mut seed = 42u64;
        let mut tick_ms = 1_000u64;
        let mut poll_ms = 5_000u64;
        let mut speed: Option<f64> = None;
        let mut run_id: Option<String> = None;
        let mut label = "pp-synth".to_string();
        let mut out: Option<String> = None;

        let mut iter = args.iter().skip(1);
        while let Some(arg) = iter.next() {
            let mut next = |what: &str| {
                iter.next()
                    .cloned()
                    .ok_or_else(|| format!("{arg} expects {what}"))
            };
            match arg.as_str() {
                "--collector-url" => collector_url = next("a URL")?,
                "--mode" => {
                    mode = match next("live|backfill|dump")?.as_str() {
                        "live" => Mode::Live,
                        "backfill" => Mode::Backfill,
                        "dump" => Mode::Dump,
                        other => return Err(format!("invalid --mode {other:?}")),
                    }
                }
                "--stages" => stages = parse_num(&next("a count")?, "--stages")?,
                "--duration" => duration_secs = parse_num(&next("seconds")?, "--duration")?,
                "--seed" => seed = parse_num(&next("a number")?, "--seed")?,
                "--tick" => tick_ms = parse_num(&next("milliseconds")?, "--tick")?,
                "--poll" => poll_ms = parse_num(&next("milliseconds")?, "--poll")?,
                "--speed" => {
                    speed = Some(
                        next("a factor")?
                            .parse()
                            .map_err(|e| format!("invalid --speed: {e}"))?,
                    )
                }
                "--run-id" => run_id = Some(next("an id")?),
                "--label" => label = next("a name")?,
                "--out" => out = Some(next("a path")?),
                "-h" | "--help" => {
                    println!("{USAGE}");
                    std::process::exit(0);
                }
                other => return Err(format!("unrecognized argument: {other}")),
            }
        }

        if stages == 0 {
            return Err("--stages must be at least 1".to_string());
        }
        // Dump streams in real time by default (so it reads like a live deploy);
        // backfill compresses hard to finish quickly.
        let speed = speed.unwrap_or(if mode == Mode::Dump { 1.0 } else { 60.0 });
        if speed <= 0.0 {
            return Err("--speed must be positive".to_string());
        }

        let run_id = run_id.unwrap_or_else(|| format!("vastai-synth-{seed}-{}", now_ms()));
        Ok(Self {
            collector_url,
            mode,
            stages,
            duration_ms: duration_secs * 1_000,
            seed,
            tick_ms,
            poll_ms,
            speed,
            run_id,
            label,
            out,
        })
    }
}

fn parse_num<T: std::str::FromStr>(s: &str, flag: &str) -> Result<T, String>
where
    T::Err: std::fmt::Display,
{
    s.parse().map_err(|e| format!("invalid {flag}: {e}"))
}

#[tokio::main(flavor = "multi_thread", worker_threads = 2)]
async fn main() -> ExitCode {
    let args: Vec<String> = std::env::args().collect();
    let config = match Config::parse(&args) {
        Ok(c) => c,
        Err(e) => {
            eprintln!("vastai-synth: {e}\n\n{USAGE}");
            return ExitCode::from(2);
        }
    };

    // Dump mode is fully self-contained: generate the scenario and write it out as
    // NDJSON. No collector, no network, nothing left on disk but the file you ask for.
    if config.mode == Mode::Dump {
        return run_dump(&config).await;
    }

    let (host, port, base) = match parse_http(&config.collector_url) {
        Ok(t) => t,
        Err(e) => {
            eprintln!("vastai-synth: invalid --collector-url {:?}: {e}", config.collector_url);
            return ExitCode::from(2);
        }
    };

    // Preflight: a producer with no reachable collector would silently spool to
    // disk and look like it "worked", so fail loudly up front instead.
    if let Err(e) = preflight(&host, port).await {
        eprintln!(
            "vastai-synth: collector unreachable at {} ({e}).\n  Start it with: \
             swactor-diag-collector --bind 0.0.0.0:{port}\n  or pass --collector-url.",
            config.collector_url
        );
        return ExitCode::FAILURE;
    }

    let params = ScenarioParams::new(config.stages, config.duration_ms, config.seed)
        .with_cadence(config.tick_ms, config.poll_ms)
        .with_base_wall_ms(now_ms())
        .with_label(config.label.clone());
    let records = generate(&params);

    let spool_root = std::env::temp_dir().join(format!("vastai-synth-spool-{}", config.run_id));

    let external = match spawn_shipper(&config, &spool_root, "vastai-external", Source::External, VastaiNodeRef::default()) {
        Ok(s) => s,
        Err(e) => {
            eprintln!("vastai-synth: could not start external shipper: {e}");
            return ExitCode::FAILURE;
        }
    };
    let mut stages: Vec<VastaiShipper> = Vec::with_capacity(config.stages);
    for i in 0..config.stages {
        let node = VastaiNodeRef {
            stage_index: Some(i as u32),
            ..Default::default()
        };
        match spawn_shipper(&config, &spool_root, &format!("vastai-stage-{i}"), Source::InVm, node) {
            Ok(s) => stages.push(s),
            Err(e) => {
                eprintln!("vastai-synth: could not start stage-{i} shipper: {e}");
                return ExitCode::FAILURE;
            }
        }
    }

    let handles: Vec<VastaiShipperHandle> = std::iter::once(external.handle())
        .chain(stages.iter().map(|s| s.handle()))
        .collect();

    eprintln!(
        "vastai-synth: run_id={} mode={:?} stages={} ({} records over {}s, speed x{})",
        config.run_id,
        config.mode,
        config.stages,
        records.len(),
        config.duration_ms / 1000,
        if config.mode == Mode::Backfill { config.speed } else { 1.0 },
    );
    eprintln!("  live stream: GET {}{}/diag/stream/{}", config.collector_url, base, config.run_id);
    eprintln!("  run list:    GET {}{}/diag/runs", config.collector_url, base);
    eprintln!("  bundle:      GET {}{}/diag/bundle/{}", config.collector_url, base, config.run_id);

    let start = Instant::now();
    for rec in &records {
        let target_ms = match config.mode {
            Mode::Backfill => (rec.at_ms as f64 / config.speed) as u64,
            // Live; Dump exits before this loop.
            _ => rec.at_ms,
        };
        let target = Duration::from_millis(target_ms);
        let elapsed = start.elapsed();
        if target > elapsed {
            tokio::time::sleep(target - elapsed).await;
        }

        let base_shipper = match rec.producer {
            Producer::External => &external,
            Producer::Stage(i) => &stages[i],
        };
        let shipper = base_shipper.with_node(rec.node.clone());
        match &rec.body {
            VastaiBody::Instance(o) => shipper.instance(o.clone()),
            VastaiBody::HostSample(h) => shipper.sample(h.clone()),
            VastaiBody::Logs(b) => shipper.logs(b.clone()),
            VastaiBody::Lifecycle(e) => shipper.lifecycle(e.clone()),
        }
    }

    // Flush every shipper (delivers pending + drains spool once) before finalize.
    for h in &handles {
        h.shutdown().await;
    }
    let delivered: u64 = handles.iter().map(|h| h.delivered_count()).sum();

    if config.mode == Mode::Backfill {
        match post_finalize(&host, port, &base, &config.run_id).await {
            Ok(()) => eprintln!("vastai-synth: finalized run; bundle written"),
            Err(e) => eprintln!("vastai-synth: finalize POST failed: {e}"),
        }
    }

    eprintln!(
        "vastai-synth: done — delivered {delivered}/{} records to {} nodes",
        records.len(),
        config.stages + 1
    );
    let _ = std::fs::remove_dir_all(&spool_root);
    ExitCode::SUCCESS
}

/// Stream the scenario forever as NDJSON (one `PlannedRecord` per line) to
/// `--out` (or stdout). Each record is paced to its scheduled `at_ms` (divided by
/// `--speed`), so output trickles out at the deploy's real cadence instead of
/// blasting at CPU speed. When a scenario cycle ends it's regenerated with a
/// fresh wall clock and the stream continues, modelling a deploy that never stops.
async fn run_dump(config: &Config) -> ExitCode {
    use std::io::{BufWriter, Write};
    use std::time::Instant;

    let to_stdout = config.out.as_deref().map(|p| p == "-").unwrap_or(true);
    let mut sink: Box<dyn Write> = if to_stdout {
        Box::new(BufWriter::new(std::io::stdout().lock()))
    } else {
        let path = config.out.as_deref().unwrap();
        match std::fs::File::create(path) {
            Ok(f) => Box::new(BufWriter::new(f)),
            Err(e) => {
                eprintln!("vastai-synth: cannot write {path:?}: {e}");
                return ExitCode::FAILURE;
            }
        }
    };

    eprintln!(
        "vastai-synth: streaming scenario as NDJSON ({} stages + external poller, {}s cycle, speed x{}, seed {}){}",
        config.stages,
        config.duration_ms / 1000,
        config.speed,
        config.seed,
        config
            .out
            .as_deref()
            .filter(|p| *p != "-")
            .map(|p| format!(" → {p}"))
            .unwrap_or_default(),
    );
    eprintln!("vastai-synth: streaming forever — Ctrl-C to stop.");

    // A single wall anchor across all cycles keeps pacing drift-free; each cycle's
    // records are offset by the cumulative duration of the cycles before it.
    let start = Instant::now();
    let cycle_ms = (config.duration_ms as f64 / config.speed).max(1.0) as u64;
    let mut cycle_offset_ms: u64 = 0;

    loop {
        let params = ScenarioParams::new(config.stages, config.duration_ms, config.seed)
            .with_cadence(config.tick_ms, config.poll_ms)
            .with_base_wall_ms(now_ms())
            .with_label(config.label.clone());
        let records = generate(&params);

        for rec in &records {
            let target_ms = cycle_offset_ms + (rec.at_ms as f64 / config.speed) as u64;
            let target = Duration::from_millis(target_ms);
            let elapsed = start.elapsed();
            if target > elapsed {
                tokio::time::sleep(target - elapsed).await;
            }

            // Serialization is infallible; a closed pipe (reader quit) is normal —
            // stop quietly rather than treating it as an error.
            let line = serde_json::to_string(rec).expect("serialize record");
            if writeln!(sink, "{line}").is_err() || sink.flush().is_err() {
                return ExitCode::SUCCESS;
            }
        }

        cycle_offset_ms += cycle_ms;
    }
}

fn spawn_shipper(
    config: &Config,
    spool_root: &std::path::Path,
    node_id: &str,
    source: Source,
    node: VastaiNodeRef,
) -> std::io::Result<VastaiShipper> {
    let cfg = VastaiShipperConfig::new(
        config.collector_url.as_str(),
        config.run_id.as_str(),
        node_id,
        spool_root.join(node_id),
    )
    .with_drain_interval(Duration::from_millis(250))
    .with_request_timeout(Duration::from_secs(5));
    VastaiShipper::spawn(cfg, node, source)
}

/// Parse `http://host[:port][/base]` → (host, port, base). http-only, matching the
/// collector/shipper constraint (inside-VPC service, no TLS).
fn parse_http(url: &str) -> Result<(String, u16, String), String> {
    let rest = url
        .strip_prefix("http://")
        .ok_or_else(|| "must start with http://".to_string())?;
    let (authority, path) = match rest.find('/') {
        Some(i) => (&rest[..i], &rest[i..]),
        None => (rest, ""),
    };
    if authority.is_empty() {
        return Err("missing host".to_string());
    }
    let (host, port) = match authority.rfind(':') {
        Some(i) => (
            authority[..i].to_string(),
            authority[i + 1..]
                .parse()
                .map_err(|e| format!("invalid port: {e}"))?,
        ),
        None => (authority.to_string(), 80u16),
    };
    Ok((host, port, path.trim_end_matches('/').to_string()))
}

async fn preflight(host: &str, port: u16) -> std::io::Result<()> {
    match tokio::time::timeout(Duration::from_secs(3), TcpStream::connect((host, port))).await {
        Ok(r) => r.map(|_| ()),
        Err(_) => Err(std::io::Error::new(
            std::io::ErrorKind::TimedOut,
            "connect timed out",
        )),
    }
}

async fn post_finalize(host: &str, port: u16, base: &str, run_id: &str) -> Result<(), String> {
    let mut stream = TcpStream::connect((host, port))
        .await
        .map_err(|e| format!("connect: {e}"))?;
    let req = format!(
        "POST {base}/diag/finalize HTTP/1.1\r\n\
         host: {host}:{port}\r\n\
         connection: close\r\n\
         content-type: application/json\r\n\
         content-length: 0\r\n\
         x-run-id: {run_id}\r\n\
         x-node-id: vastai-external\r\n\
         x-node-send-ms: {}\r\n\r\n",
        now_ms()
    );
    stream
        .write_all(req.as_bytes())
        .await
        .map_err(|e| format!("write: {e}"))?;
    stream.flush().await.ok();
    let mut buf = Vec::with_capacity(256);
    stream
        .read_to_end(&mut buf)
        .await
        .map_err(|e| format!("read: {e}"))?;
    let head = String::from_utf8_lossy(&buf);
    let status = head
        .split_whitespace()
        .nth(1)
        .and_then(|s| s.parse::<u16>().ok())
        .ok_or_else(|| "bad response".to_string())?;
    if (200..300).contains(&status) {
        Ok(())
    } else {
        Err(format!("status {status}"))
    }
}
