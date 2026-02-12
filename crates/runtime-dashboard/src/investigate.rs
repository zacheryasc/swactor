//! Line-oriented diagnostic protocol for LLM-driven runtime investigation.
//!
//! Send text commands on stdin, receive JSON responses on stdout (one per line).
//! All human-readable diagnostics go to stderr.
//!
//! # Protocol
//!
//! ```text
//! → overview
//! ← {"ok":true,"command":"overview","data":{"workers":4,"actors":120,...}}
//!
//! → hot 5
//! ← {"ok":true,"command":"hot","data":[{"address":"a1b2...","worker_id":2,"mailbox_depth":47},..]}
//!
//! → diff 2
//! ← {"ok":true,"command":"diff","data":{"elapsed_s":2.0,"delta_messages":8432,"msg_per_sec":4216.0,...}}
//! ```

use std::collections::HashMap;
use std::io::{self, BufRead, Write};
use std::sync::Arc;
use std::time::{Duration, Instant};

use serde::Serialize;
use swactor::runtime::Runtime;
use swactor::stats::RuntimeStats;

use crate::collector::StatsCollector;

/// Run the investigate REPL. Blocks until stdin is closed or `quit` is received.
pub fn run_investigate(runtime: Arc<Runtime>, collector: Arc<StatsCollector>) -> io::Result<()> {
    let stdin = io::stdin();
    let mut stdout = io::stdout();

    eprintln!("swactor investigate protocol ready — send `help` for commands");

    for line in stdin.lock().lines() {
        let line = line?;
        let line = line.trim();
        if line.is_empty() {
            continue;
        }

        let parts: Vec<&str> = line.split_whitespace().collect();
        let cmd = parts[0];
        let args = &parts[1..];

        if cmd == "quit" || cmd == "exit" {
            break;
        }

        let response = dispatch_repl(cmd, args, &runtime, &collector);

        stdout.write_all(response.as_bytes())?;
        stdout.write_all(b"\n")?;
        stdout.flush()?;
    }

    Ok(())
}

fn dispatch_repl(cmd: &str, args: &[&str], runtime: &Runtime, collector: &StatsCollector) -> String {
    match cmd {
        "help" => cmd_help(),
        "overview" => cmd_overview(runtime, collector),
        "workers" => cmd_workers(runtime),
        "worker" => cmd_worker(runtime, collector, args),
        "actors" => cmd_actors(runtime, collector, args),
        "actor" => cmd_actor(runtime, collector, args),
        "hot" => cmd_hot(runtime, collector, args),
        "phases" => cmd_phases(runtime, args),
        "diff" => cmd_diff(runtime, collector, args),
        _ => err_response(cmd, &format!("unknown command `{cmd}` — try `help`")),
    }
}

/// Dispatch an investigate command from HTTP query parameters.
///
/// Maps `?cmd=overview`, `?cmd=hot&n=10`, etc. to the appropriate command function.
pub fn dispatch_command(
    cmd: &str,
    params: &HashMap<String, String>,
    runtime: &Runtime,
    collector: &StatsCollector,
) -> String {
    match cmd {
        "help" => cmd_help(),
        "overview" => cmd_overview(runtime, collector),
        "workers" => cmd_workers(runtime),
        "worker" => {
            let id = params.get("id").map(|s| s.as_str()).unwrap_or("");
            cmd_worker(runtime, collector, &[id])
        }
        "actors" => {
            let mut args = Vec::new();
            if let Some(sort) = params.get("sort") {
                args.push("--sort");
                args.push(sort.as_str());
            }
            if let Some(limit) = params.get("limit") {
                args.push("--limit");
                args.push(limit.as_str());
            }
            if let Some(worker) = params.get("worker") {
                args.push("--worker");
                args.push(worker.as_str());
            }
            cmd_actors(runtime, collector, &args)
        }
        "actor" => {
            let prefix = params.get("prefix").map(|s| s.as_str()).unwrap_or("");
            cmd_actor(runtime, collector, &[prefix])
        }
        "hot" => {
            let n = params.get("n").map(|s| s.as_str()).unwrap_or("10");
            cmd_hot(runtime, collector, &[n])
        }
        "phases" => {
            match params.get("worker") {
                Some(w) => cmd_phases(runtime, &[w.as_str()]),
                None => cmd_phases(runtime, &[]),
            }
        }
        "diff" => {
            let secs = params.get("seconds").map(|s| s.as_str()).unwrap_or("");
            cmd_diff(runtime, collector, &[secs])
        }
        _ => err_response(cmd, &format!("unknown command `{cmd}` — try `help`")),
    }
}

// ── Helpers ─────────────────────────────────────────────────────────────

fn ok_response(cmd: &str, data: impl Serialize) -> String {
    serde_json::to_string(&serde_json::json!({
        "ok": true,
        "command": cmd,
        "data": data,
    }))
    .unwrap_or_else(|e| err_response(cmd, &format!("serialization error: {e}")))
}

fn err_response(cmd: &str, msg: &str) -> String {
    serde_json::to_string(&serde_json::json!({
        "ok": false,
        "command": cmd,
        "error": msg,
    }))
    .unwrap()
}

fn format_addr(addr: &swactor::actor::ActorAddress) -> String {
    format!("{addr}")
}

fn full_hex(addr: &swactor::actor::ActorAddress) -> String {
    addr.0.iter().map(|b| format!("{b:02x}")).collect()
}

fn enriched_stats(rt: &Runtime, col: &StatsCollector) -> RuntimeStats {
    let mut s = rt.stats();
    col.enrich(&mut s);
    s
}

// ── Commands ────────────────────────────────────────────────────────────

pub fn cmd_help() -> String {
    ok_response(
        "help",
        serde_json::json!({
            "commands": [
                {"name": "overview",   "usage": "overview",                   "description": "Summary: worker count, actor count, total messages, mailbox depth, panics"},
                {"name": "workers",    "usage": "workers",                    "description": "Per-worker stats: actors, mailbox depth, messages, sends (local/cross/inbox), panics"},
                {"name": "worker",     "usage": "worker <id>",               "description": "Single worker detail with tick-phase timing breakdown"},
                {"name": "actors",     "usage": "actors [--sort mailbox|worker|address] [--limit N] [--worker W]", "description": "List actors with optional sorting, limit, and worker filter"},
                {"name": "actor",      "usage": "actor <hex_prefix>",        "description": "Find actor(s) whose address starts with the given hex prefix"},
                {"name": "hot",        "usage": "hot [N]",                   "description": "Top N actors by mailbox depth (default 10)"},
                {"name": "phases",     "usage": "phases [worker_id]",        "description": "Tick-phase time breakdown (all workers or one)"},
                {"name": "diff",       "usage": "diff <seconds>",            "description": "Collect two snapshots N seconds apart, report deltas and rates"},
                {"name": "quit",       "usage": "quit",                      "description": "Exit the investigate session"},
            ]
        }),
    )
}

pub fn cmd_overview(rt: &Runtime, col: &StatsCollector) -> String {
    let stats = enriched_stats(rt, col);
    let total_msgs: u64 = stats.workers.iter().map(|w| w.messages_processed).sum();
    let total_mailbox: usize = stats.workers.iter().map(|w| w.mailbox_depth).sum();
    let total_panics: u64 = stats.workers.iter().map(|w| w.panics).sum();
    let total_type_mismatches: u64 = stats.workers.iter().map(|w| w.type_mismatches).sum();
    let total_local: u64 = stats.workers.iter().map(|w| w.local_sends).sum();
    let total_cross: u64 = stats.workers.iter().map(|w| w.cross_sends).sum();
    let total_inbox: u64 = stats.workers.iter().map(|w| w.inbox_sends).sum();

    ok_response(
        "overview",
        serde_json::json!({
            "workers": stats.num_workers,
            "actors": stats.actor_details.len(),
            "total_messages_processed": total_msgs,
            "total_mailbox_depth": total_mailbox,
            "total_panics": total_panics,
            "total_type_mismatches": total_type_mismatches,
            "sends": {
                "local": total_local,
                "cross_worker": total_cross,
                "inbox": total_inbox,
            },
        }),
    )
}

pub fn cmd_workers(rt: &Runtime) -> String {
    let stats = rt.stats();
    let workers: Vec<_> = stats
        .workers
        .iter()
        .map(|w| {
            serde_json::json!({
                "id": w.id,
                "actors": w.num_actors,
                "mailbox_depth": w.mailbox_depth,
                "messages_processed": w.messages_processed,
                "local_sends": w.local_sends,
                "cross_sends": w.cross_sends,
                "inbox_sends": w.inbox_sends,
                "type_mismatches": w.type_mismatches,
                "panics": w.panics,
            })
        })
        .collect();
    ok_response("workers", workers)
}

pub fn cmd_worker(rt: &Runtime, col: &StatsCollector, args: &[&str]) -> String {
    let id: usize = match args.first().and_then(|s| s.parse().ok()) {
        Some(id) => id,
        None => return err_response("worker", "usage: worker <id>"),
    };

    let stats = enriched_stats(rt, col);
    let w = match stats.workers.iter().find(|w| w.id == id) {
        Some(w) => w,
        None => {
            return err_response(
                "worker",
                &format!("worker {id} not found (have 0..{})", stats.num_workers),
            )
        }
    };

    // Tick phase breakdown for this worker
    let timings = stats.tick_timings.get(id).cloned().unwrap_or_default();
    let phase_breakdown = compute_phase_breakdown(&timings);

    let actors_on_worker: Vec<_> = stats
        .actor_details
        .iter()
        .filter(|a| a.worker_id == id)
        .map(|a| {
            serde_json::json!({
                "address": format_addr(&a.address),
                "mailbox_depth": a.mailbox_depth,
                "last_msg_type": a.last_msg_type,
                "messages_processed": a.messages_processed,
                "poisoned": a.poisoned,
            })
        })
        .collect();

    ok_response(
        "worker",
        serde_json::json!({
            "id": w.id,
            "actors": w.num_actors,
            "mailbox_depth": w.mailbox_depth,
            "messages_processed": w.messages_processed,
            "local_sends": w.local_sends,
            "cross_sends": w.cross_sends,
            "inbox_sends": w.inbox_sends,
            "type_mismatches": w.type_mismatches,
            "panics": w.panics,
            "tick_phases": phase_breakdown,
            "actor_details": actors_on_worker,
        }),
    )
}

pub fn cmd_actors(rt: &Runtime, col: &StatsCollector, args: &[&str]) -> String {
    let stats = enriched_stats(rt, col);
    let mut actors = stats.actor_details.clone();

    // Parse flags
    let mut sort_by = "mailbox";
    let mut limit: usize = usize::MAX;
    let mut worker_filter: Option<usize> = None;
    let mut i = 0;
    while i < args.len() {
        match args[i] {
            "--sort" if i + 1 < args.len() => {
                sort_by = args[i + 1];
                i += 2;
            }
            "--limit" if i + 1 < args.len() => {
                limit = args[i + 1].parse().unwrap_or(usize::MAX);
                i += 2;
            }
            "--worker" if i + 1 < args.len() => {
                worker_filter = args[i + 1].parse().ok();
                i += 2;
            }
            _ => {
                i += 1;
            }
        }
    }

    if let Some(wid) = worker_filter {
        actors.retain(|a| a.worker_id == wid);
    }

    match sort_by {
        "mailbox" => actors.sort_by(|a, b| b.mailbox_depth.cmp(&a.mailbox_depth)),
        "worker" => actors.sort_by_key(|a| a.worker_id),
        "address" => actors.sort_by(|a, b| a.address.0.cmp(&b.address.0)),
        other => return err_response("actors", &format!("unknown sort field `{other}` — use mailbox|worker|address")),
    }

    actors.truncate(limit);

    let rows: Vec<_> = actors
        .iter()
        .map(|a| {
            serde_json::json!({
                "address": format_addr(&a.address),
                "address_full": full_hex(&a.address),
                "worker_id": a.worker_id,
                "mailbox_depth": a.mailbox_depth,
                "last_msg_type": a.last_msg_type,
                "messages_processed": a.messages_processed,
                "poisoned": a.poisoned,
            })
        })
        .collect();

    ok_response(
        "actors",
        serde_json::json!({
            "total": stats.actor_details.len(),
            "returned": rows.len(),
            "sort": sort_by,
            "actors": rows,
        }),
    )
}

pub fn cmd_actor(rt: &Runtime, col: &StatsCollector, args: &[&str]) -> String {
    let prefix = match args.first() {
        Some(p) => *p,
        None => return err_response("actor", "usage: actor <hex_prefix>"),
    };

    let stats = enriched_stats(rt, col);
    let matches: Vec<_> = stats
        .actor_details
        .iter()
        .filter(|a| full_hex(&a.address).starts_with(prefix))
        .map(|a| {
            serde_json::json!({
                "address": format_addr(&a.address),
                "address_full": full_hex(&a.address),
                "worker_id": a.worker_id,
                "mailbox_depth": a.mailbox_depth,
                "last_msg_type": a.last_msg_type,
                "messages_processed": a.messages_processed,
                "poisoned": a.poisoned,
            })
        })
        .collect();

    ok_response(
        "actor",
        serde_json::json!({
            "prefix": prefix,
            "matches": matches.len(),
            "actors": matches,
        }),
    )
}

pub fn cmd_hot(rt: &Runtime, col: &StatsCollector, args: &[&str]) -> String {
    let n: usize = args.first().and_then(|s| s.parse().ok()).unwrap_or(10);
    let stats = enriched_stats(rt, col);

    let mut actors = stats.actor_details.clone();
    actors.sort_by(|a, b| b.mailbox_depth.cmp(&a.mailbox_depth));
    actors.truncate(n);

    let rows: Vec<_> = actors
        .iter()
        .map(|a| {
            serde_json::json!({
                "address": format_addr(&a.address),
                "address_full": full_hex(&a.address),
                "worker_id": a.worker_id,
                "mailbox_depth": a.mailbox_depth,
                "last_msg_type": a.last_msg_type,
                "messages_processed": a.messages_processed,
                "poisoned": a.poisoned,
            })
        })
        .collect();

    ok_response("hot", rows)
}

pub fn cmd_phases(rt: &Runtime, args: &[&str]) -> String {
    let stats = rt.stats();

    let worker_filter: Option<usize> = args.first().and_then(|s| s.parse().ok());

    let phase_names = [
        "spawn_drain",
        "transfer_drain",
        "tick_all",
        "spawn_drain_2",
        "pending_local",
        "stats_publish",
    ];

    let mut results = Vec::new();
    for (i, timings) in stats.tick_timings.iter().enumerate() {
        if let Some(wid) = worker_filter {
            if i != wid {
                continue;
            }
        }
        let breakdown = compute_phase_breakdown(timings);
        results.push(serde_json::json!({
            "worker_id": i,
            "ticks_sampled": timings.len(),
            "phases": breakdown,
            "phase_names": phase_names,
        }));
    }

    ok_response("phases", results)
}

pub fn cmd_diff(rt: &Runtime, col: &StatsCollector, args: &[&str]) -> String {
    let secs: f64 = match args.first().and_then(|s| s.parse().ok()) {
        Some(s) if s > 0.0 && s <= 30.0 => s,
        Some(_) => return err_response("diff", "seconds must be between 0 and 30"),
        None => return err_response("diff", "usage: diff <seconds>"),
    };

    let before = enriched_stats(rt, col);
    let t0 = Instant::now();
    std::thread::sleep(Duration::from_secs_f64(secs));
    let after = enriched_stats(rt, col);
    let elapsed = t0.elapsed().as_secs_f64();

    let msgs_before: u64 = before.workers.iter().map(|w| w.messages_processed).sum();
    let msgs_after: u64 = after.workers.iter().map(|w| w.messages_processed).sum();
    let delta_msgs = msgs_after.saturating_sub(msgs_before);

    let local_before: u64 = before.workers.iter().map(|w| w.local_sends).sum();
    let local_after: u64 = after.workers.iter().map(|w| w.local_sends).sum();
    let cross_before: u64 = before.workers.iter().map(|w| w.cross_sends).sum();
    let cross_after: u64 = after.workers.iter().map(|w| w.cross_sends).sum();

    let mailbox_before: usize = before.workers.iter().map(|w| w.mailbox_depth).sum();
    let mailbox_after: usize = after.workers.iter().map(|w| w.mailbox_depth).sum();

    let per_worker: Vec<_> = after
        .workers
        .iter()
        .enumerate()
        .map(|(i, w)| {
            let prev = before.workers.get(i);
            let d = prev
                .map(|p| w.messages_processed.saturating_sub(p.messages_processed))
                .unwrap_or(0);
            serde_json::json!({
                "worker_id": i,
                "delta_messages": d,
                "msg_per_sec": d as f64 / elapsed,
                "actors_before": prev.map(|p| p.num_actors).unwrap_or(0),
                "actors_after": w.num_actors,
                "mailbox_before": prev.map(|p| p.mailbox_depth).unwrap_or(0),
                "mailbox_after": w.mailbox_depth,
            })
        })
        .collect();

    ok_response(
        "diff",
        serde_json::json!({
            "elapsed_s": elapsed,
            "actors_before": before.actor_details.len(),
            "actors_after": after.actor_details.len(),
            "delta_messages": delta_msgs,
            "msg_per_sec": delta_msgs as f64 / elapsed,
            "delta_local_sends": local_after.saturating_sub(local_before),
            "delta_cross_sends": cross_after.saturating_sub(cross_before),
            "mailbox_before": mailbox_before,
            "mailbox_after": mailbox_after,
            "per_worker": per_worker,
        }),
    )
}

// ── Phase breakdown helper ──────────────────────────────────────────────

fn compute_phase_breakdown(
    timings: &[swactor::stats::TickTiming],
) -> serde_json::Value {
    if timings.is_empty() {
        return serde_json::json!({
            "ticks": 0,
            "active_pct": 0.0,
            "avg_tick_us": 0.0,
            "phases_us": [0, 0, 0, 0, 0, 0],
            "phases_pct": [0.0, 0.0, 0.0, 0.0, 0.0, 0.0],
        });
    }

    let n = timings.len();
    let active = timings.iter().filter(|t| t.did_work).count();
    let active_pct = (active as f64 / n as f64) * 100.0;

    let mut phase_sums = [0u64; 6];
    for t in timings {
        for (i, &us) in t.phase_us.iter().enumerate() {
            phase_sums[i] += us;
        }
    }
    let total_us: u64 = phase_sums.iter().sum();
    let avg_tick_us = total_us as f64 / n as f64;

    let phases_pct: Vec<f64> = if total_us == 0 {
        vec![0.0; 6]
    } else {
        phase_sums
            .iter()
            .map(|&s| (s as f64 / total_us as f64) * 100.0)
            .collect()
    };

    serde_json::json!({
        "ticks": n,
        "active_pct": active_pct,
        "avg_tick_us": avg_tick_us,
        "phases_us": phase_sums,
        "phases_pct": phases_pct,
    })
}
