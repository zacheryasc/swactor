//! Built-in command handlers for runtime inspection and management.
//!
//! Extracted from `crates/dashboard/src/investigate.rs`.

use std::collections::HashMap;
use std::time::{Duration, Instant};

use swactor::actor::ActorAddress;
use swactor::stats::TickTiming;

use super::{CommandContext, CommandHandler, CommandMeta, CommandResponse};

// ─── Arg helpers ─────────────────────────────────────────────────────────────

fn arg_str<'a>(args: &'a HashMap<String, serde_json::Value>, key: &str) -> Option<&'a str> {
    args.get(key).and_then(|v| v.as_str())
}

fn arg_usize(args: &HashMap<String, serde_json::Value>, key: &str) -> Option<usize> {
    args.get(key).and_then(|v| {
        v.as_u64()
            .map(|n| n as usize)
            .or_else(|| v.as_str().and_then(|s| s.parse().ok()))
    })
}

fn arg_f64(args: &HashMap<String, serde_json::Value>, key: &str) -> Option<f64> {
    args.get(key).and_then(|v| {
        v.as_f64()
            .or_else(|| v.as_str().and_then(|s| s.parse().ok()))
    })
}

// ─── Display helpers ─────────────────────────────────────────────────────────

fn format_addr(addr: &ActorAddress) -> String {
    format!("{addr}")
}

fn full_hex(addr: &ActorAddress) -> String {
    addr.0.iter().map(|b| format!("{b:02x}")).collect()
}

// ─── Phase breakdown helper ──────────────────────────────────────────────────

fn compute_phase_breakdown(timings: &[TickTiming]) -> serde_json::Value {
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

// ─── Read Commands ───────────────────────────────────────────────────────────

pub struct OverviewCommand;

impl CommandHandler for OverviewCommand {
    fn meta(&self) -> CommandMeta {
        CommandMeta {
            name: "overview",
            description: "Summary: worker count, actor count, total messages, mailbox depth, panics",
            usage: "overview",
            is_write: false,
        }
    }

    fn handle(
        &self,
        _args: &HashMap<String, serde_json::Value>,
        ctx: &CommandContext,
    ) -> CommandResponse {
        let stats = ctx.enriched_stats();
        let total_msgs: u64 = stats.workers.iter().map(|w| w.messages_processed).sum();
        let total_mailbox: usize = stats.workers.iter().map(|w| w.mailbox_depth).sum();
        let total_panics: u64 = stats.workers.iter().map(|w| w.panics).sum();
        let total_type_mismatches: u64 = stats.workers.iter().map(|w| w.type_mismatches).sum();
        let total_local: u64 = stats.workers.iter().map(|w| w.local_sends).sum();
        let total_cross: u64 = stats.workers.iter().map(|w| w.cross_sends).sum();
        let total_inbox: u64 = stats.workers.iter().map(|w| w.inbox_sends).sum();

        CommandResponse::ok(
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
}

pub struct WorkersCommand;

impl CommandHandler for WorkersCommand {
    fn meta(&self) -> CommandMeta {
        CommandMeta {
            name: "workers",
            description: "Per-worker stats: actors, mailbox depth, messages, sends, panics",
            usage: "workers",
            is_write: false,
        }
    }

    fn handle(
        &self,
        _args: &HashMap<String, serde_json::Value>,
        ctx: &CommandContext,
    ) -> CommandResponse {
        let stats = ctx.stats();
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
        CommandResponse::ok("workers", workers)
    }
}

pub struct WorkerCommand;

impl CommandHandler for WorkerCommand {
    fn meta(&self) -> CommandMeta {
        CommandMeta {
            name: "worker",
            description: "Single worker detail with tick-phase timing breakdown",
            usage: "worker <id>",
            is_write: false,
        }
    }

    fn handle(
        &self,
        args: &HashMap<String, serde_json::Value>,
        ctx: &CommandContext,
    ) -> CommandResponse {
        let id = match arg_usize(args, "id") {
            Some(id) => id,
            None => return CommandResponse::err("worker", "usage: worker <id>"),
        };

        let stats = ctx.enriched_stats();
        let w = match stats.workers.iter().find(|w| w.id == id) {
            Some(w) => w,
            None => {
                return CommandResponse::err(
                    "worker",
                    format!("worker {id} not found (have 0..{})", stats.num_workers),
                );
            }
        };

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

        CommandResponse::ok(
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
}

pub struct ActorsCommand;

impl CommandHandler for ActorsCommand {
    fn meta(&self) -> CommandMeta {
        CommandMeta {
            name: "actors",
            description: "List actors with optional sorting, limit, and worker filter",
            usage: "actors [--sort mailbox|worker|address] [--limit N] [--worker W]",
            is_write: false,
        }
    }

    fn handle(
        &self,
        args: &HashMap<String, serde_json::Value>,
        ctx: &CommandContext,
    ) -> CommandResponse {
        let stats = ctx.enriched_stats();
        let mut actors = stats.actor_details.clone();

        let sort_by = arg_str(args, "sort").unwrap_or("mailbox");
        let limit = arg_usize(args, "limit").unwrap_or(usize::MAX);
        let worker_filter = arg_usize(args, "worker");

        if let Some(wid) = worker_filter {
            actors.retain(|a| a.worker_id == wid);
        }

        match sort_by {
            "mailbox" => actors.sort_by(|a, b| b.mailbox_depth.cmp(&a.mailbox_depth)),
            "worker" => actors.sort_by_key(|a| a.worker_id),
            "address" => actors.sort_by(|a, b| a.address.0.cmp(&b.address.0)),
            other => {
                return CommandResponse::err(
                    "actors",
                    format!("unknown sort field `{other}` — use mailbox|worker|address"),
                );
            }
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

        CommandResponse::ok(
            "actors",
            serde_json::json!({
                "total": stats.actor_details.len(),
                "returned": rows.len(),
                "sort": sort_by,
                "actors": rows,
            }),
        )
    }
}

pub struct ActorCommand;

impl CommandHandler for ActorCommand {
    fn meta(&self) -> CommandMeta {
        CommandMeta {
            name: "actor",
            description: "Find actor(s) whose address starts with the given hex prefix",
            usage: "actor <hex_prefix>",
            is_write: false,
        }
    }

    fn handle(
        &self,
        args: &HashMap<String, serde_json::Value>,
        ctx: &CommandContext,
    ) -> CommandResponse {
        let prefix = match arg_str(args, "prefix") {
            Some(p) => p,
            None => return CommandResponse::err("actor", "usage: actor <hex_prefix>"),
        };

        let stats = ctx.enriched_stats();
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

        CommandResponse::ok(
            "actor",
            serde_json::json!({
                "prefix": prefix,
                "matches": matches.len(),
                "actors": matches,
            }),
        )
    }
}

pub struct HotCommand;

impl CommandHandler for HotCommand {
    fn meta(&self) -> CommandMeta {
        CommandMeta {
            name: "hot",
            description: "Top N actors by mailbox depth (default 10)",
            usage: "hot [N]",
            is_write: false,
        }
    }

    fn handle(
        &self,
        args: &HashMap<String, serde_json::Value>,
        ctx: &CommandContext,
    ) -> CommandResponse {
        let n = arg_usize(args, "n").unwrap_or(10);
        let stats = ctx.enriched_stats();

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

        CommandResponse::ok("hot", rows)
    }
}

pub struct PhasesCommand;

impl CommandHandler for PhasesCommand {
    fn meta(&self) -> CommandMeta {
        CommandMeta {
            name: "phases",
            description: "Tick-phase time breakdown (all workers or one)",
            usage: "phases [worker_id]",
            is_write: false,
        }
    }

    fn handle(
        &self,
        args: &HashMap<String, serde_json::Value>,
        ctx: &CommandContext,
    ) -> CommandResponse {
        let stats = ctx.stats();
        let worker_filter = arg_usize(args, "worker");

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
            if let Some(wid) = worker_filter
                && i != wid
            {
                continue;
            }
            let breakdown = compute_phase_breakdown(timings);
            results.push(serde_json::json!({
                "worker_id": i,
                "ticks_sampled": timings.len(),
                "phases": breakdown,
                "phase_names": phase_names,
            }));
        }

        CommandResponse::ok("phases", results)
    }
}

pub struct DiffCommand;

impl CommandHandler for DiffCommand {
    fn meta(&self) -> CommandMeta {
        CommandMeta {
            name: "diff",
            description: "Collect two snapshots N seconds apart, report deltas and rates",
            usage: "diff <seconds>",
            is_write: false,
        }
    }

    fn handle(
        &self,
        args: &HashMap<String, serde_json::Value>,
        ctx: &CommandContext,
    ) -> CommandResponse {
        let secs = match arg_f64(args, "seconds") {
            Some(s) if s > 0.0 && s <= 30.0 => s,
            Some(_) => return CommandResponse::err("diff", "seconds must be between 0 and 30"),
            None => return CommandResponse::err("diff", "usage: diff <seconds>"),
        };

        let before = ctx.enriched_stats();
        let t0 = Instant::now();
        std::thread::sleep(Duration::from_secs_f64(secs));
        let after = ctx.enriched_stats();
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

        CommandResponse::ok(
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
}

// ─── Write Commands ──────────────────────────────────────────────────────────

pub struct ShutdownCommand;

impl CommandHandler for ShutdownCommand {
    fn meta(&self) -> CommandMeta {
        CommandMeta {
            name: "shutdown",
            description: "Signal the runtime to shut down gracefully",
            usage: "shutdown",
            is_write: true,
        }
    }

    fn handle(
        &self,
        _args: &HashMap<String, serde_json::Value>,
        ctx: &CommandContext,
    ) -> CommandResponse {
        ctx.runtime.shutdown();
        CommandResponse::ok(
            "shutdown",
            serde_json::json!({"status": "shutdown signaled"}),
        )
    }
}
