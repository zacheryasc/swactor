//! Behavioral tests for the swactor-command crate.
//!
//! Tests exercise the full dispatch path: parse → route → handle → response.

use std::collections::HashMap;
use std::sync::Arc;

use swactor::actor::ActorInterface;
use swactor::runtime::{Ctx, Runtime, RuntimeConfig};
use runtime_dashboard::command::{
    from_query_params, parse_line, CommandContext, CommandRequest, CommandResponse, CommandRouter,
};

// ── Test Helpers ─────────────────────────────────────────────────────────────

fn single_thread_config() -> RuntimeConfig {
    RuntimeConfig {
        num_threads: 1,
        ..RuntimeConfig::default()
    }
}

fn make_router_and_ctx() -> (CommandRouter, CommandContext) {
    let rt = Arc::new(Runtime::new(single_thread_config()));
    let router = CommandRouter::with_builtins();
    let ctx = CommandContext::new(rt);
    (router, ctx)
}

fn dispatch_text(router: &CommandRouter, ctx: &CommandContext, line: &str) -> CommandResponse {
    let req = parse_line(line);
    let resp = router.dispatch(&req, ctx);
    // Verify JSON round-trip works
    let json = resp.to_json_line();
    serde_json::from_str::<CommandResponse>(&json)
        .expect("response should be valid JSON")
}

/// A no-op actor for spawning into the runtime.
struct DummyActor;
#[derive(Clone)]
struct DummyMsg;
impl ActorInterface for DummyActor {
    type Incoming = DummyMsg;
    type Response = ();
    fn handle(&mut self, _ctx: &Ctx, _msg: DummyMsg) {}
}

// ── Tests ────────────────────────────────────────────────────────────────────

/// Given a router with builtins,
/// when "help" is dispatched,
/// then the response lists all registered commands.
#[test]
fn help_lists_all_registered_commands() {
    let (router, ctx) = make_router_and_ctx();
    let resp = dispatch_text(&router, &ctx, "help");

    assert!(resp.ok, "help should succeed");
    assert_eq!(resp.command, "help");

    let data = resp.data.unwrap();
    let commands = data["commands"].as_array().unwrap();

    // Should have all builtins + help itself
    let names: Vec<&str> = commands
        .iter()
        .map(|c| c["name"].as_str().unwrap())
        .collect();
    assert!(names.contains(&"overview"), "should list overview");
    assert!(names.contains(&"workers"), "should list workers");
    assert!(names.contains(&"worker"), "should list worker");
    assert!(names.contains(&"actors"), "should list actors");
    assert!(names.contains(&"hot"), "should list hot");
    assert!(names.contains(&"phases"), "should list phases");
    assert!(names.contains(&"diff"), "should list diff");
    assert!(names.contains(&"shutdown"), "should list shutdown");
    assert!(names.contains(&"help"), "should list help itself");

    // Should be sorted
    let mut sorted = names.clone();
    sorted.sort();
    assert_eq!(names, sorted, "commands should be sorted alphabetically");
}

/// Given a router,
/// when an unknown command is dispatched,
/// then the response indicates failure with a helpful message.
#[test]
fn unknown_command_returns_error() {
    let (router, ctx) = make_router_and_ctx();
    let resp = dispatch_text(&router, &ctx, "nonexistent");

    assert!(!resp.ok, "unknown command should fail");
    assert_eq!(resp.command, "nonexistent");
    let err = resp.error.unwrap();
    assert!(
        err.contains("unknown command") && err.contains("help"),
        "error should mention 'unknown command' and suggest 'help', got: {err}"
    );
}

/// Given a runtime with no actors,
/// when "overview" is dispatched,
/// then the response contains expected summary fields with zero counts.
#[test]
fn overview_returns_summary_fields() {
    let (router, ctx) = make_router_and_ctx();
    let resp = dispatch_text(&router, &ctx, "overview");

    assert!(resp.ok);
    assert_eq!(resp.command, "overview");

    let data = resp.data.unwrap();
    assert_eq!(data["workers"], 1, "single-threaded = 1 worker");
    assert_eq!(data["actors"], 0, "no actors spawned");
    assert_eq!(data["total_messages_processed"], 0);
    assert_eq!(data["total_panics"], 0);
    assert!(data["sends"].is_object(), "sends should be an object");
}

/// Given a runtime with spawned actors,
/// when "workers" is dispatched,
/// then the response contains per-worker stats.
#[test]
fn workers_returns_per_worker_info() {
    let rt = Arc::new(Runtime::new(single_thread_config()));
    // Spawn some actors
    rt.spawn(DummyActor).unwrap();
    rt.spawn(DummyActor).unwrap();
    rt.tick();

    let router = CommandRouter::with_builtins();
    let ctx = CommandContext::new(rt);
    let resp = dispatch_text(&router, &ctx, "workers");

    assert!(resp.ok);
    let data = resp.data.unwrap();
    let workers = data.as_array().unwrap();
    assert_eq!(workers.len(), 1, "single-threaded has 1 worker");
    assert_eq!(workers[0]["id"], 0);
    assert_eq!(workers[0]["actors"], 2, "2 actors spawned on worker 0");
}

/// Given "worker 0" with a valid ID,
/// when dispatched,
/// then the response includes worker detail and tick phase info.
#[test]
fn worker_command_with_valid_id() {
    let (router, ctx) = make_router_and_ctx();
    let resp = dispatch_text(&router, &ctx, "worker 0");

    assert!(resp.ok);
    assert_eq!(resp.command, "worker");
    let data = resp.data.unwrap();
    assert_eq!(data["id"], 0);
    assert!(data["tick_phases"].is_object(), "should include phase breakdown");
}

/// Given "worker 99",
/// when dispatched,
/// then the response is an error (worker not found).
#[test]
fn worker_command_invalid_id_returns_error() {
    let (router, ctx) = make_router_and_ctx();
    let resp = dispatch_text(&router, &ctx, "worker 99");

    assert!(!resp.ok);
    assert!(resp.error.unwrap().contains("not found"));
}

/// Given "worker" with no ID,
/// when dispatched,
/// then the response is a usage error.
#[test]
fn worker_command_missing_id_returns_usage() {
    let (router, ctx) = make_router_and_ctx();
    let resp = dispatch_text(&router, &ctx, "worker");

    assert!(!resp.ok);
    assert!(resp.error.unwrap().contains("usage"));
}

/// Given "phases",
/// when dispatched,
/// then the response includes phase breakdown per worker.
#[test]
fn phases_command_returns_breakdown() {
    let (router, ctx) = make_router_and_ctx();
    let resp = dispatch_text(&router, &ctx, "phases");

    assert!(resp.ok);
    let data = resp.data.unwrap();
    let phases = data.as_array().unwrap();
    assert_eq!(phases.len(), 1, "single-threaded = 1 worker");
    assert_eq!(phases[0]["worker_id"], 0);
}

/// Given a runtime, when "shutdown" is dispatched,
/// then the response indicates success.
#[test]
fn shutdown_command_signals_runtime() {
    let (router, ctx) = make_router_and_ctx();
    let resp = dispatch_text(&router, &ctx, "shutdown");

    assert!(resp.ok);
    assert_eq!(resp.command, "shutdown");
    let data = resp.data.unwrap();
    assert_eq!(data["status"], "shutdown signaled");
}

// ── REPL Parser Tests ────────────────────────────────────────────────────────

/// Given a simple command with no args,
/// when parsed,
/// then the command name is extracted correctly.
#[test]
fn parse_line_simple_command() {
    let req = parse_line("overview");
    assert_eq!(req.command, "overview");
    assert!(req.args.is_empty());
}

/// Given a command with positional args,
/// when parsed,
/// then positional args are mapped to named parameters.
#[test]
fn parse_line_positional_args() {
    let req = parse_line("worker 3");
    assert_eq!(req.command, "worker");
    assert_eq!(req.args["id"], "3");

    let req = parse_line("hot 5");
    assert_eq!(req.command, "hot");
    assert_eq!(req.args["n"], "5");

    let req = parse_line("diff 2.5");
    assert_eq!(req.command, "diff");
    assert_eq!(req.args["seconds"], "2.5");

    let req = parse_line("actor a1b2");
    assert_eq!(req.command, "actor");
    assert_eq!(req.args["prefix"], "a1b2");
}

/// Given a command with --flag value pairs,
/// when parsed,
/// then flags are mapped to named args.
#[test]
fn parse_line_flags() {
    let req = parse_line("actors --sort mailbox --limit 5");
    assert_eq!(req.command, "actors");
    assert_eq!(req.args["sort"], "mailbox");
    assert_eq!(req.args["limit"], "5");
}

/// Given a command with mixed positional and flag args,
/// when parsed,
/// then both are captured correctly.
#[test]
fn parse_line_mixed_args() {
    let req = parse_line("actors --sort worker --worker 2 --limit 10");
    assert_eq!(req.command, "actors");
    assert_eq!(req.args["sort"], "worker");
    assert_eq!(req.args["worker"], "2");
    assert_eq!(req.args["limit"], "10");
}

/// Given empty input,
/// when parsed,
/// then default to "help".
#[test]
fn parse_line_empty_defaults_to_help() {
    let req = parse_line("");
    assert_eq!(req.command, "help");
}

// ── REST Adapter Tests ───────────────────────────────────────────────────────

/// Given query params with cmd and other params,
/// when converted,
/// then cmd becomes the command and others become args.
#[test]
fn from_query_params_extracts_cmd() {
    let mut params = HashMap::new();
    params.insert("cmd".to_string(), "actor".to_string());
    params.insert("prefix".to_string(), "a1b2".to_string());

    let req = from_query_params(&params);
    assert_eq!(req.command, "actor");
    assert_eq!(req.args["prefix"], "a1b2");
    assert!(!req.args.contains_key("cmd"), "cmd should not be in args");
}

/// Given query params with no cmd,
/// when converted,
/// then default to "help".
#[test]
fn from_query_params_defaults_to_help() {
    let params = HashMap::new();
    let req = from_query_params(&params);
    assert_eq!(req.command, "help");
}

// ── Custom Handler Test ──────────────────────────────────────────────────────

/// Given a custom command handler registered on the router,
/// when that command is dispatched,
/// then the custom handler runs and returns its response.
#[test]
fn custom_command_handler() {
    struct PingCommand;
    impl runtime_dashboard::command::CommandHandler for PingCommand {
        fn meta(&self) -> runtime_dashboard::command::CommandMeta {
            runtime_dashboard::command::CommandMeta {
                name: "ping",
                description: "Respond with pong",
                usage: "ping",
                is_write: false,
            }
        }
        fn handle(
            &self,
            _args: &HashMap<String, serde_json::Value>,
            _ctx: &CommandContext,
        ) -> CommandResponse {
            CommandResponse::ok("ping", serde_json::json!({"reply": "pong"}))
        }
    }

    let rt = Arc::new(Runtime::new(single_thread_config()));
    let mut router = CommandRouter::with_builtins();
    router.register(Box::new(PingCommand));
    let ctx = CommandContext::new(rt);

    let resp = dispatch_text(&router, &ctx, "ping");
    assert!(resp.ok);
    assert_eq!(resp.data.unwrap()["reply"], "pong");

    // Should also appear in help
    let help = dispatch_text(&router, &ctx, "help");
    let commands = help.data.unwrap()["commands"].as_array().unwrap().clone();
    let names: Vec<&str> = commands.iter().map(|c| c["name"].as_str().unwrap()).collect();
    assert!(names.contains(&"ping"), "custom command should appear in help");
}

/// Given a JSON-serialized CommandResponse,
/// when deserialized,
/// then ok/false fields, data, and error are preserved.
#[test]
fn response_json_roundtrip() {
    let ok_resp = CommandResponse::ok("test", serde_json::json!({"key": "value"}));
    let json = ok_resp.to_json_line();
    let parsed: CommandResponse = serde_json::from_str(&json).unwrap();
    assert!(parsed.ok);
    assert_eq!(parsed.command, "test");
    assert_eq!(parsed.data.unwrap()["key"], "value");
    assert!(parsed.error.is_none());

    let err_resp = CommandResponse::err("bad", "something went wrong");
    let json = err_resp.to_json_line();
    let parsed: CommandResponse = serde_json::from_str(&json).unwrap();
    assert!(!parsed.ok);
    assert_eq!(parsed.command, "bad");
    assert!(parsed.data.is_none());
    assert_eq!(parsed.error.unwrap(), "something went wrong");
}
