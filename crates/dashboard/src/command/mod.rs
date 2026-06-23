//! Frontend-agnostic command dispatch for swactor runtimes.
//!
//! Provides [`CommandRouter`] that maps command names to [`CommandHandler`]
//! implementations, with built-in commands for runtime inspection and management.
//!
//! # Architecture
//!
//! ```text
//! Frontend (REPL, REST, TUI, WebSocket)
//!     │
//!     ▼
//! CommandRouter::dispatch(CommandRequest, CommandContext)
//!     │
//!     ├── built-in handlers (overview, workers, actors, …)
//!     └── custom handlers (user-registered)
//! ```

pub mod builtins;

use std::collections::HashMap;
use std::sync::Arc;

use serde::{Deserialize, Serialize};
use swactor::runtime::Runtime;
use swactor::stats::RuntimeStats;

// ─── Core Types ──────────────────────────────────────────────────────────────

/// A command request.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CommandRequest {
    pub command: String,
    pub args: HashMap<String, serde_json::Value>,
}

/// A command response. Always JSON-serializable.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CommandResponse {
    pub ok: bool,
    pub command: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub data: Option<serde_json::Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
}

impl CommandResponse {
    pub fn ok(command: &str, data: impl Serialize) -> Self {
        Self {
            ok: true,
            command: command.to_string(),
            data: Some(serde_json::to_value(data).unwrap_or(serde_json::Value::Null)),
            error: None,
        }
    }

    pub fn err(command: &str, msg: impl Into<String>) -> Self {
        Self {
            ok: false,
            command: command.to_string(),
            data: None,
            error: Some(msg.into()),
        }
    }

    /// Serialize to a single JSON line (for REPL/wire protocol).
    pub fn to_json_line(&self) -> String {
        serde_json::to_string(self).unwrap_or_else(|e| {
            format!(r#"{{"ok":false,"command":"","error":"serialization: {e}"}}"#)
        })
    }
}

// ─── Handler Trait ───────────────────────────────────────────────────────────

/// Metadata about a command, used for help text and validation.
pub struct CommandMeta {
    pub name: &'static str,
    pub description: &'static str,
    pub usage: &'static str,
    pub is_write: bool,
}

/// A command handler. Implementations are stateless — all state
/// comes through [`CommandContext`].
pub trait CommandHandler: Send + Sync {
    fn meta(&self) -> CommandMeta;
    fn handle(
        &self,
        args: &HashMap<String, serde_json::Value>,
        ctx: &CommandContext,
    ) -> CommandResponse;
}

// ─── Stats Enrichment ────────────────────────────────────────────────────────

/// Enriches [`RuntimeStats`] with per-actor detail.
///
/// Implement this on your stats collector so command handlers can access
/// enriched data without depending on the dashboard crate.
pub trait StatsEnricher: Send + Sync {
    fn enrich(&self, stats: &mut RuntimeStats);
}

// ─── Context ─────────────────────────────────────────────────────────────────

/// Context available to command handlers.
pub struct CommandContext {
    pub runtime: Arc<Runtime>,
    pub enricher: Option<Arc<dyn StatsEnricher>>,
}

impl CommandContext {
    pub fn new(runtime: Arc<Runtime>) -> Self {
        Self {
            runtime,
            enricher: None,
        }
    }

    pub fn with_enricher(runtime: Arc<Runtime>, enricher: Arc<dyn StatsEnricher>) -> Self {
        Self {
            runtime,
            enricher: Some(enricher),
        }
    }

    /// Raw runtime stats (no enrichment).
    pub fn stats(&self) -> RuntimeStats {
        self.runtime.stats()
    }

    /// Runtime stats enriched with per-actor detail (if an enricher is set).
    pub fn enriched_stats(&self) -> RuntimeStats {
        let mut s = self.runtime.stats();
        if let Some(e) = &self.enricher {
            e.enrich(&mut s);
        }
        s
    }
}

// ─── Router ──────────────────────────────────────────────────────────────────

/// Central command dispatch.
pub struct CommandRouter {
    handlers: HashMap<String, Box<dyn CommandHandler>>,
}

impl Default for CommandRouter {
    fn default() -> Self {
        Self::new()
    }
}

impl CommandRouter {
    pub fn new() -> Self {
        Self {
            handlers: HashMap::new(),
        }
    }

    /// Create a router with all built-in commands registered.
    pub fn with_builtins() -> Self {
        let mut router = Self::new();
        router.register(Box::new(builtins::OverviewCommand));
        router.register(Box::new(builtins::WorkersCommand));
        router.register(Box::new(builtins::WorkerCommand));
        router.register(Box::new(builtins::ActorsCommand));
        router.register(Box::new(builtins::ActorCommand));
        router.register(Box::new(builtins::HotCommand));
        router.register(Box::new(builtins::PhasesCommand));
        router.register(Box::new(builtins::DiffCommand));
        router.register(Box::new(builtins::ShutdownCommand));
        router
    }

    /// Register a custom command handler.
    pub fn register(&mut self, handler: Box<dyn CommandHandler>) {
        let name = handler.meta().name.to_string();
        self.handlers.insert(name, handler);
    }

    /// Dispatch a command request.
    ///
    /// The `help` command is handled directly by the router (it needs
    /// access to all registered handlers).
    pub fn dispatch(&self, req: &CommandRequest, ctx: &CommandContext) -> CommandResponse {
        if req.command == "help" {
            return self.cmd_help();
        }
        match self.handlers.get(&req.command) {
            Some(handler) => handler.handle(&req.args, ctx),
            None => CommandResponse::err(
                &req.command,
                format!("unknown command `{}` — try `help`", req.command),
            ),
        }
    }

    fn cmd_help(&self) -> CommandResponse {
        let mut commands: Vec<serde_json::Value> = self
            .handlers
            .values()
            .map(|h| {
                let m = h.meta();
                serde_json::json!({
                    "name": m.name,
                    "usage": m.usage,
                    "description": m.description,
                    "is_write": m.is_write,
                })
            })
            .collect();
        // Add help itself
        commands.push(serde_json::json!({
            "name": "help",
            "usage": "help",
            "description": "List all available commands",
            "is_write": false,
        }));
        commands.sort_by(|a, b| {
            a["name"]
                .as_str()
                .unwrap_or("")
                .cmp(b["name"].as_str().unwrap_or(""))
        });
        CommandResponse::ok("help", serde_json::json!({ "commands": commands }))
    }

    /// List names of all registered commands (sorted).
    pub fn command_names(&self) -> Vec<&str> {
        let mut names: Vec<_> = self.handlers.keys().map(|s| s.as_str()).collect();
        names.push("help");
        names.sort();
        names
    }
}

// ─── Input Parsers ──────────────────────────────────────────────────────────

/// Parse a REPL text line into a [`CommandRequest`].
///
/// Handles `--flag value` pairs and maps positional arguments to
/// command-specific named parameters.
pub fn parse_line(line: &str) -> CommandRequest {
    let parts: Vec<&str> = line.split_whitespace().collect();
    if parts.is_empty() {
        return CommandRequest {
            command: "help".to_string(),
            args: HashMap::new(),
        };
    }
    let command = parts[0].to_string();
    let rest = &parts[1..];

    let mut args = HashMap::new();
    let mut i = 0;
    let mut positional = 0;

    while i < rest.len() {
        if let Some(key) = rest[i].strip_prefix("--") {
            if i + 1 < rest.len() && !rest[i + 1].starts_with("--") {
                args.insert(
                    key.to_string(),
                    serde_json::Value::String(rest[i + 1].to_string()),
                );
                i += 2;
            } else {
                args.insert(key.to_string(), serde_json::Value::Bool(true));
                i += 1;
            }
        } else {
            let name = positional_arg_name(&command, positional);
            if !name.is_empty() {
                args.insert(
                    name.to_string(),
                    serde_json::Value::String(rest[i].to_string()),
                );
            }
            positional += 1;
            i += 1;
        }
    }

    CommandRequest { command, args }
}

/// Convert HTTP query parameters to a [`CommandRequest`].
pub fn from_query_params(params: &HashMap<String, String>) -> CommandRequest {
    let command = params.get("cmd").cloned().unwrap_or_else(|| "help".into());
    let args: HashMap<String, serde_json::Value> = params
        .iter()
        .filter(|(k, _)| *k != "cmd")
        .map(|(k, v)| (k.clone(), serde_json::Value::String(v.clone())))
        .collect();
    CommandRequest { command, args }
}

/// Map positional argument index to the named parameter for each command.
fn positional_arg_name(command: &str, position: usize) -> &'static str {
    match (command, position) {
        ("worker", 0) => "id",
        ("actor", 0) => "prefix",
        ("hot", 0) => "n",
        ("phases", 0) => "worker",
        ("diff", 0) => "seconds",
        _ => "",
    }
}
