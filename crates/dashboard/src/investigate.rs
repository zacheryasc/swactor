//! Line-oriented diagnostic protocol for LLM-driven runtime investigation.
//!
//! Delegates all command logic to the `command` module.
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

use swactor::runtime::Runtime;
use crate::command::{CommandContext, CommandRouter};

use crate::collector::StatsCollector;

/// Run the investigate REPL. Blocks until stdin is closed or `quit` is received.
pub fn run_investigate(runtime: Arc<Runtime>, collector: Arc<StatsCollector>) -> io::Result<()> {
    let router = CommandRouter::with_builtins();
    let ctx = CommandContext::with_enricher(runtime, collector);

    let stdin = io::stdin();
    let mut stdout = io::stdout();

    eprintln!("swactor investigate protocol ready — send `help` for commands");

    for line in stdin.lock().lines() {
        let line = line?;
        let line = line.trim();
        if line.is_empty() {
            continue;
        }
        if line == "quit" || line == "exit" {
            break;
        }

        let req = crate::command::parse_line(line);
        let resp = router.dispatch(&req, &ctx);

        stdout.write_all(resp.to_json_line().as_bytes())?;
        stdout.write_all(b"\n")?;
        stdout.flush()?;
    }

    Ok(())
}

/// Dispatch an investigate command from HTTP query parameters.
///
/// Maps `?cmd=overview`, `?cmd=hot&n=10`, etc. to the appropriate command.
pub fn dispatch_command(
    params: &HashMap<String, String>,
    router: &CommandRouter,
    ctx: &CommandContext,
) -> String {
    let req = crate::command::from_query_params(params);
    router.dispatch(&req, ctx).to_json_line()
}
