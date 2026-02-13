//! Input parsers for REPL lines and HTTP query parameters.

use std::collections::HashMap;

use crate::CommandRequest;

/// Parse a REPL text line into a [`CommandRequest`].
///
/// Handles `--flag value` pairs and maps positional arguments to
/// command-specific named parameters.
///
/// # Examples
///
/// ```text
/// "overview"              → { command: "overview", args: {} }
/// "worker 3"              → { command: "worker", args: { "id": "3" } }
/// "actors --sort mailbox" → { command: "actors", args: { "sort": "mailbox" } }
/// "hot 5"                 → { command: "hot", args: { "n": "5" } }
/// ```
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
///
/// The `cmd` parameter becomes the command name; all other parameters
/// become string-valued arguments.
pub fn from_query_params(params: &HashMap<String, String>) -> CommandRequest {
    let command = params
        .get("cmd")
        .cloned()
        .unwrap_or_else(|| "help".into());
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
