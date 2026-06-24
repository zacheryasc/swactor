use std::io::{self, BufRead, Write};
use std::process::ExitCode;

use serde_json::json;

fn main() -> ExitCode {
    let stdin = io::stdin();
    let mut stdout = io::stdout();

    for line in stdin.lock().lines() {
        let line = match line {
            Ok(line) => line,
            Err(error) => {
                let _ = writeln!(
                    stdout,
                    "{}",
                    json!({"type":"WorkerFatal","reason":format!("stdin:{error}")})
                );
                return ExitCode::from(1);
            }
        };
        if line.trim().is_empty() {
            continue;
        }
        let value: serde_json::Value = match serde_json::from_str(&line) {
            Ok(value) => value,
            Err(error) => {
                let _ = writeln!(
                    stdout,
                    "{}",
                    json!({"type":"ProcessFault","reason":format!("invalid-json:{error}")})
                );
                continue;
            }
        };
        let Some(kind) = value.get("type").and_then(|value| value.as_str()) else {
            let _ = writeln!(
                stdout,
                "{}",
                json!({"type":"ProcessFault","reason":"missing-type"})
            );
            continue;
        };
        match kind {
            "InitializeWorker" => {
                let _ = writeln!(stdout, "{}", json!({"type":"WorkerReady","generation":1}));
            }
            "InstallRing" => {
                let ring_id = value
                    .get("ring_id")
                    .and_then(|value| value.as_u64())
                    .unwrap_or(0);
                let _ = writeln!(
                    stdout,
                    "{}",
                    json!({"type":"RingInstalled","ring_id":ring_id})
                );
            }
            "ExecuteStep" => {
                let step_id = value
                    .get("step_id")
                    .and_then(|value| value.as_u64())
                    .unwrap_or(0);
                let _ = writeln!(
                    stdout,
                    "{}",
                    json!({"type":"StepCompleted","step_id":step_id})
                );
            }
            "ReleaseDeviceObject" => {
                let handle = value
                    .get("handle")
                    .and_then(|value| value.as_u64())
                    .unwrap_or(0);
                let _ = writeln!(
                    stdout,
                    "{}",
                    json!({"type":"DeviceObjectReleased","handle":handle})
                );
            }
            "RingReadable" => {
                let ring_id = value
                    .get("ring_id")
                    .and_then(|value| value.as_u64())
                    .unwrap_or(0);
                let _ = writeln!(
                    stdout,
                    "{}",
                    json!({"type":"RingReadableAck","ring_id":ring_id})
                );
            }
            "ShutdownWorker" => {
                let _ = writeln!(stdout, "{}", json!({"type":"WorkerStopped"}));
                let _ = stdout.flush();
                return ExitCode::SUCCESS;
            }
            _ => {
                let _ = writeln!(
                    stdout,
                    "{}",
                    json!({"type":"ProcessFault","reason":"unknown-command","command":kind})
                );
            }
        }
        let _ = stdout.flush();
    }

    ExitCode::SUCCESS
}
