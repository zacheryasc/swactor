#![cfg(not(test))]

use std::env;
use std::io::{self, Write as _};
use std::sync::Arc;

use base64::Engine as _;
use flate2::read::ZlibDecoder;
use myelin_e2e_fuzz::native::{NativeProgram, run_contextual_program};
use swactor::runtime::{RuntimeConfig, RuntimeParts};
use swactor_engine::{Engine, TokioBackend, TokioConfig};

fn program() -> Result<NativeProgram, String> {
    let mut args = env::args().skip(1);
    let option = args
        .next()
        .ok_or_else(|| "expected --program-env-zlib ENVIRONMENT_VARIABLE".to_owned())?;
    let variable = args
        .next()
        .ok_or_else(|| format!("{option} requires an environment variable name"))?;
    if option != "--program-env-zlib" || args.next().is_some() {
        return Err("expected exactly --program-env-zlib ENVIRONMENT_VARIABLE".into());
    }
    let encoded = env::var(&variable)
        .map_err(|error| format!("read native program environment {variable}: {error}"))?;
    let compressed = base64::engine::general_purpose::STANDARD
        .decode(encoded)
        .map_err(|error| format!("decode native program: {error}"))?;
    // Deserialize incrementally rather than materializing an unbounded expanded
    // JSON copy. Core admission checks bound payloads, actions, and allocations.
    let program: NativeProgram = serde_json::from_reader(ZlibDecoder::new(compressed.as_slice()))
        .map_err(|error| format!("parse native program: {error}"))?;
    program.case.validate()?;
    if program.budget_ms == 0 {
        return Err("native process budget must be nonzero".into());
    }
    if !program
        .case
        .processes
        .iter()
        .any(|process| process.id == program.process_id)
    {
        return Err(format!(
            "unknown native process role: {}",
            program.process_id
        ));
    }
    Ok(program)
}

fn main() {
    let result = program().and_then(|program| {
        let parts = RuntimeParts::new(RuntimeConfig::default());
        let runtime = parts.runtime().clone();
        let backend = TokioBackend::new(TokioConfig::default())
            .map_err(|error| format!("start native runtime: {error}"))?;
        let engine =
            Engine::new(parts, backend).map_err(|error| format!("start native engine: {error}"))?;
        let tasks = engine.task_spawner();
        let result = run_contextual_program(
            program,
            runtime,
            &engine,
            tasks,
            Arc::new(|record| {
                let mut stdout = io::stdout().lock();
                serde_json::to_writer(&mut stdout, record)?;
                stdout.write_all(b"\n")?;
                // Stdout carries causal stop/release barriers. Flush before a peer
                // can wait for the controller's response to this milestone.
                stdout.flush()
            }),
        )
        .map_err(|error| error.to_string());
        drop(engine);
        result
    });
    if let Err(error) = result {
        eprintln!("myelin-e2e-native: {error}");
        std::process::exit(1);
    }
}
