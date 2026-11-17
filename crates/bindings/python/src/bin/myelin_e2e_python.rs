#![cfg(not(test))]

use std::env;
use std::fs;
use std::io::Read as _;

use base64::Engine as _;
use flate2::read::ZlibDecoder;
use swactor::launcher::{Engine, RuntimeConfig, RuntimeParts, TokioBackend, TokioConfig};

fn source() -> Result<String, String> {
    let mut args = env::args().skip(1);
    let option = args
        .next()
        .ok_or_else(|| "expected one generated Python source option".to_owned())?;
    let value = args
        .next()
        .ok_or_else(|| format!("{option} requires a value"))?;
    if args.next().is_some() {
        return Err("expected exactly one generated Python source option".to_owned());
    }
    match option.as_str() {
        "--source-file" => fs::read_to_string(&value)
            .map_err(|error| format!("read generated Python source {value}: {error}")),
        "--source-env" | "--source-env-zlib" => {
            let encoded = env::var(&value).map_err(|_| {
                format!("generated program environment variable is missing: {value}")
            })?;
            let bytes = base64::engine::general_purpose::STANDARD
                .decode(encoded)
                .map_err(|error| format!("decode generated Python source: {error}"))?;
            let bytes = if option == "--source-env-zlib" {
                let mut decoded = Vec::new();
                ZlibDecoder::new(bytes.as_slice())
                    .read_to_end(&mut decoded)
                    .map_err(|error| format!("decompress generated Python source: {error}"))?;
                decoded
            } else {
                bytes
            };
            String::from_utf8(bytes)
                .map_err(|error| format!("generated Python source is not UTF-8: {error}"))
        }
        _ => Err(format!(
            "unsupported generated Python source option: {option}"
        )),
    }
}

fn main() {
    let result = source().and_then(|source| {
        let parts = RuntimeParts::new(RuntimeConfig::default());
        let runtime = parts.runtime().clone();
        let backend = TokioBackend::new(TokioConfig::default())
            .map_err(|error| format!("start contextual runtime: {error}"))?;
        let engine = Engine::new(parts, backend)
            .map_err(|error| format!("start contextual engine: {error}"))?;
        let tasks = engine.task_spawner();
        swactor::run_contextual_source(&source, runtime, engine, tasks)
            .map_err(|error| error.to_string())
    });
    if let Err(error) = result {
        eprintln!("myelin-e2e-python: {error}");
        std::process::exit(1);
    }
}
