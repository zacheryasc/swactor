use std::io::{self, BufRead, BufReader, Write};
use std::net::TcpStream;
use std::process::ExitCode;

use mvp_system::prompt_rpc::{PromptEvent, SubmitPrompt, write_json_line};

const DEFAULT_RPC_ADDR: &str = "127.0.0.1:19777";
const DEFAULT_MAX_TOKENS: u32 = 64;
const DEFAULT_TIMEOUT_MS: u64 = 120_000;

fn main() -> ExitCode {
    match run() {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => {
            eprintln!("mvp-chat: {error}");
            ExitCode::from(1)
        }
    }
}

fn run() -> Result<(), String> {
    let config = Config::from_env_and_args()?;
    let mut stream = TcpStream::connect(&config.addr)
        .map_err(|e| format!("connect prompt RPC {}: {e}", config.addr))?;
    let mut reader = BufReader::new(
        stream
            .try_clone()
            .map_err(|e| format!("clone prompt RPC stream: {e}"))?,
    );
    let stdin = io::stdin();
    let mut next_request_id = 1_u64;

    eprintln!("mvp-chat: connected to {}", config.addr);
    loop {
        print!("> ");
        io::stdout()
            .flush()
            .map_err(|e| format!("flush prompt: {e}"))?;
        let mut prompt = String::new();
        let n = stdin
            .read_line(&mut prompt)
            .map_err(|e| format!("read stdin: {e}"))?;
        if n == 0 {
            return Ok(());
        }
        let prompt = prompt.trim_end().to_owned();
        if prompt.eq_ignore_ascii_case("/quit") || prompt.eq_ignore_ascii_case("/exit") {
            return Ok(());
        }
        if prompt.trim().is_empty() {
            continue;
        }

        let request_id = next_request_id;
        next_request_id = next_request_id.wrapping_add(1).max(1);
        let request = SubmitPrompt {
            request_id,
            prompt_text: prompt,
            max_tokens: config.max_tokens,
            timeout_ms: config.timeout_ms,
        };
        write_json_line(&mut stream, &request)?;

        loop {
            let mut line = String::new();
            let n = reader
                .read_line(&mut line)
                .map_err(|e| format!("read prompt event: {e}"))?;
            if n == 0 {
                return Err("prompt RPC closed".to_owned());
            }
            let event = serde_json::from_str::<PromptEvent>(&line)
                .map_err(|e| format!("parse prompt event: {e}"))?;
            match event {
                PromptEvent::TextDelta {
                    request_id: seen,
                    text,
                } if seen == request_id => {
                    print!("{text}");
                    io::stdout()
                        .flush()
                        .map_err(|e| format!("flush text delta: {e}"))?;
                }
                PromptEvent::Done {
                    request_id: seen,
                    tokens_generated,
                    elapsed_ms,
                    ..
                } if seen == request_id => {
                    println!();
                    eprintln!(
                        "mvp-chat: done request={} tokens={} elapsed_ms={}",
                        seen, tokens_generated, elapsed_ms
                    );
                    break;
                }
                PromptEvent::Fault {
                    request_id: seen,
                    error,
                } if seen == request_id => {
                    eprintln!("mvp-chat: fault request={seen}: {error}");
                    break;
                }
                _ => {}
            }
        }
    }
}

struct Config {
    addr: String,
    max_tokens: u32,
    timeout_ms: u64,
}

impl Config {
    fn from_env_and_args() -> Result<Self, String> {
        let mut config = Self {
            addr: std::env::var("MVP_PROMPT_RPC_ADDR")
                .unwrap_or_else(|_| DEFAULT_RPC_ADDR.to_owned()),
            max_tokens: env_u32("MVP_PROMPT_MAX_TOKENS", DEFAULT_MAX_TOKENS)?,
            timeout_ms: env_u64("MVP_PROMPT_TIMEOUT_MS", DEFAULT_TIMEOUT_MS)?,
        };
        let mut args = std::env::args().skip(1);
        while let Some(arg) = args.next() {
            match arg.as_str() {
                "--addr" => {
                    config.addr = args
                        .next()
                        .ok_or_else(|| "missing value after --addr".to_owned())?
                }
                "--max-tokens" => {
                    config.max_tokens = parse_next(&mut args, "--max-tokens")?;
                }
                "--timeout-ms" => {
                    config.timeout_ms = parse_next(&mut args, "--timeout-ms")?;
                }
                other => return Err(format!("unknown argument {other:?}")),
            }
        }
        Ok(config)
    }
}

fn env_u64(name: &str, default: u64) -> Result<u64, String> {
    match std::env::var(name).ok().filter(|value| !value.is_empty()) {
        Some(value) => value
            .parse::<u64>()
            .map_err(|e| format!("invalid {name}={value:?}: {e}")),
        None => Ok(default),
    }
}

fn env_u32(name: &str, default: u32) -> Result<u32, String> {
    match std::env::var(name).ok().filter(|value| !value.is_empty()) {
        Some(value) => value
            .parse::<u32>()
            .map_err(|e| format!("invalid {name}={value:?}: {e}")),
        None => Ok(default),
    }
}

fn parse_next<T>(args: &mut impl Iterator<Item = String>, name: &str) -> Result<T, String>
where
    T: std::str::FromStr,
    T::Err: std::fmt::Display,
{
    let value = args
        .next()
        .ok_or_else(|| format!("missing value after {name}"))?;
    value
        .parse::<T>()
        .map_err(|e| format!("invalid {name}={value:?}: {e}"))
}
