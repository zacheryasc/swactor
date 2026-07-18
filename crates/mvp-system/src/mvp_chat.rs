use std::io::{self, BufRead, Write};
use std::path::PathBuf;
use std::sync::Arc;

use datastream::{DatastreamEndpoint, Lifetime, StreamDescriptor, StreamId, StreamOrigin};
use swactor::Error;
use tokio::sync::Notify;
type Result<T> = std::result::Result<T, swactor::Error>;

#[derive(Clone)]
enum ChatSignals {}

const MVP_CHAT_DATASTREAM_NODE: &str = "mvp-chat";
const MVP_CHAT_DATASTREAM_LABEL: &str = "mvp chat";

struct MvpChatDatastream {
    endpoint: Arc<DatastreamEndpoint>,
    wake: Arc<Notify>,
}

impl MvpChatDatastream {
    fn new(run_id: u64) -> Self {
        let stream = StreamId::new(MVP_CHAT_DATASTREAM_NODE, Lifetime(run_id));
        let endpoint = DatastreamEndpoint::with_descriptor(
            StreamDescriptor {
                stream,
                label: Some(MVP_CHAT_DATASTREAM_LABEL.to_owned()),
                origin: StreamOrigin::Orchestrator,
            },
            4096,
            1024,
        );

        Self {
            endpoint: Arc::new(endpoint),
            wake: Arc::new(Notify::new()),
        }
    }

    fn spawn_handler(&self, handle: &tokio::runtime::Handle) -> tokio::task::JoinHandle<()> {
        let endpoint = Arc::clone(&self.endpoint);
        let wake = Arc::clone(&self.wake);

        handle.spawn(async move {
            loop {
                wake.notified().await;

                while endpoint.tick().drained != 0 {}
            }
        })
    }
}

#[derive(Debug, PartialEq, Eq)]
enum PromptStep {
    Response(String),
    Ignore,
    Exit,
}

fn read_prompt_step<R>(input: &mut R) -> PromptStep
where
    R: BufRead,
{
    let mut line = String::new();

    match input.read_line(&mut line) {
        Ok(0) => return PromptStep::Exit,
        Ok(_) => {}
        Err(_) => return PromptStep::Exit,
    }

    let prompt = line.trim_end().to_owned();

    if prompt.trim().is_empty() {
        return PromptStep::Ignore;
    }

    PromptStep::Response(prompt_response(&prompt))
}

fn prompt_response(prompt: &str) -> String {
    format!("Hello, {prompt}!")
}

fn run_prompt_loop() -> Result<()> {
    let stdin = io::stdin();
    let mut input = stdin.lock();

    let stdout = io::stdout();
    let mut output = stdout.lock();

    run_prompt_loop_with_io(&mut input, &mut output)
}

fn run_prompt_loop_with_io<R, W>(input: &mut R, output: &mut W) -> Result<()>
where
    R: BufRead,
    W: Write,
{
    loop {
        write!(output, "prompt:> ")
            .map_err(|error| Error::from(format!("write prompt marker: {error}")))?;
        output
            .flush()
            .map_err(|error| Error::from(format!("flush prompt marker: {error}")))?;

        match read_prompt_step(input) {
            PromptStep::Response(response) => {
                writeln!(output, "{response}")
                    .map_err(|error| Error::from(format!("write prompt response: {error}")))?;
            }
            PromptStep::Ignore => continue,
            PromptStep::Exit => return Ok(()),
        }
    }
}

enum ChatRuntimeEvent {
    PromptExited(Result<()>),
    PromptPanicked,
    CtrlC(std::io::Result<()>),
}

struct PromptLoop {
    join: Option<std::thread::JoinHandle<()>>,
}

impl PromptLoop {
    fn spawn(events: tokio::sync::mpsc::UnboundedSender<ChatRuntimeEvent>) -> Result<Self> {
        let join = std::thread::Builder::new()
            .name("mvp-chat-prompt".to_owned())
            .spawn(move || {
                let event = match std::panic::catch_unwind(run_prompt_loop) {
                    Ok(result) => ChatRuntimeEvent::PromptExited(result),
                    Err(_) => ChatRuntimeEvent::PromptPanicked,
                };

                let _ = events.send(event);
            })
            .map_err(|error| Error::from(format!("spawn prompt loop: {error}")))?;

        Ok(Self { join: Some(join) })
    }

    fn join_finished(&mut self) -> Result<()> {
        let Some(join) = self.join.take() else {
            return Ok(());
        };

        join.join()
            .map_err(|_| Error::from("prompt loop thread panicked".to_owned()))
    }

    fn detach(mut self) {
        let _ = self.join.take();
    }
}

fn spawn_ctrl_c_reporter(
    handle: &tokio::runtime::Handle,
    events: tokio::sync::mpsc::UnboundedSender<ChatRuntimeEvent>,
) {
    handle.spawn(async move {
        let result = tokio::signal::ctrl_c().await;
        let _ = events.send(ChatRuntimeEvent::CtrlC(result));
    });
}

fn join_dashboard_http(result: std::result::Result<(), tokio::task::JoinError>) -> Result<()> {
    result.map_err(|error| Error::from(format!("dashboard HTTP task failed: {error}")))
}

fn join_datastream_handler(result: std::result::Result<(), tokio::task::JoinError>) -> Result<()> {
    result.map_err(|error| Error::from(format!("datastream handler task failed: {error}")))
}

async fn supervise_chat_runtime(
    dashboard: &dashboard::DashboardHandle,
    mut dashboard_http: tokio::task::JoinHandle<()>,
    mut datastream_handler: tokio::task::JoinHandle<()>,
    prompt_loop: &mut PromptLoop,
    events: &mut tokio::sync::mpsc::UnboundedReceiver<ChatRuntimeEvent>,
) -> Result<()> {
    tokio::select! {
        result = &mut dashboard_http => {
            join_dashboard_http(result)?;
            Err("dashboard HTTP server exited before shutdown"
                .to_owned()
                .into())
        }
        result = &mut datastream_handler => {
            join_datastream_handler(result)?;
            Err("datastream handler exited before shutdown"
                .to_owned()
                .into())
        }
        event = events.recv() => {
            let event = event.ok_or_else(|| Error::from("runtime event channel closed".to_owned()))?;
            let run_result = match event {
                ChatRuntimeEvent::PromptExited(prompt_result) => {
                    match prompt_loop.join_finished() {
                        Ok(()) => prompt_result,
                        Err(error) => Err(error),
                    }
                }
                ChatRuntimeEvent::PromptPanicked => {
                    match prompt_loop.join_finished() {
                        Ok(()) => Err("prompt loop panicked".to_owned().into()),
                        Err(error) => Err(error),
                    }
                }
                ChatRuntimeEvent::CtrlC(result) => {
                    result
                        .map_err(|error| Error::from(format!("ctrl-c handler failed: {error}")))
                        .map(|_| ())
                }
            };

            dashboard.shutdown();
            join_dashboard_http(dashboard_http.await)?;

            run_result
        }
    }
}

pub fn run_from_args<I>(args: I) -> Result<()>
where
    I: IntoIterator<Item = String>,
{
    let mut config_path: Option<PathBuf> = None;
    let mut provider_selector: Option<&'static str> = None;

    let mut args = args.into_iter();
    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--process" | "--docker" | "--vastai" => {
                let selected = match arg.as_str() {
                    "--process" => "process",
                    "--docker" => "docker",
                    "--vastai" => "vastai",
                    _ => unreachable!(),
                };
                if provider_selector.replace(selected).is_some() {
                    return Err("conflicting provider selectors; use exactly one of --process, --docker, or --vastai".to_owned().into());
                }
            }
            "--config" => {
                let value = args
                    .next()
                    .ok_or_else(|| "--config requires a path".to_owned())?;
                config_path = Some(PathBuf::from(value));
            }
            "--yes" | "-y" | "--dump-logs" | "--cached-model" | "--skip-rebuild" => {}
            "--pipeline-stages" => {
                let value = args
                    .next()
                    .ok_or_else(|| "--pipeline-stages requires a value".to_owned())?;
                let stages = value
                    .parse::<u32>()
                    .map_err(|error| format!("parse --pipeline-stages: {error}"))?;
                if stages == 0 {
                    return Err("--pipeline-stages must be greater than 0".into());
                }
            }
            value if value.starts_with("--dump-logs=") => {
                if value["--dump-logs=".len()..].is_empty() {
                    return Err("--dump-logs= requires a path".into());
                }
            }
            value if value.starts_with("--cached-model=") => {
                if value["--cached-model=".len()..].is_empty() {
                    return Err("--cached-model= requires a path".into());
                }
            }
            value => return Err(format!("unknown mvp-chat argument: {value}").into()),
        }
    }

    let loaded_config = crate::config::TomlConfigOverlay::load(config_path.as_deref())?;
    let run_id = loaded_config.overlay.runtime.run_id.unwrap_or(0);
    let rt = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .map_err(|e| Error::from(e.to_string()))?;

    let chat_datastream = MvpChatDatastream::new(run_id);
    let datastream_handler = chat_datastream.spawn_handler(rt.handle());

    let _swactor = swactor::runtime::Runtime::new(swactor::runtime::RuntimeConfig {
        num_threads: 1,
        ..Default::default()
    });
    let _chat_inbox = _swactor.new_inbox::<ChatSignals>()?;

    let dashboard = dashboard::DashboardHandle::new(dashboard::DashboardConfig::default());
    let dashboard_http = dashboard.spawn_http(rt.handle());
    let (events_tx, mut events_rx) = tokio::sync::mpsc::unbounded_channel();

    spawn_ctrl_c_reporter(rt.handle(), events_tx.clone());
    let mut prompt_loop = PromptLoop::spawn(events_tx)?;

    let result = rt.block_on(supervise_chat_runtime(
        &dashboard,
        dashboard_http,
        datastream_handler,
        &mut prompt_loop,
        &mut events_rx,
    ));

    if result.is_err() {
        prompt_loop.detach();
    }

    result
}
