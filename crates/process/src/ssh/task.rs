use std::sync::{Arc, OnceLock};
use std::time::Duration;

use russh::client;
use russh::{ChannelMsg, Sig};
use russh_keys::key::PublicKey;
use tokio::sync::mpsc;
use tokio::time::{Instant, sleep_until};

use crate::event::ProcessEvent;
use crate::types::EventQueue;
use crate::types::{ExitStatus, ProcessMode, ProcessSpec, Signal};
use crate::types::ProcessWaker;

use super::config::SshConfig;

/// Minimal SSH client handler that accepts all host keys.
pub(super) struct SshHandler;

#[async_trait::async_trait]
impl client::Handler for SshHandler {
    type Error = russh::Error;

    async fn check_server_key(
        &mut self,
        _server_public_key: &PublicKey,
    ) -> Result<bool, Self::Error> {
        Ok(true)
    }
}

/// Commands sent from the SshDriver to the background task.
pub(super) enum SshCommand {
    WriteStdin(Vec<u8>),
    SendSignal(Signal),
    ResizePty { cols: u16, rows: u16 },
    CloseStdin,
    ScheduleKillTimeout(Duration),
}

/// Run the full SSH session lifecycle.
///
/// Three phases: connect+auth, channel setup, event loop.
/// All events are pushed to `queue` and the waker is fired.
pub(super) async fn run_ssh_session(
    config: SshConfig,
    spec: ProcessSpec,
    queue: EventQueue,
    waker_slot: Arc<OnceLock<ProcessWaker>>,
    mut command_rx: mpsc::UnboundedReceiver<SshCommand>,
) {
    let has_pty = spec.mode == ProcessMode::Interactive;

    // --- Phase 1: Connect + Auth ---
    let session = match connect_and_auth(&config).await {
        Ok(session) => session,
        Err(e) => {
            push_and_wake(&queue, &waker_slot, ProcessEvent::SpawnFailed {
                reason: format!("SSH connection failed: {e}"),
            });
            return;
        }
    };

    // --- Phase 2: Channel setup ---
    let channel = match setup_channel(&session, &spec, has_pty).await {
        Ok(ch) => ch,
        Err(e) => {
            push_and_wake(&queue, &waker_slot, ProcessEvent::SpawnFailed {
                reason: format!("SSH channel setup failed: {e}"),
            });
            return;
        }
    };

    push_and_wake(&queue, &waker_slot, ProcessEvent::Started);

    // --- Phase 3: Event loop ---
    run_event_loop(channel, &mut command_rx, &queue, &waker_slot, has_pty).await;
}

async fn connect_and_auth(
    config: &SshConfig,
) -> Result<russh::client::Handle<SshHandler>, Box<dyn std::error::Error + Send + Sync>> {
    let ssh_config = russh::client::Config {
        keepalive_interval: Some(Duration::from_secs(30)),
        keepalive_max: 3,
        ..Default::default()
    };

    let mut session = russh::client::connect(
        Arc::new(ssh_config),
        (config.host.as_str(), config.port),
        SshHandler,
    )
    .await?;

    let key = russh_keys::load_secret_key(
        &config.key_file,
        config.key_passphrase.as_deref(),
    )?;

    let authenticated = session
        .authenticate_publickey(&config.username, Arc::new(key))
        .await?;

    if !authenticated {
        return Err("authentication rejected by server".into());
    }

    Ok(session)
}

async fn setup_channel(
    session: &russh::client::Handle<SshHandler>,
    spec: &ProcessSpec,
    has_pty: bool,
) -> Result<russh::Channel<russh::client::Msg>, Box<dyn std::error::Error + Send + Sync>> {
    let channel = session.channel_open_session().await?;

    if has_pty {
        let (cols, rows) = spec
            .initial_pty_size
            .map(|s| (s.cols as u32, s.rows as u32))
            .unwrap_or((80, 24));
        channel
            .request_pty(true, "xterm-256color", cols, rows, 0, 0, &[])
            .await?;
    }

    // Best-effort env vars (many SSH servers restrict SetEnv)
    for (key, val) in &spec.env {
        let _ = channel.set_env(true, key, val).await;
    }

    if has_pty {
        channel.request_shell(true).await?;
    } else {
        let cmd = build_remote_command(spec);
        channel.exec(true, cmd).await?;
    }

    Ok(channel)
}

async fn run_event_loop(
    mut channel: russh::Channel<russh::client::Msg>,
    command_rx: &mut mpsc::UnboundedReceiver<SshCommand>,
    queue: &EventQueue,
    waker_slot: &Arc<OnceLock<ProcessWaker>>,
    has_pty: bool,
) {
    let mut pending_exit: Option<ExitStatus> = None;
    let mut kill_deadline: Option<Instant> = None;

    loop {
        // Build the kill-timeout future
        let kill_sleep = async {
            match kill_deadline {
                Some(deadline) => sleep_until(deadline).await,
                None => std::future::pending().await,
            }
        };

        tokio::select! {
            msg = channel.wait() => {
                match msg {
                    Some(ChannelMsg::Data { data }) => {
                        push_and_wake(queue, waker_slot, ProcessEvent::OutputReceived {
                            data: data.to_vec(),
                            is_stderr: false,
                        });
                    }
                    Some(ChannelMsg::ExtendedData { data, ext: 1 }) => {
                        push_and_wake(queue, waker_slot, ProcessEvent::OutputReceived {
                            data: data.to_vec(),
                            is_stderr: true,
                        });
                    }
                    Some(ChannelMsg::ExtendedData { .. }) => {
                        // Ignore non-stderr extended data
                    }
                    Some(ChannelMsg::ExitStatus { exit_status }) => {
                        pending_exit = Some(ExitStatus::Code(exit_status as i32));
                    }
                    Some(ChannelMsg::ExitSignal { signal_name, .. }) => {
                        pending_exit = Some(ExitStatus::Signal(
                            signal_name_to_code(&signal_name),
                        ));
                    }
                    Some(ChannelMsg::Eof) | Some(ChannelMsg::Close) | None => {
                        let status = pending_exit.take().unwrap_or(ExitStatus::Unknown);
                        push_and_wake(queue, waker_slot, ProcessEvent::Exited { status });
                        break;
                    }
                    _ => {}
                }
            }

            cmd = command_rx.recv() => {
                match cmd {
                    Some(SshCommand::WriteStdin(data)) => {
                        let len = data.len();
                        match channel.data(&data[..]).await {
                            Ok(()) => {
                                push_and_wake(queue, waker_slot, ProcessEvent::StdinWritten {
                                    byte_count: len,
                                });
                            }
                            Err(e) => {
                                push_and_wake(queue, waker_slot, ProcessEvent::ConnectionLost {
                                    reason: format!("stdin write failed: {e}"),
                                });
                            }
                        }
                    }
                    Some(SshCommand::SendSignal(Signal::Kill)) => {
                        let _ = channel.close().await;
                        push_and_wake(queue, waker_slot, ProcessEvent::SignalSent);
                    }
                    Some(SshCommand::SendSignal(signal)) => {
                        let sig = signal_to_russh(signal);
                        let _ = channel.signal(sig).await;
                        push_and_wake(queue, waker_slot, ProcessEvent::SignalSent);
                    }
                    Some(SshCommand::ResizePty { cols, rows }) => {
                        if has_pty {
                            let _ = channel.window_change(
                                cols as u32, rows as u32, 0, 0,
                            ).await;
                        }
                        push_and_wake(queue, waker_slot, ProcessEvent::PtyResized);
                    }
                    Some(SshCommand::CloseStdin) => {
                        let _ = channel.eof().await;
                    }
                    Some(SshCommand::ScheduleKillTimeout(dur)) => {
                        kill_deadline = Some(Instant::now() + dur);
                    }
                    None => {
                        // Sender dropped — close channel
                        let _ = channel.close().await;
                        break;
                    }
                }
            }

            _ = kill_sleep => {
                push_and_wake(queue, waker_slot, ProcessEvent::KillTimeout);
                kill_deadline = None;
            }
        }
    }
}

/// Push an event and wake the actor.
fn push_and_wake(
    queue: &EventQueue,
    waker_slot: &Arc<OnceLock<ProcessWaker>>,
    event: ProcessEvent,
) {
    queue.push(event);
    if let Some(w) = waker_slot.get() {
        w.wake();
    }
}

/// Build a remote exec command string from a ProcessSpec.
///
/// Produces: `cd '<dir>' && KEY='VAL' ... <cmd> <args>`
fn build_remote_command(spec: &ProcessSpec) -> String {
    let mut parts = Vec::new();

    if let Some(ref dir) = spec.working_dir {
        parts.push(format!("cd {}", shell_escape(dir)));
    }

    for (key, val) in &spec.env {
        parts.push(format!("{}={}", key, shell_escape(val)));
    }

    let mut cmd = shell_escape(&spec.command);
    for arg in &spec.args {
        cmd.push(' ');
        cmd.push_str(&shell_escape(arg));
    }
    parts.push(cmd);

    if parts.len() > 1 && spec.working_dir.is_some() {
        // Join with && so cd failure aborts
        let cd_part = parts.remove(0);
        format!("{} && {}", cd_part, parts.join(" "))
    } else {
        parts.join(" ")
    }
}

/// POSIX single-quote escaping: wrap in single quotes, escape embedded quotes.
fn shell_escape(s: &str) -> String {
    if s.is_empty() {
        return "''".to_string();
    }
    // If the string is simple (alphanumeric + safe chars), no quoting needed
    if s.chars().all(|c| c.is_ascii_alphanumeric() || matches!(c, '-' | '_' | '.' | '/' | ':' | ',' | '+' | '=')) {
        return s.to_string();
    }
    // Single-quote the string, replacing ' with '\''
    format!("'{}'", s.replace('\'', "'\\''"))
}

fn signal_to_russh(signal: Signal) -> Sig {
    match signal {
        Signal::Terminate => Sig::TERM,
        Signal::Kill => Sig::KILL,
        Signal::Hangup => Sig::HUP,
        Signal::Interrupt => Sig::INT,
        Signal::Other(_) => Sig::TERM, // Best effort fallback
    }
}

fn signal_name_to_code(sig: &Sig) -> i32 {
    match sig {
        Sig::HUP => 1,
        Sig::INT => 2,
        Sig::QUIT => 3,
        Sig::ABRT => 6,
        Sig::KILL => 9,
        Sig::ALRM => 14,
        Sig::TERM => 15,
        Sig::USR1 => 10,
        _ => 15, // Default to SIGTERM code
    }
}
