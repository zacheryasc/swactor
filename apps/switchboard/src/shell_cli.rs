//! Byte-stream client; only the daemon owns the node identity and remote process.

#[cfg(not(test))]
use std::io::IsTerminal;

use anyhow::{Context, Result, bail};
#[cfg(not(test))]
use swactor_engine::BlockingWorkSender;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};

#[cfg(not(test))]
use crate::bootstrap::Bootstrap;
#[cfg(not(test))]
use crate::cli_terminal::{Event, Terminal};
#[cfg(not(test))]
use crate::local_control;
use crate::protocol::{Frame, MAX_DATA, read_frame, write_frame};

#[cfg(not(test))]
pub async fn run(target: String, blocking: &BlockingWorkSender) -> Result<i32> {
    // Redirected connections retain ordinary pipes and final-command completion.
    let interactive = std::io::stdin().is_terminal()
        && std::io::stdout().is_terminal()
        && std::io::stderr().is_terminal();
    let bootstrap = Bootstrap::load(&Bootstrap::default_path()?)?;
    let mut stream = local_control::connect(bootstrap.operator_public_key).await?;
    let mut terminal = if interactive { Some(Terminal::open(blocking)?) } else { None };
    let open = match &terminal {
        Some(terminal) => Frame::OpenTerminal { target: target.clone(), size: terminal.size },
        None => Frame::Open { target: target.clone() },
    };
    write_frame(&mut stream, &open).await?;
    let (mut reader, mut writer) = tokio::io::split(stream);
    let mut output = tokio::io::stdout();
    let mut error = tokio::io::stderr();
    let mut input = tokio::io::stdin();
    let send = async {
        match &mut terminal {
            Some(terminal) => forward_terminal(terminal, &mut writer).await,
            None => forward_input(&mut input, &mut writer).await.map(|()| 1),
        }
    };
    let receive = async {
        await_ready(&mut reader).await?;
        if interactive {
            error.write_all(format!("Connected to {target:?}. Ctrl-] disconnects.\r\n").as_bytes()).await?;
            error.flush().await?;
        }
        forward_output(&mut reader, &mut output, &mut error).await
    };
    let result = tokio::select! {
        result = send => result,
        result = receive => result,
    };
    output.flush().await?;
    error.flush().await?;
    // Terminal's guard also restores the console on every error/cancellation path.
    drop(terminal);
    result
}

async fn await_ready(reader: &mut (impl AsyncRead + Unpin)) -> Result<()> {
    loop {
        match read_frame(reader).await? {
            Some(Frame::Ready) => return Ok(()),
            Some(Frame::Ping) => {}
            Some(Frame::Error(error)) => bail!("{error}"),
            None => bail!("operator daemon disconnected before opening the shell"),
            _ => bail!("operator daemon sent an unexpected shell opening response"),
        }
    }
}

async fn forward_input<R: AsyncRead + Unpin, W: AsyncWrite + Unpin>(
    input: &mut R, writer: &mut W,
) -> Result<()> {
    let mut data = Vec::with_capacity(MAX_DATA);
    let mut needs_newline = false;
    loop {
        data.clear();
        let count = (&mut *input).take(MAX_DATA as u64).read_buf(&mut data).await?;
        let eof = count == 0;
        if eof && needs_newline { data.push(b'\n'); }
        if !data.is_empty() {
            needs_newline = data.last() != Some(&b'\n');
            let frame = Frame::Stdin(data);
            write_frame(writer, &frame).await?;
            data = match frame { Frame::Stdin(data) => data, _ => unreachable!() };
        }
        if eof {
            write_frame(writer, &Frame::StdinClosed).await?;
            // EOF closes only remote stdin. Output owns normal session completion.
            return std::future::pending().await;
        }
    }
}

#[cfg(not(test))]
async fn forward_terminal<W: AsyncWrite + Unpin>(terminal: &mut Terminal, writer: &mut W) -> Result<i32> {
    while let Some(event) = terminal.next().await? {
        match event {
            Event::Resize(size) => write_frame(writer, &Frame::Resize(size)).await?,
            Event::Input(bytes) => if terminal_bytes(bytes, writer).await? { return Ok(0); },
        }
    }
    // Terminal EOF, unlike pipe EOF, cancels the owned remote session.
    Ok(0)
}

async fn terminal_bytes<W: AsyncWrite + Unpin>(mut bytes: Vec<u8>, writer: &mut W) -> Result<bool> {
    let disconnect = bytes.iter().position(|byte| *byte == 0x1d);
    if let Some(end) = disconnect { bytes.truncate(end); }
    if !bytes.is_empty() { write_frame(writer, &Frame::Stdin(bytes)).await?; }
    Ok(disconnect.is_some())
}

async fn forward_output<R: AsyncRead + Unpin, O: AsyncWrite + Unpin, E: AsyncWrite + Unpin>(
    reader: &mut R, output: &mut O, error: &mut E,
) -> Result<i32> {
    loop {
        // This future remains inside the independent output pump across input
        // progress; a partially received frame is never restarted.
        match read_frame(reader).await?.context("operator daemon disconnected before the shell exited")? {
            Frame::Ping => {}
            Frame::Stdout(bytes) => { output.write_all(&bytes).await?; output.flush().await?; }
            Frame::Stderr(bytes) => { error.write_all(&bytes).await?; error.flush().await?; }
            Frame::Exit(status) => return Ok(status),
            Frame::Error(error) => bail!("{error}"),
            _ => bail!("operator daemon sent an unexpected shell frame"),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn eof_finishes_unterminated_commands_without_ending_the_output_pump() {
        engine_test!({
        for script in [b"".as_slice(), b"last", b"last\n", b"a\nb"] {
            let (mut writer, mut reader) = tokio::io::duplex(32);
            let mut input = script;
            let receive = async {
                let mut bytes = Vec::new();
                loop {
                    match read_frame(&mut reader).await.unwrap().unwrap() {
                        Frame::Stdin(data) => bytes.extend(data),
                        Frame::StdinClosed => return bytes,
                        frame => panic!("unexpected input {frame:?}"),
                    }
                }
            };
            let bytes = tokio::select! {
                result = forward_input(&mut input, &mut writer) => panic!("EOF ended the session: {result:?}"),
                bytes = receive => bytes,
            };
            let mut expected = script.to_vec();
            if !expected.is_empty() && !expected.ends_with(b"\n") { expected.push(b'\n'); }
            assert_eq!(bytes, expected);
        }
        });
    }

    #[test]
    fn output_streams_stay_separate_and_exit_does_not_wait_for_stdin() {
        engine_test!({
        let (mut remote, local) = tokio::io::duplex(1);
        let (mut reader, mut writer) = tokio::io::split(local);
        let mut input = b"input whose frame is backpressured".as_slice();
        let mut stdout = Vec::new();
        let mut stderr = Vec::new();
        let client = async {
            await_ready(&mut reader).await.unwrap();
            tokio::select! {
                result = forward_input(&mut input, &mut writer) => panic!("input ended: {result:?}"),
                result = forward_output(&mut reader, &mut stdout, &mut stderr) => result.unwrap(),
            }
        };
        let server = async {
            for frame in [
                Frame::Ping, Frame::Ready, Frame::Stdout(vec![0, 255]),
                Frame::Ping, Frame::Stderr(b"ordinary error".to_vec()),
                Frame::Stdout(b"after stderr".to_vec()), Frame::Exit(23),
            ] {
                write_frame(&mut remote, &frame).await.unwrap();
            }
        };
        let (status, ()) = tokio::join!(client, server);
        assert_eq!(status, 23);
        assert_eq!(stdout, [b"\0\xff".as_slice(), b"after stderr"].concat());
        assert_eq!(stderr, b"ordinary error");
        });
    }

    #[test]
    fn terminal_keys_are_bytes_and_only_disconnect_is_local() {
        engine_test!({
            let mut frames = Vec::new();
            let keys = b"\x1b[A\x1b[D\x03\r".to_vec();
            assert!(!terminal_bytes(keys.clone(), &mut frames).await.unwrap());
            assert!(terminal_bytes(b"last\x1dnot sent".to_vec(), &mut frames).await.unwrap());
            let mut reader = frames.as_slice();
            assert_eq!(read_frame(&mut reader).await.unwrap(), Some(Frame::Stdin(keys)));
            assert_eq!(read_frame(&mut reader).await.unwrap(), Some(Frame::Stdin(b"last".to_vec())));
            assert_eq!(read_frame(&mut reader).await.unwrap(), None);
        });
    }
}
