use anyhow::{Result, bail, ensure};
use swactor_process::TerminalSize;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};

pub const MAX_DATA: usize = 16 * 1024;

#[derive(Debug, PartialEq, Eq)]
pub enum Frame {
    Open { target: String },
    OpenTerminal { target: String, size: TerminalSize },
    Resize(TerminalSize),
    Describe,
    Name(String),
    Ready,
    Stdin(Vec<u8>),
    Stdout(Vec<u8>),
    Stderr(Vec<u8>),
    Exit(i32),
    Error(String),
    StdinClosed,
    Ping,
}

pub async fn read_frame(reader: &mut (impl AsyncRead + Unpin)) -> Result<Option<Frame>> {
    let mut tag = [0];
    if reader.read(&mut tag).await? == 0 { return Ok(None); }
    let len = reader.read_u32().await? as usize;
    ensure!(len <= MAX_DATA, "shell frame exceeds {MAX_DATA} bytes");
    let frame = match tag[0] {
        0 => {
            ensure!((1..=256).contains(&len), "invalid shell open frame");
            Frame::Open { target: read_text(reader, len).await? }
        }
        1 => { ensure!(len == 0, "invalid describe frame"); Frame::Describe }
        2 => { ensure!(len <= 256, "node name too long"); Frame::Name(read_text(reader, len).await?) }
        3 => { ensure!(len == 0, "invalid ready frame"); Frame::Ready }
        4..=6 => {
            let mut data = vec![0; len];
            reader.read_exact(&mut data).await?;
            match tag[0] {
                4 => Frame::Stdin(data),
                5 => Frame::Stdout(data),
                _ => Frame::Stderr(data),
            }
        }
        7 => { ensure!(len == 4, "invalid exit frame"); Frame::Exit(reader.read_i32().await?) }
        8 => { ensure!(len <= 4096, "shell error too long"); Frame::Error(read_text(reader, len).await?) }
        9 => { ensure!(len == 0, "invalid stdin closure frame"); Frame::StdinClosed }
        10 => { ensure!(len == 0, "invalid ping frame"); Frame::Ping }
        11 => {
            ensure!((5..=260).contains(&len), "invalid terminal open frame");
            let size = read_size(reader).await?;
            Frame::OpenTerminal { target: read_text(reader, len - 4).await?, size }
        }
        12 => { ensure!(len == 4, "invalid terminal resize frame"); Frame::Resize(read_size(reader).await?) }
        other => bail!("unknown shell frame {other}"),
    };
    Ok(Some(frame))
}

async fn read_size(reader: &mut (impl AsyncRead + Unpin)) -> Result<TerminalSize> {
    let size = TerminalSize { rows: reader.read_u16().await?, cols: reader.read_u16().await? };
    ensure!(size.rows != 0 && size.cols != 0, "terminal dimensions must be nonzero");
    Ok(size)
}

async fn read_text(reader: &mut (impl AsyncRead + Unpin), len: usize) -> Result<String> {
    let mut data = vec![0; len];
    reader.read_exact(&mut data).await?;
    Ok(String::from_utf8(data)?)
}

pub async fn write_frame(writer: &mut (impl AsyncWrite + Unpin), frame: &Frame) -> Result<()> {
    let (tag, len) = match frame {
        Frame::Open { target } => {
            ensure!(!target.is_empty() && target.len() <= 256, "invalid node selector");
            (0, target.len())
        }
        Frame::OpenTerminal { target, size } => {
            ensure!(!target.is_empty() && target.len() <= 256, "invalid node selector");
            ensure!(size.rows != 0 && size.cols != 0, "terminal dimensions must be nonzero");
            (11, target.len() + 4)
        }
        Frame::Resize(size) => {
            ensure!(size.rows != 0 && size.cols != 0, "terminal dimensions must be nonzero");
            (12, 4)
        }
        Frame::Describe => (1, 0),
        Frame::Name(name) => { ensure!(name.len() <= 256, "node name too long"); (2, name.len()) }
        Frame::Ready => (3, 0),
        Frame::Stdin(data) => (4, data.len()),
        Frame::Stdout(data) => (5, data.len()),
        Frame::Stderr(data) => (6, data.len()),
        Frame::Exit(_) => (7, 4),
        Frame::Error(error) => { ensure!(error.len() <= 4096, "shell error too long"); (8, error.len()) }
        Frame::StdinClosed => (9, 0),
        Frame::Ping => (10, 0),
    };
    ensure!(len <= MAX_DATA, "shell frame exceeds {MAX_DATA} bytes");
    let length = (len as u32).to_be_bytes();
    writer.write_all(&[tag, length[0], length[1], length[2], length[3]]).await?;
    match frame {
        Frame::Open { target } => writer.write_all(target.as_bytes()).await?,
        Frame::OpenTerminal { target, size } => {
            writer.write_all(&size.rows.to_be_bytes()).await?;
            writer.write_all(&size.cols.to_be_bytes()).await?;
            writer.write_all(target.as_bytes()).await?;
        }
        Frame::Resize(size) => {
            writer.write_all(&size.rows.to_be_bytes()).await?;
            writer.write_all(&size.cols.to_be_bytes()).await?;
        }
        Frame::Name(text) | Frame::Error(text) => writer.write_all(text.as_bytes()).await?,
        Frame::Stdin(data) | Frame::Stdout(data) | Frame::Stderr(data) => writer.write_all(data).await?,
        Frame::Exit(code) => writer.write_all(&code.to_be_bytes()).await?,
        Frame::Describe | Frame::Ready | Frame::StdinClosed | Frame::Ping => {}
    }
    writer.flush().await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn split_frames_preserve_tags_bytes_and_signed_status() {
        engine_test!({
        let (mut writer, mut reader) = tokio::io::duplex(1);
        let frames = vec![
            Frame::Open { target: "computer".to_owned() }, Frame::Describe,
            Frame::Name("node".to_owned()), Frame::Ready,
            Frame::Stdin(vec![0, 255, 10]), Frame::Stdout(vec![7; MAX_DATA]),
            Frame::Stderr(vec![0, 254]), Frame::Exit(-23),
            Frame::Error("é".to_owned()), Frame::StdinClosed, Frame::Ping,
            Frame::OpenTerminal { target: "terminal".to_owned(), size: TerminalSize { rows: 24, cols: 80 } },
            Frame::Resize(TerminalSize { rows: 45, cols: 132 }),
        ];
        let send = async {
            for frame in &frames { write_frame(&mut writer, frame).await.unwrap(); }
            writer.shutdown().await.unwrap();
        };
        let receive = async {
            for expected in &frames { assert_eq!(read_frame(&mut reader).await.unwrap().as_ref(), Some(expected)); }
            assert_eq!(read_frame(&mut reader).await.unwrap(), None);
        };
        tokio::join!(send, receive);
        });
    }

    #[test]
    fn malformed_and_truncated_frames_are_rejected() {
        engine_test!({
        let mut cases = vec![
            vec![13, 0, 0, 0, 0], vec![0, 0, 0, 0, 0],
            vec![0, 0, 0, 1, 1], vec![2, 0, 0, 1, 1],
            vec![1, 0, 0, 0, 1], vec![3, 0, 0, 0, 1],
            vec![7, 0, 0, 0, 3], vec![8, 0, 0, 16, 1],
            vec![9, 0, 0, 0, 1], vec![10, 0, 0, 0, 1],
            vec![4, 0, 0, 64, 1], vec![5, 0, 0, 0, 2, 1],
            vec![0, 0, 0, 0, 1, 255], vec![2, 0, 0, 0, 1, 255],
            vec![8, 0, 0, 0, 1, 255],
            vec![11, 0, 0, 0, 4, 0, 24, 0, 80],
            vec![11, 0, 0, 0, 5, 0, 0, 0, 80, b'x'],
            vec![12, 0, 0, 0, 3], vec![12, 0, 0, 0, 4, 0, 24, 0, 0],
        ];
        for len in 1..5 { cases.push(vec![0; len]); }
        for bytes in cases {
            assert!(read_frame(&mut bytes.as_slice()).await.is_err(), "accepted {bytes:?}");
        }
        let mut output = Vec::new();
        assert!(write_frame(&mut output, &Frame::Stdin(vec![0; MAX_DATA + 1])).await.is_err());
        assert!(output.is_empty(), "invalid frame wrote a partial header");
        assert!(write_frame(&mut output, &Frame::Resize(TerminalSize { rows: 0, cols: 80 })).await.is_err());
        assert!(output.is_empty());
        });
    }
}
