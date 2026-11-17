//! Native console input, owned by the CLI engine and restored before it exits.

use std::io;
use std::sync::{Arc, atomic::{AtomicBool, Ordering}};

use swactor_engine::{BlockingWorkSender, OwnedIoThread};
use swactor_process::TerminalSize;
use tokio::sync::mpsc;

use crate::protocol::MAX_DATA;

pub enum Event {
    Input(Vec<u8>),
    Resize(TerminalSize),
}

pub struct Terminal {
    // Drop joins the reader before this guard restores the console modes.
    _mode: platform::Mode,
    events: mpsc::Receiver<io::Result<Event>>,
    stop: Arc<AtomicBool>,
    worker: Option<OwnedIoThread<()>>,
    pub size: TerminalSize,
}

impl Terminal {
    pub fn open(blocking: &BlockingWorkSender) -> io::Result<Self> {
        let (mode, mut input, size) = platform::open()?;
        let (send, events) = mpsc::channel(16);
        let stop = Arc::new(AtomicBool::new(false));
        let stopping = stop.clone();
        let worker = blocking.spawn_owned_thread("shell-console-input", move || {
            let result = input.run(&send, &stopping, size);
            if let Err(error) = result { let _ = send.blocking_send(Err(error)); }
        })?;
        Ok(Self { _mode: mode, events, stop, worker: Some(worker), size })
    }

    pub async fn next(&mut self) -> io::Result<Option<Event>> {
        self.events.recv().await.transpose()
    }
}

impl Drop for Terminal {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::Release);
        // Release a reader blocked by input backpressure before joining it.
        self.events.close();
        self.worker.take();
    }
}

fn send_event(send: &mpsc::Sender<io::Result<Event>>, event: Event) -> io::Result<()> {
    send.blocking_send(Ok(event)).map_err(|_| io::ErrorKind::BrokenPipe.into())
}

fn send_resize(send: &mpsc::Sender<io::Result<Event>>, previous: &mut TerminalSize, size: TerminalSize) -> io::Result<()> {
    // A minimized terminal may transiently report zero dimensions.
    if size.rows != 0 && size.cols != 0 && size != *previous {
        send_event(send, Event::Resize(size))?;
        *previous = size;
    }
    Ok(())
}

#[cfg(unix)]
mod platform {
    use super::*;
    use std::fs::File;
    use std::io::Read;
    use std::os::fd::{AsFd, AsRawFd};

    pub struct Mode {
        terminal: File,
        saved: libc::termios,
    }

    impl Drop for Mode {
        fn drop(&mut self) {
            // SAFETY: this descriptor and termios snapshot remain valid until drop.
            while unsafe { libc::tcsetattr(self.terminal.as_raw_fd(), libc::TCSANOW, &self.saved) } == -1 {
                if io::Error::last_os_error().kind() != io::ErrorKind::Interrupted { break; }
            }
        }
    }

    pub struct Input(File);

    pub fn open() -> io::Result<(Mode, Input, TerminalSize)> {
        // Retain the actual stdin terminal, including clients without a
        // controlling /dev/tty. Raw VMIN=1 and a sole reader make poll/read bounded.
        let terminal = File::from(std::io::stdin().as_fd().try_clone_to_owned()?);
        let input = Input(terminal.try_clone()?);
        let size = input.size()?;
        let mut saved = std::mem::MaybeUninit::uninit();
        // SAFETY: tcgetattr initializes the supplied termios on success.
        if unsafe { libc::tcgetattr(terminal.as_raw_fd(), saved.as_mut_ptr()) } == -1 {
            return Err(io::Error::last_os_error());
        }
        let saved = unsafe { saved.assume_init() };
        let mode = Mode { terminal, saved };
        let mut raw = saved;
        // SAFETY: raw is an initialized termios; the terminal stays owned by Mode.
        unsafe { libc::cfmakeraw(&mut raw); }
        if unsafe { libc::tcsetattr(mode.terminal.as_raw_fd(), libc::TCSANOW, &raw) } == -1 {
            return Err(io::Error::last_os_error());
        }
        Ok((mode, input, size))
    }

    impl Input {
        fn size(&self) -> io::Result<TerminalSize> {
            let mut size = std::mem::MaybeUninit::<libc::winsize>::uninit();
            // SAFETY: ioctl initializes a winsize for this owned terminal descriptor.
            if unsafe { libc::ioctl(self.0.as_raw_fd(), libc::TIOCGWINSZ, size.as_mut_ptr()) } == -1 {
                return Err(io::Error::last_os_error());
            }
            let size = unsafe { size.assume_init() };
            Ok(TerminalSize { rows: size.ws_row, cols: size.ws_col })
        }

        pub fn run(&mut self, send: &mpsc::Sender<io::Result<Event>>, stop: &AtomicBool, mut size: TerminalSize) -> io::Result<()> {
            let mut bytes = [0; MAX_DATA];
            while !stop.load(Ordering::Acquire) {
                let mut fd = libc::pollfd { fd: self.0.as_raw_fd(), events: libc::POLLIN, revents: 0 };
                // A bounded native wait gives cancellation and resize observation
                // independent of stdin activity, without process-wide signal hooks.
                // SAFETY: one valid pollfd remains alive throughout the call.
                if unsafe { libc::poll(&mut fd, 1, 50) } == -1 {
                    let error = io::Error::last_os_error();
                    if error.kind() == io::ErrorKind::Interrupted { continue; }
                    return Err(error);
                }
                if stop.load(Ordering::Acquire) { break; }
                send_resize(send, &mut size, self.size()?)?;
                if fd.revents == 0 { continue; }
                match self.0.read(&mut bytes) {
                    Ok(0) => break,
                    Ok(count) => send_event(send, Event::Input(bytes[..count].to_vec()))?,
                    Err(error) if matches!(error.kind(), io::ErrorKind::Interrupted | io::ErrorKind::WouldBlock) => {}
                    Err(error) => return Err(error),
                }
            }
            Ok(())
        }
    }
}

#[cfg(windows)]
mod platform {
    use super::*;
    use std::fs::{File, OpenOptions};
    use std::io::Write;
    use std::os::windows::io::AsRawHandle;
    use windows_sys::Win32::{
        Foundation::{WAIT_FAILED, WAIT_OBJECT_0},
        System::{Console::*, Threading::WaitForSingleObject},
        UI::Input::KeyboardAndMouse::*,
    };

    pub struct Mode {
        input: File,
        output: File,
        input_mode: u32,
        output_mode: u32,
        output_cp: u32,
    }

    impl Drop for Mode {
        fn drop(&mut self) {
            // SAFETY: the console handles are retained until after restoration.
            unsafe {
                SetConsoleMode(self.input.as_raw_handle(), self.input_mode);
                SetConsoleMode(self.output.as_raw_handle(), self.output_mode);
                SetConsoleOutputCP(self.output_cp);
            }
        }
    }

    pub struct Input {
        input: File,
        output: File,
        encoder: KeyEncoder,
    }

    fn check(success: i32) -> io::Result<()> {
        if success == 0 { Err(io::Error::last_os_error()) } else { Ok(()) }
    }

    pub fn open() -> io::Result<(Mode, Input, TerminalSize)> {
        let input = OpenOptions::new().read(true).write(true).open("CONIN$")?;
        let output = OpenOptions::new().read(true).write(true).open("CONOUT$")?;
        let (mut input_mode, mut output_mode) = (0, 0);
        // SAFETY: these are live console handles and valid output pointers.
        unsafe {
            check(GetConsoleMode(input.as_raw_handle(), &mut input_mode))?;
            check(GetConsoleMode(output.as_raw_handle(), &mut output_mode))?;
        }
        let reader = Input { input: input.try_clone()?, output: output.try_clone()?, encoder: KeyEncoder::default() };
        let size = reader.size()?;
        let mode = Mode { input, output, input_mode, output_mode, output_cp: unsafe { GetConsoleOutputCP() } };
        // Modern conhost queues VT characters; Wine can still queue native keys.
        // The encoder handles both without an uncancellable ReadConsole wait.
        let raw = (input_mode | ENABLE_VIRTUAL_TERMINAL_INPUT | ENABLE_WINDOW_INPUT | ENABLE_EXTENDED_FLAGS)
            & !(ENABLE_LINE_INPUT | ENABLE_ECHO_INPUT | ENABLE_PROCESSED_INPUT | ENABLE_QUICK_EDIT_MODE | ENABLE_MOUSE_INPUT);
        unsafe {
            check(SetConsoleMode(mode.input.as_raw_handle(), raw))?;
            check(SetConsoleMode(mode.output.as_raw_handle(), output_mode | ENABLE_PROCESSED_OUTPUT
                | ENABLE_VIRTUAL_TERMINAL_PROCESSING | DISABLE_NEWLINE_AUTO_RETURN))?;
            check(SetConsoleOutputCP(65001))?;
        }
        Ok((mode, reader, size))
    }

    impl Input {
        fn size(&self) -> io::Result<TerminalSize> {
            let mut info = CONSOLE_SCREEN_BUFFER_INFO::default();
            // SAFETY: output is a live console handle, and info is writable.
            check(unsafe { GetConsoleScreenBufferInfo(self.output.as_raw_handle(), &mut info) })?;
            Ok(TerminalSize {
                rows: (info.srWindow.Bottom - info.srWindow.Top + 1) as u16,
                cols: (info.srWindow.Right - info.srWindow.Left + 1) as u16,
            })
        }

        pub fn run(&mut self, send: &mpsc::Sender<io::Result<Event>>, stop: &AtomicBool, mut size: TerminalSize) -> io::Result<()> {
            let mut records = [INPUT_RECORD::default(); 128];
            while !stop.load(Ordering::Acquire) {
                // SAFETY: the input handle stays alive throughout the bounded wait.
                let ready = unsafe { WaitForSingleObject(self.input.as_raw_handle(), 50) };
                if ready == WAIT_FAILED { return Err(io::Error::last_os_error()); }
                if stop.load(Ordering::Acquire) { break; }
                send_resize(send, &mut size, self.size()?)?;
                if ready != WAIT_OBJECT_0 { continue; }
                let mut count = 0;
                // We are the sole reader. Peek first so resize/key-up events can
                // never leave ReadConsoleInput waiting for a character.
                unsafe {
                    check(PeekConsoleInputW(self.input.as_raw_handle(), records.as_mut_ptr(), records.len() as u32, &mut count))?;
                    if count == 0 { continue; }
                    check(ReadConsoleInputW(self.input.as_raw_handle(), records.as_mut_ptr(), count, &mut count))?;
                }
                let mut bytes = Vec::new();
                for record in &records[..count as usize] {
                    if record.EventType != KEY_EVENT as u16 { continue; }
                    // SAFETY: EventType selects the initialized KeyEvent union arm.
                    let key = unsafe { record.Event.KeyEvent };
                    let mut encoded = [0; 16];
                    let count = self.encoder.encode(key, &mut encoded)?;
                    if count == 0 { continue; }
                    let encoded = &encoded[..count];
                    for _ in 0..key.wRepeatCount {
                        if bytes.len() + encoded.len() > MAX_DATA {
                            send_event(send, Event::Input(std::mem::take(&mut bytes)))?;
                        }
                        bytes.extend_from_slice(encoded);
                    }
                }
                if !bytes.is_empty() { send_event(send, Event::Input(bytes))?; }
            }
            Ok(())
        }
    }

    #[derive(Default)]
    struct KeyEncoder {
        high_surrogate: Option<u16>,
    }

    impl KeyEncoder {
        fn character(&mut self, unit: u16) -> io::Result<Option<char>> {
            if (0xd800..=0xdbff).contains(&unit) {
                if self.high_surrogate.replace(unit).is_some() {
                    return Err(io::Error::new(io::ErrorKind::InvalidData, "invalid console UTF-16"));
                }
                return Ok(None);
            }
            let code = match self.high_surrogate.take() {
                Some(high) if (0xdc00..=0xdfff).contains(&unit) =>
                    0x10000 + (((high - 0xd800) as u32) << 10) + (unit - 0xdc00) as u32,
                Some(_) => return Err(io::Error::new(io::ErrorKind::InvalidData, "invalid console UTF-16")),
                None => unit as u32,
            };
            char::from_u32(code).map(Some)
                .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "invalid console UTF-16"))
        }
        fn encode(&mut self, key: KEY_EVENT_RECORD, bytes: &mut [u8; 16]) -> io::Result<usize> {
            let mut vk = key.wVirtualKeyCode;
            // SAFETY: the Unicode console API initializes the UnicodeChar arm.
            let mut unit = unsafe { key.uChar.UnicodeChar };
            // Alt+numpad Unicode input can be delivered on Alt's key-up.
            if key.bKeyDown == 0 && !(vk == VK_MENU && unit != 0) { return Ok(0); }
            let state = key.dwControlKeyState;
            let ctrl = state & (LEFT_CTRL_PRESSED | RIGHT_CTRL_PRESSED) != 0;
            let alt = state & (LEFT_ALT_PRESSED | RIGHT_ALT_PRESSED) != 0;
            let mut shift = state & SHIFT_PRESSED != 0;
            if (VK_F13..=VK_F24).contains(&vk) {
                vk -= VK_F13 - VK_F1;
                shift = true;
            }
            let modifier = 1 + u8::from(shift) + 2 * u8::from(alt) + 4 * u8::from(ctrl);
            let sequence = match vk {
                VK_UP => Some((1, b'A', false)),
                VK_DOWN => Some((1, b'B', false)),
                VK_RIGHT => Some((1, b'C', false)),
                VK_LEFT => Some((1, b'D', false)),
                VK_HOME => Some((1, b'H', false)),
                VK_END => Some((1, b'F', false)),
                VK_INSERT => Some((2, b'~', false)),
                VK_DELETE => Some((3, b'~', false)),
                VK_PRIOR => Some((5, b'~', false)),
                VK_NEXT => Some((6, b'~', false)),
                VK_F1..=VK_F4 => Some((1, b'P' + (vk - VK_F1) as u8, true)),
                VK_F5..=VK_F12 => Some(([15, 17, 18, 19, 20, 21, 23, 24][(vk - VK_F5) as usize], b'~', false)),
                VK_TAB if shift => Some((1, b'Z', false)),
                _ => None,
            };
            if let Some((parameter, final_byte, ss3)) = sequence {
                let mut cursor = &mut bytes[..];
                if modifier == 1 || (vk == VK_TAB && modifier == 2) {
                    if final_byte == b'~' { write!(cursor, "\x1b[{parameter}~")?; }
                    else { cursor.write_all(&[0x1b, if ss3 { b'O' } else { b'[' }, final_byte])?; }
                } else {
                    write!(cursor, "\x1b[{parameter};{modifier}{}", final_byte as char)?;
                }
                return Ok(16 - cursor.len());
            }
            if vk == VK_BACK { unit = if ctrl { 8 } else { 127 }; }
            if vk == VK_RETURN { unit = if ctrl { 10 } else { 13 }; }
            if unit == 0 {
                if !ctrl { return Ok(0); }
                unit = match vk {
                    VK_A..=VK_Z => vk - VK_A + 1,
                    VK_SPACE | VK_2 => 0,
                    VK_OEM_4 => 27,
                    VK_OEM_5 => 28,
                    VK_OEM_6 => 29,
                    VK_6 => 30,
                    VK_OEM_MINUS => 31,
                    _ => return Ok(0),
                };
            }
            let Some(character) = self.character(unit)? else { return Ok(0); };
            // Ctrl+Alt printable text is AltGr, not a Meta-prefixed keystroke.
            let prefix = usize::from(alt && !(ctrl && unit >= 32) && vk != VK_MENU);
            if prefix != 0 { bytes[0] = 0x1b; }
            Ok(prefix + character.encode_utf8(&mut bytes[prefix..]).len())
        }
    }

    #[cfg(test)]
    mod tests {
        use super::*;

        fn key(vk: u16, unit: u16, state: u32) -> KEY_EVENT_RECORD {
            KEY_EVENT_RECORD {
                bKeyDown: 1, wRepeatCount: 1, wVirtualKeyCode: vk,
                wVirtualScanCode: 0, uChar: KEY_EVENT_RECORD_0 { UnicodeChar: unit },
                dwControlKeyState: state,
            }
        }

        #[test]
        fn native_navigation_and_function_keys_preserve_modifiers() {
            let mut encoder = KeyEncoder::default();
            let mut bytes = [0; 16];
            for (vk, state, expected) in [
                (VK_UP, 0, b"\x1b[A".as_slice()),
                (VK_LEFT, LEFT_CTRL_PRESSED, b"\x1b[1;5D"),
                (VK_DOWN, SHIFT_PRESSED, b"\x1b[1;2B"),
                (VK_RIGHT, LEFT_ALT_PRESSED, b"\x1b[1;3C"),
                (VK_HOME, 0, b"\x1b[H"),
                (VK_END, LEFT_ALT_PRESSED | LEFT_CTRL_PRESSED, b"\x1b[1;7F"),
                (VK_INSERT, 0, b"\x1b[2~"),
                (VK_DELETE, LEFT_CTRL_PRESSED, b"\x1b[3;5~"),
                (VK_PRIOR, 0, b"\x1b[5~"),
                (VK_NEXT, SHIFT_PRESSED, b"\x1b[6;2~"),
                (VK_F1, 0, b"\x1bOP"),
                (VK_F4, LEFT_CTRL_PRESSED, b"\x1b[1;5S"),
                (VK_F5, 0, b"\x1b[15~"),
                (VK_F12, 0, b"\x1b[24~"),
                (VK_F24, LEFT_ALT_PRESSED, b"\x1b[24;4~"),
                (VK_TAB, SHIFT_PRESSED, b"\x1b[Z"),
            ] {
                let count = encoder.encode(key(vk, 0, state), &mut bytes).unwrap();
                assert_eq!(&bytes[..count], expected);
            }
        }

        #[test]
        fn translated_vt_and_unicode_are_not_reencoded_as_native_keys() {
            let mut encoder = KeyEncoder::default();
            let mut bytes = [0; 16];
            for &unit in b"\x1b[1;5A" {
                let count = encoder.encode(key(0, unit as u16, 0), &mut bytes).unwrap();
                assert_eq!(&bytes[..count], &[unit]);
            }
            for (event, expected) in [
                (key(VK_C, 3, LEFT_CTRL_PRESSED), b"\x03".as_slice()),
                (key(VK_C, 0, LEFT_CTRL_PRESSED), b"\x03"),
                (key(VK_OEM_6, 0, LEFT_CTRL_PRESSED), b"\x1d"),
                (key(VK_SPACE, 0, LEFT_CTRL_PRESSED), b"\0"),
                (key(VK_X, b'x' as u16, LEFT_ALT_PRESSED), b"\x1bx"),
                (key(VK_E, 0x20ac, RIGHT_ALT_PRESSED | LEFT_CTRL_PRESSED), "€".as_bytes()),
                (key(VK_SHIFT, 0, SHIFT_PRESSED), b""),
                (key(0, 0xd83d, 0), b""),
                (key(0, 0xde00, 0), "\u{1f600}".as_bytes()),
            ] {
                let count = encoder.encode(event, &mut bytes).unwrap();
                assert_eq!(&bytes[..count], expected);
            }
        }
    }
}
