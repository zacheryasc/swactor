use std::fs::File;
use std::io::{self, Write};
use std::os::windows::ffi::{OsStrExt, OsStringExt};
use std::os::windows::io::{AsRawHandle, FromRawHandle, OwnedHandle};
use std::os::windows::process::ProcThreadAttributeList;
use std::path::{Path, PathBuf};
use std::ptr;
use std::sync::{Arc, Mutex};

use windows_sys::Win32::Foundation::{WAIT_FAILED, WAIT_OBJECT_0, WAIT_TIMEOUT};
use windows_sys::Win32::Globalization::{CompareStringOrdinal, CSTR_EQUAL};
use windows_sys::Win32::System::Console::{
    COORD, ClosePseudoConsole, CreatePseudoConsole, HPCON, ResizePseudoConsole,
};
use windows_sys::Win32::System::Pipes::CreatePipe;
use windows_sys::Win32::System::SystemInformation::{GetSystemDirectoryW, GetWindowsDirectoryW};
use windows_sys::Win32::System::Threading::{
    CREATE_UNICODE_ENVIRONMENT, CreateProcessW, EXTENDED_STARTUPINFO_PRESENT,
    GetExitCodeProcess, INFINITE, PROCESS_INFORMATION, STARTF_USESTDHANDLES,
    STARTUPINFOEXW, WaitForSingleObject,
};

use super::TerminalSize;
use crate::types::{ExitStatus, ProcessSpec};

pub(crate) struct TerminalSession {
    console: Arc<Mutex<Option<HPCON>>>,
    input: Option<File>,
    output: Option<File>,
}

impl TerminalSession {
    pub(crate) fn new(size: TerminalSize) -> io::Result<Self> {
        size.validate()?;
        let (input_read, input_write) = pipe()?;
        let (output_read, output_write) = pipe()?;
        let mut console = 0;
        let result = unsafe {
            CreatePseudoConsole(coordinates(size), input_read.as_raw_handle(),
                output_write.as_raw_handle(), 0, &mut console)
        };
        check_hresult(result)?;
        // ConPTY retains its ends. Our duplicates must close or EOF and blocked
        // write cancellation would depend on handles outside console ownership.
        drop(input_read);
        drop(output_write);
        Ok(Self {
            console: Arc::new(Mutex::new(Some(console))),
            input: Some(input_write),
            output: Some(output_read),
        })
    }

    pub(crate) fn handle(&self) -> HPCON {
        self.console.lock().unwrap().expect("owned pseudo console")
    }

    pub(crate) fn take_reader(&mut self) -> File {
        self.output.take().expect("terminal reader taken once")
    }

    pub(crate) fn take_writer(&mut self) -> TerminalWriter {
        TerminalWriter {
            input: self.input.take().expect("terminal writer taken once"),
            console: Arc::clone(&self.console),
        }
    }

    pub(crate) fn close(&mut self) {
        let console = self.console.lock().unwrap_or_else(|error| error.into_inner()).take();
        drop(self.input.take());
        // On setup failure no reader was admitted. Closing the read end makes
        // console output fail rather than blocking a synchronous console close.
        drop(self.output.take());
        if let Some(console) = console {
            // A successfully admitted output reader continues draining while
            // this synchronous close runs; joining it first would deadlock.
            unsafe { ClosePseudoConsole(console) };
        }
    }
}

impl Drop for TerminalSession {
    fn drop(&mut self) {
        self.close();
    }
}

pub(crate) struct TerminalWriter {
    input: File,
    console: Arc<Mutex<Option<HPCON>>>,
}

impl TerminalWriter {
    pub(crate) fn resize(&mut self, size: TerminalSize) -> io::Result<()> {
        // Serialize resize with console invalidation, not with pipe I/O.
        let console = self.console.lock().unwrap_or_else(|error| error.into_inner());
        let console = console.ok_or_else(|| io::Error::new(io::ErrorKind::BrokenPipe, "terminal is closed"))?;
        check_hresult(unsafe { ResizePseudoConsole(console, coordinates(size)) })
    }
}

impl Write for TerminalWriter {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        self.input.write(bytes)
    }

    fn flush(&mut self) -> io::Result<()> {
        self.input.flush()
    }
}

fn pipe() -> io::Result<(File, File)> {
    let mut read = ptr::null_mut();
    let mut write = ptr::null_mut();
    // Null SECURITY_ATTRIBUTES creates non-inheritable handles. ConPTY receives
    // its handles explicitly; children inherit only their console attachment.
    if unsafe { CreatePipe(&mut read, &mut write, ptr::null(), 0) } == 0 {
        return Err(io::Error::last_os_error());
    }
    Ok(unsafe { (File::from_raw_handle(read), File::from_raw_handle(write)) })
}

fn coordinates(size: TerminalSize) -> COORD {
    COORD { X: size.cols as i16, Y: size.rows as i16 }
}

fn check_hresult(result: i32) -> io::Result<()> {
    if result < 0 {
        Err(io::Error::other(format!("ConPTY failed: HRESULT 0x{:08x}", result as u32)))
    } else {
        Ok(())
    }
}

/// The same managed owner holds this process and its atomically admitted Job.
/// Native creation is necessary because std::Command duplicates the host's
/// redirected standard handles, overriding ConPTY's console handles.
pub(crate) struct TerminalChild {
    process: OwnedHandle,
    pid: u32,
}

impl TerminalChild {
    pub(crate) fn spawn(spec: &ProcessSpec, attributes: &ProcThreadAttributeList<'_>) -> io::Result<Self> {
        let program = resolve_program(spec)?;
        let program = wide_nul(program.as_os_str())?;
        let mut command_line = Vec::with_capacity(spec.command.len() + spec.args.iter().map(|arg| arg.len() + 3).sum::<usize>() + 3);
        if spec.command.contains(['\0', '"']) {
            return Err(io::Error::new(io::ErrorKind::InvalidInput, "invalid terminal executable name"));
        }
        command_line.push(b'"' as u16);
        command_line.extend(spec.command.encode_utf16());
        command_line.push(b'"' as u16);
        for arg in &spec.args {
            command_line.push(b' ' as u16);
            append_argument(&mut command_line, arg)?;
        }
        command_line.push(0);
        let environment = environment_block(spec)?;
        let directory = spec.working_dir.as_ref().map(|path| wide_nul(path.as_os_str())).transpose()?;
        let mut startup: STARTUPINFOEXW = unsafe { std::mem::zeroed() };
        startup.StartupInfo.cb = size_of::<STARTUPINFOEXW>() as u32;
        // Explicit NULL values, not NUL-device handles or inherited pipes.
        // This is also required by Wine when the host itself is redirected.
        startup.StartupInfo.dwFlags = STARTF_USESTDHANDLES;
        startup.lpAttributeList = attributes.as_ptr().cast_mut().cast();
        let mut info: PROCESS_INFORMATION = unsafe { std::mem::zeroed() };
        let created = unsafe {
            CreateProcessW(program.as_ptr(), command_line.as_mut_ptr(), ptr::null(), ptr::null(), 0,
                CREATE_UNICODE_ENVIRONMENT | EXTENDED_STARTUPINFO_PRESENT,
                environment.as_ref().map_or(ptr::null(), |block| block.as_ptr().cast()),
                directory.as_ref().map_or(ptr::null(), |path| path.as_ptr()),
                &startup.StartupInfo, &mut info)
        };
        if created == 0 { return Err(io::Error::last_os_error()); }
        // The job attribute takes effect before any child instruction runs.
        let process = unsafe { OwnedHandle::from_raw_handle(info.hProcess) };
        drop(unsafe { OwnedHandle::from_raw_handle(info.hThread) });
        Ok(Self { process, pid: info.dwProcessId })
    }

    pub(crate) fn id(&self) -> u32 { self.pid }

    pub(crate) fn handle(&self) -> std::os::windows::io::RawHandle {
        self.process.as_raw_handle()
    }

    pub(crate) fn try_wait(&self) -> io::Result<Option<ExitStatus>> {
        self.wait_for(0)
    }

    pub(crate) fn wait(&self) -> io::Result<ExitStatus> {
        self.wait_for(INFINITE)?.ok_or_else(|| io::Error::other("infinite process wait timed out"))
    }

    fn wait_for(&self, timeout: u32) -> io::Result<Option<ExitStatus>> {
        match unsafe { WaitForSingleObject(self.handle(), timeout) } {
            WAIT_OBJECT_0 => {
                let mut code = 0;
                if unsafe { GetExitCodeProcess(self.handle(), &mut code) } == 0 {
                    return Err(io::Error::last_os_error());
                }
                Ok(Some(ExitStatus::Code(code as i32)))
            }
            WAIT_TIMEOUT => Ok(None),
            WAIT_FAILED => Err(io::Error::last_os_error()),
            result => Err(io::Error::other(format!("unexpected terminal process wait: {result}"))),
        }
    }
}

fn wide_nul(value: &std::ffi::OsStr) -> io::Result<Vec<u16>> {
    let mut wide: Vec<_> = value.encode_wide().collect();
    if wide.contains(&0) {
        return Err(io::Error::new(io::ErrorKind::InvalidInput, "process string contains NUL"));
    }
    wide.push(0);
    Ok(wide)
}

fn append_argument(command: &mut Vec<u16>, arg: &str) -> io::Result<()> {
    if arg.contains('\0') {
        return Err(io::Error::new(io::ErrorKind::InvalidInput, "process argument contains NUL"));
    }
    let quote = arg.is_empty() || arg.contains([' ', '\t', '"']);
    if !quote {
        command.extend(arg.encode_utf16());
        return Ok(());
    }
    command.push(b'"' as u16);
    let mut slashes = 0;
    for unit in arg.encode_utf16() {
        if unit == b'\\' as u16 {
            slashes += 1;
        } else {
            command.extend(std::iter::repeat_n(b'\\' as u16, if unit == b'"' as u16 { slashes * 2 + 1 } else { slashes }));
            command.push(unit);
            slashes = 0;
        }
    }
    command.extend(std::iter::repeat_n(b'\\' as u16, slashes * 2));
    command.push(b'"' as u16);
    Ok(())
}

fn environment_block(spec: &ProcessSpec) -> io::Result<Option<Vec<u16>>> {
    if spec.env.is_empty() { return Ok(None); }
    let mut entries: Vec<(Vec<u16>, Vec<u16>)> = std::env::vars_os()
        .map(|(key, value)| (key.encode_wide().collect(), value.encode_wide().collect())).collect();
    for (key, value) in &spec.env {
        if key.is_empty() || key.contains(['=', '\0']) || value.contains('\0') {
            return Err(io::Error::new(io::ErrorKind::InvalidInput, "invalid process environment entry"));
        }
        let key: Vec<u16> = key.encode_utf16().collect();
        let value: Vec<u16> = value.encode_utf16().collect();
        if let Some(entry) = entries.iter_mut().find(|(name, _)| compare_names(name, &key) == CSTR_EQUAL) {
            *entry = (key, value);
        } else {
            entries.push((key, value));
        }
    }
    entries.sort_unstable_by(|(left, _), (right, _)| compare_names(left, right).cmp(&CSTR_EQUAL));
    let mut block = Vec::with_capacity(entries.iter().map(|(key, value)| key.len() + value.len() + 2).sum::<usize>() + 1);
    for (key, value) in entries {
        block.extend(key);
        block.push(b'=' as u16);
        block.extend(value);
        block.push(0);
    }
    block.push(0);
    Ok(Some(block))
}

fn compare_names(left: &[u16], right: &[u16]) -> i32 {
    unsafe { CompareStringOrdinal(left.as_ptr(), left.len() as i32, right.as_ptr(), right.len() as i32, 1) }
}

fn resolve_program(spec: &ProcessSpec) -> io::Result<PathBuf> {
    let path = Path::new(&spec.command);
    if spec.command.is_empty() || spec.command.ends_with(['/', '\\']) || spec.command.contains('\0') {
        return Err(io::Error::new(io::ErrorKind::InvalidInput, "terminal executable has no valid filename"));
    }
    if path.components().count() != 1 || spec.command.contains(['/', '\\', ':']) {
        let absolute = std::path::absolute(path)?;
        if !spec.command.to_ascii_lowercase().ends_with(".exe") {
            let mut with_extension = absolute.as_os_str().to_os_string();
            with_extension.push(".exe");
            if Path::new(&with_extension).is_file() { return Ok(with_extension.into()); }
        }
        return Ok(absolute);
    }
    let mut filename = path.as_os_str().to_os_string();
    if !spec.command.contains('.') { filename.push(".exe"); }
    let find = |directory: PathBuf| {
        let candidate = directory.join(&filename);
        candidate.is_file().then(|| std::path::absolute(candidate)).transpose()
    };
    // Match Command's executable search order, notably omitting an implicit
    // current-directory search and honoring the child's PATH override first.
    if let Some((_, paths)) = spec.env.iter().find(|(key, _)| key.eq_ignore_ascii_case("PATH")) {
        for directory in std::env::split_paths(paths).filter(|path| !path.as_os_str().is_empty()) {
            if let Some(program) = find(directory)? { return Ok(program); }
        }
    }
    if let Ok(executable) = std::env::current_exe() {
        if let Some(directory) = executable.parent() {
            if let Some(program) = find(directory.to_owned())? { return Ok(program); }
        }
    }
    for get_directory in [GetSystemDirectoryW, GetWindowsDirectoryW] {
        let mut buffer = [0u16; 260];
        let length = unsafe { get_directory(buffer.as_mut_ptr(), buffer.len() as u32) };
        let directory = if length == 0 {
            return Err(io::Error::last_os_error());
        } else if length < buffer.len() as u32 {
            PathBuf::from(std::ffi::OsString::from_wide(&buffer[..length as usize]))
        } else {
            let mut buffer = vec![0u16; length as usize + 1];
            let length = unsafe { get_directory(buffer.as_mut_ptr(), buffer.len() as u32) };
            if length == 0 || length as usize >= buffer.len() { return Err(io::Error::last_os_error()); }
            PathBuf::from(std::ffi::OsString::from_wide(&buffer[..length as usize]))
        };
        if let Some(program) = find(directory)? { return Ok(program); }
    }
    if let Some(paths) = std::env::var_os("PATH") {
        for directory in std::env::split_paths(&paths).filter(|path| !path.as_os_str().is_empty()) {
            if let Some(program) = find(directory)? { return Ok(program); }
        }
    }
    Err(io::Error::new(io::ErrorKind::NotFound, "terminal executable not found"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn terminal_arguments_preserve_windows_quoting() {
        for (argument, expected) in [
            ("", "\"\""),
            ("plain", "plain"),
            ("a b", "\"a b\""),
            ("a\"b", "\"a\\\"b\""),
            ("a b\\", "\"a b\\\\\""),
            ("café 日本語", "\"café 日本語\""),
        ] {
            let mut command = Vec::new();
            append_argument(&mut command, argument).unwrap();
            assert_eq!(String::from_utf16(&command).unwrap(), expected);
        }
        assert_eq!(append_argument(&mut Vec::new(), "bad\0arg").unwrap_err().kind(), io::ErrorKind::InvalidInput);
    }
}
