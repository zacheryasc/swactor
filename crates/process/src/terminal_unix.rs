use std::fs::File;
use std::io::{self, Read, Write};
use std::os::fd::{AsRawFd, FromRawFd, OwnedFd};
use std::os::unix::net::UnixStream;
use std::os::unix::process::CommandExt;
use std::process::{Command, Stdio};

use super::TerminalSize;

pub(crate) struct TerminalSession {
    master: File,
    slave: File,
}

impl TerminalSession {
    pub(crate) fn new(size: TerminalSize) -> io::Result<Self> {
        size.validate()?;
        // Atomic CLOEXEC prevents parallel process creation from inheriting PTYs.
        let fd = unsafe {
            libc::posix_openpt(libc::O_RDWR | libc::O_NOCTTY | libc::O_CLOEXEC | libc::O_NONBLOCK)
        };
        if fd < 0 {
            return Err(io::Error::last_os_error());
        }
        let master = unsafe { File::from_raw_fd(fd) };
        if unsafe { libc::grantpt(fd) } < 0 || unsafe { libc::unlockpt(fd) } < 0 {
            return Err(io::Error::last_os_error());
        }
        #[cfg(target_os = "linux")]
        let slave_fd = unsafe {
            libc::ioctl(fd, libc::TIOCGPTPEER, libc::O_RDWR | libc::O_NOCTTY | libc::O_CLOEXEC)
        };
        #[cfg(not(target_os = "linux"))]
        let slave_fd = {
            let mut name = [0 as libc::c_char; 1024];
            let result = unsafe { libc::ptsname_r(fd, name.as_mut_ptr(), name.len()) };
            if result != 0 {
                return Err(io::Error::from_raw_os_error(result));
            }
            unsafe { libc::open(name.as_ptr(), libc::O_RDWR | libc::O_NOCTTY | libc::O_CLOEXEC) }
        };
        if slave_fd < 0 {
            return Err(io::Error::last_os_error());
        }
        let slave = unsafe { File::from_raw_fd(slave_fd) };
        resize(&master, size)?;
        Ok(Self { master, slave })
    }

    pub(crate) fn configure_command(&self, command: &mut Command) -> io::Result<()> {
        command.stdin(Stdio::from(self.slave.try_clone()?))
            .stdout(Stdio::from(self.slave.try_clone()?))
            .stderr(Stdio::from(self.slave.try_clone()?));
        // Only async-signal-safe syscalls are used between fork and exec. Do not
        // also use Command::process_group: its setpgid would make setsid fail.
        unsafe {
            command.pre_exec(|| {
                if libc::setsid() < 0 || libc::ioctl(0, libc::TIOCSCTTY, 0) < 0 {
                    return Err(io::Error::last_os_error());
                }
                if libc::tcsetpgrp(0, libc::getpid()) < 0 {
                    return Err(io::Error::last_os_error());
                }
                Ok(())
            });
        }
        Ok(())
    }

    pub(crate) fn into_io(self) -> io::Result<(TerminalReader, TerminalWriter)> {
        let (wake, cancel) = UnixStream::pair()?;
        let writer = TerminalWriter {
            master: self.master.try_clone()?,
            wake: wake.into(),
            cancel: Some(cancel.into()),
        };
        // The duplicated master shares O_NONBLOCK. Each worker polls its own
        // readiness without retaining a kernel-blocked PTY read/write.
        drop(self.slave);
        Ok((TerminalReader(self.master), writer))
    }
}

pub(crate) struct TerminalReader(File);

impl Read for TerminalReader {
    fn read(&mut self, bytes: &mut [u8]) -> io::Result<usize> {
        loop {
            match self.0.read(bytes) {
                Err(error) if error.kind() == io::ErrorKind::WouldBlock => {
                    let mut descriptors = [libc::pollfd {
                        fd: self.0.as_raw_fd(), events: libc::POLLIN, revents: 0,
                    }];
                    wait_ready(&mut descriptors)?;
                }
                // Linux reports the last slave closing as EIO on the master.
                Err(error) if error.raw_os_error() == Some(libc::EIO) => return Ok(0),
                result => return result,
            }
        }
    }
}

pub(crate) struct TerminalWriter {
    master: File,
    wake: OwnedFd,
    cancel: Option<OwnedFd>,
}

impl TerminalWriter {
    pub(crate) fn take_cancellation(&mut self) -> OwnedFd {
        self.cancel.take().expect("terminal writer cancellation taken once")
    }

    pub(crate) fn resize(&mut self, size: TerminalSize) -> io::Result<()> {
        resize(&self.master, size).map_err(terminal_error)
    }
}

impl Write for TerminalWriter {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        loop {
            match self.master.write(bytes) {
                Err(error) if error.kind() == io::ErrorKind::WouldBlock => {
                    let mut descriptors = [
                        libc::pollfd { fd: self.master.as_raw_fd(), events: libc::POLLOUT, revents: 0 },
                        libc::pollfd { fd: self.wake.as_raw_fd(), events: libc::POLLIN, revents: 0 },
                    ];
                    wait_ready(&mut descriptors)?;
                    if descriptors[1].revents != 0 {
                        return Err(io::Error::new(io::ErrorKind::BrokenPipe, "terminal input is closed"));
                    }
                }
                result => return result.map_err(terminal_error),
            }
        }
    }

    fn flush(&mut self) -> io::Result<()> {
        self.master.flush()
    }
}

fn wait_ready(descriptors: &mut [libc::pollfd]) -> io::Result<()> {
    loop {
        if unsafe { libc::poll(descriptors.as_mut_ptr(), descriptors.len() as libc::nfds_t, -1) } >= 0 {
            return Ok(());
        }
        let error = io::Error::last_os_error();
        if error.kind() != io::ErrorKind::Interrupted {
            return Err(error);
        }
    }
}

fn resize(master: &File, size: TerminalSize) -> io::Result<()> {
    let window = libc::winsize {
        ws_row: size.rows,
        ws_col: size.cols,
        ws_xpixel: 0,
        ws_ypixel: 0,
    };
    if unsafe { libc::ioctl(master.as_raw_fd(), libc::TIOCSWINSZ, &window) } < 0 {
        return Err(io::Error::last_os_error());
    }
    Ok(())
}

fn terminal_error(error: io::Error) -> io::Error {
    // A PTY master reports EIO after its last slave closes, unlike a pipe's
    // EPIPE. Expose the same closed-input contract for both native transports.
    if error.raw_os_error() == Some(libc::EIO) {
        io::Error::new(io::ErrorKind::BrokenPipe, error)
    } else {
        error
    }
}

/// Interactive shells create a distinct process group for every job. The
/// unreaped session leader pins the session id until every such job is killed.
#[cfg(target_os = "linux")]
pub(crate) fn cancel_session(session: u32) -> io::Result<()> {
    loop {
        let mut signalled = false;
        for entry in std::fs::read_dir("/proc")? {
            let entry = entry?;
            let Some(pid) = entry.file_name().to_str().and_then(|name| name.parse::<u32>().ok()) else {
                continue;
            };
            if pid == session || unsafe { libc::getsid(pid as libc::pid_t) } != session as libc::pid_t {
                continue;
            }
            let fd = unsafe { libc::syscall(libc::SYS_pidfd_open, pid, 0) };
            if fd < 0 {
                let error = io::Error::last_os_error();
                if error.raw_os_error() == Some(libc::ESRCH) {
                    continue;
                }
                return Err(error);
            }
            let identity = unsafe { OwnedFd::from_raw_fd(fd as i32) };
            // Recheck membership after retaining identity; never kill a process
            // whose PID was reused outside the owned session during the scan.
            if unsafe { libc::getsid(pid as libc::pid_t) } != session as libc::pid_t {
                continue;
            }
            let mut poll = libc::pollfd { fd: identity.as_raw_fd(), events: libc::POLLIN, revents: 0 };
            loop {
                let result = unsafe { libc::poll(&mut poll, 1, 0) };
                if result > 0 { break; }
                if result == 0 { break; }
                let error = io::Error::last_os_error();
                if error.kind() != io::ErrorKind::Interrupted { return Err(error); }
            }
            if poll.revents != 0 { continue; }
            let result = unsafe {
                libc::syscall(libc::SYS_pidfd_send_signal, identity.as_raw_fd(), libc::SIGKILL,
                    std::ptr::null::<libc::siginfo_t>(), 0)
            };
            if result < 0 {
                let error = io::Error::last_os_error();
                if error.raw_os_error() == Some(libc::ESRCH) {
                    continue;
                }
                return Err(error);
            }
            // Wait for this exact process to release its descriptors; zombies
            // are already readable and do not keep the terminal open.
            loop {
                let result = unsafe { libc::poll(&mut poll, 1, -1) };
                if result >= 0 { break; }
                let error = io::Error::last_os_error();
                if error.kind() != io::ErrorKind::Interrupted { return Err(error); }
            }
            // A live member may have forked before receiving SIGKILL; rescan.
            signalled = true;
        }
        if !signalled { return Ok(()); }
    }
}

