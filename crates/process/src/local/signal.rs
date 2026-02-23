use crate::types::Signal;

/// Map a `Signal` enum variant to the corresponding libc signal constant.
pub(crate) fn signal_to_libc(signal: Signal) -> libc::c_int {
    match signal {
        Signal::Terminate => libc::SIGTERM,
        Signal::Kill => libc::SIGKILL,
        Signal::Hangup => libc::SIGHUP,
        Signal::Interrupt => libc::SIGINT,
        Signal::Other(n) => n,
    }
}

/// Send a signal to a process by PID. Returns `Ok(())` on success.
pub(crate) fn send_signal(pid: u32, signal: Signal) -> Result<(), String> {
    let sig = signal_to_libc(signal);
    // Safety: kill() is safe to call with any pid/signal combo;
    // it returns -1 on error which we check.
    let ret = unsafe { libc::kill(pid as libc::pid_t, sig) };
    if ret == 0 {
        Ok(())
    } else {
        Err(format!(
            "kill({}, {}) failed: {}",
            pid,
            sig,
            std::io::Error::last_os_error()
        ))
    }
}
