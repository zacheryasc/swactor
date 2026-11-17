use std::io;
use std::os::windows::io::{AsRawHandle, FromRawHandle, OwnedHandle};
use std::os::windows::process::{CommandExt, ProcThreadAttributeList};
use std::process::{Child, Command, Stdio};
use std::ptr;
use std::sync::Arc;
use std::time::{Duration, Instant};

use crossbeam_queue::SegQueue;
use swactor_engine::{BlockingWorkSender, OwnedIoThread};
use windows_sys::Win32::Foundation::{WAIT_FAILED, WAIT_OBJECT_0, WAIT_TIMEOUT};
use windows_sys::Win32::System::Console::{CTRL_BREAK_EVENT, GenerateConsoleCtrlEvent};
use windows_sys::Win32::System::JobObjects::{
    CreateJobObjectW, JOBOBJECT_EXTENDED_LIMIT_INFORMATION, JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE,
    JobObjectExtendedLimitInformation, SetInformationJobObject,
};
use windows_sys::Win32::System::Threading::{
    CREATE_NEW_PROCESS_GROUP, CreateEventW, INFINITE, PROC_THREAD_ATTRIBUTE_JOB_LIST,
    PROC_THREAD_ATTRIBUTE_PSEUDOCONSOLE, SetEvent,
    WaitForMultipleObjects,
};

use crate::stdin::{ProcessInput, ProcessInputWriter};
use crate::supervisor_common::{
    ThreadCommand, ThreadEvent, ThreadEventSink, join_output_readers, spawn_output_reader,
};
use crate::types::{ExitStatus, ProcessSpec};
use crate::terminal::{TerminalChild, TerminalSession, TerminalSize};

#[derive(Clone)]
struct CommandWake(Arc<OwnedHandle>);

impl CommandWake {
    fn new() -> io::Result<Self> {
        // Auto-reset, initially unsignalled, non-inheritable event.
        let handle = unsafe { CreateEventW(ptr::null(), 0, 0, ptr::null()) };
        if handle.is_null() {
            return Err(io::Error::last_os_error());
        }
        // SAFETY: CreateEventW returned a new, owned handle.
        Ok(Self(Arc::new(unsafe { OwnedHandle::from_raw_handle(handle) })))
    }

    fn wake(&self) -> io::Result<()> {
        // SAFETY: the shared owner keeps the event valid for the call.
        if unsafe { SetEvent(self.0.as_raw_handle()) } == 0 {
            return Err(io::Error::last_os_error());
        }
        Ok(())
    }
}

pub(crate) struct ProcessThreadHandle {
    commands: Arc<SegQueue<ThreadCommand>>,
    wake: CommandWake,
    join: Option<OwnedIoThread<()>>,
}

pub(crate) struct ProcessSupervisorThread;

impl ProcessSupervisorThread {
    pub(crate) fn start(
        blocking: &BlockingWorkSender,
        spec: ProcessSpec,
        input: Option<ProcessInput>,
        events: ThreadEventSink,
    ) -> Result<ProcessThreadHandle, swactor::Error> {
        let commands = Arc::new(SegQueue::new());
        let wake = CommandWake::new()
            .map_err(|error| swactor::Error::from(format!("create process command event: {error}")))?;
        let thread_commands = Arc::clone(&commands);
        let thread_wake = wake.clone();
        let io_work = blocking.clone();
        let join = blocking
            .spawn_owned_thread("process-supervisor", move || {
                let failure_events = events.clone();
                let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                    supervisor_thread_main(
                        &io_work,
                        spec,
                        input,
                        events,
                        thread_commands,
                        thread_wake,
                    );
                }));
                if result.is_err() {
                    failure_events.push(ThreadEvent::Error {
                        error: "process supervisor thread panicked".to_owned(),
                    });
                    failure_events.push(ThreadEvent::ThreadFinished);
                }
            })
            .map_err(|error| swactor::Error::from(format!("start process supervisor: {error}")))?;
        Ok(ProcessThreadHandle {
            commands,
            wake,
            join: Some(join),
        })
    }
}

impl ProcessThreadHandle {
    fn send(&self, command: ThreadCommand) -> Result<(), String> {
        self.commands.push(command);
        self.wake
            .wake()
            .map_err(|error| format!("wake process supervisor: {error}"))
    }

    pub(crate) fn stop(&self, kill_after: Option<Duration>) -> Result<(), String> {
        self.send(ThreadCommand::Stop { kill_after })
    }

    pub(crate) fn shutdown_now(&self) -> Result<(), String> {
        self.send(ThreadCommand::ShutdownNow)
    }

    pub(crate) fn join_if_finished(&mut self) -> Result<bool, String> {
        if self.join.as_ref().is_some_and(|join| !join.is_finished()) {
            return Ok(false);
        }
        if let Some(join) = self.join.take() {
            join.join()
                .map_err(|_| "process supervisor thread panicked".to_owned())?;
        }
        Ok(true)
    }
}

impl Drop for ProcessThreadHandle {
    fn drop(&mut self) {
        let _ = self.shutdown_now();
        if let Some(join) = self.join.take() {
            let _ = join.join();
        }
    }
}

enum OwnedChild {
    Piped(Child),
    Terminal(TerminalChild),
}

impl OwnedChild {
    fn id(&self) -> u32 {
        match self {
            Self::Piped(child) => child.id(),
            Self::Terminal(child) => child.id(),
        }
    }

    fn as_raw_handle(&self) -> std::os::windows::io::RawHandle {
        match self {
            Self::Piped(child) => child.as_raw_handle(),
            Self::Terminal(child) => child.handle(),
        }
    }

    fn wait(&mut self) -> io::Result<ExitStatus> {
        match self {
            Self::Piped(child) => child.wait().map(|status| {
                status.code().map(ExitStatus::Code).unwrap_or(ExitStatus::Unknown)
            }),
            Self::Terminal(child) => child.wait(),
        }
    }

    fn try_wait(&mut self) -> io::Result<Option<ExitStatus>> {
        match self {
            Self::Piped(child) => child.try_wait().map(|status| status.map(|status| {
                status.code().map(ExitStatus::Code).unwrap_or(ExitStatus::Unknown)
            })),
            Self::Terminal(child) => child.try_wait(),
        }
    }
}

// Closing the last job handle cancels the entire owned process tree. The job is
// installed during CreateProcess, before child code can run or spawn descendants.
struct ProcessOwner {
    child: OwnedChild,
    job: Option<OwnedHandle>,
    readers: Vec<OwnedIoThread<()>>,
    input_writer: Option<ProcessInputWriter>,
    terminal: Option<TerminalSession>,
}

impl ProcessOwner {
    fn spawn(spec: &ProcessSpec, piped_stdin: bool, size: Option<TerminalSize>) -> io::Result<Self> {
        let handle = unsafe { CreateJobObjectW(ptr::null(), ptr::null()) };
        if handle.is_null() {
            return Err(io::Error::last_os_error());
        }
        // SAFETY: CreateJobObjectW returned a new, owned handle.
        let job = unsafe { OwnedHandle::from_raw_handle(handle) };
        let mut limits = JOBOBJECT_EXTENDED_LIMIT_INFORMATION::default();
        limits.BasicLimitInformation.LimitFlags = JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE;
        // SAFETY: limits has the layout and size required by this information class.
        if unsafe {
            SetInformationJobObject(
                job.as_raw_handle(),
                JobObjectExtendedLimitInformation,
                (&raw const limits).cast(),
                size_of::<JOBOBJECT_EXTENDED_LIMIT_INFORMATION>() as u32,
            )
        } == 0
        {
            return Err(io::Error::last_os_error());
        }
        let jobs = [job.as_raw_handle()];
        let terminal = size.map(TerminalSession::new).transpose()?;
        let mut attributes = ProcThreadAttributeList::build()
            .attribute(PROC_THREAD_ATTRIBUTE_JOB_LIST as usize, &jobs);
        if let Some(terminal) = &terminal {
            // This attribute takes the HPCON value itself, not a pointer to it.
            // The session outlives CreateProcess and remains owned with the job.
            attributes = unsafe {
                attributes.raw_attribute(PROC_THREAD_ATTRIBUTE_PSEUDOCONSOLE as usize,
                    terminal.handle() as *const std::ffi::c_void, size_of::<isize>())
            };
        }
        let attributes = attributes.finish()?;
        let child = if terminal.is_some() {
            OwnedChild::Terminal(TerminalChild::spawn(spec, &attributes)?)
        } else {
            let mut command = Command::new(&spec.command);
            command.args(&spec.args).envs(&spec.env)
                .stdin(if piped_stdin { Stdio::piped() } else { Stdio::null() })
                .stdout(Stdio::piped())
                .stderr(Stdio::piped())
                .creation_flags(CREATE_NEW_PROCESS_GROUP);
            if let Some(directory) = &spec.working_dir {
                command.current_dir(directory);
            }
            OwnedChild::Piped(command.spawn_with_attributes(&attributes)?)
        };
        Ok(Self {
            child,
            job: Some(job),
            readers: Vec::new(),
            input_writer: None,
            terminal,
        })
    }

    fn finish(&mut self) -> io::Result<ExitStatus> {
        // The job is not inherited. Closing our handle releases descendants'
        // inherited pipes and any blocked writer, even after leader exit.
        drop(self.job.take());
        let result = self.child.wait();
        // Kill/reap before closing the console, while its reader still drains.
        // Console close then breaks blocked input and delivers output EOF.
        if let Some(mut terminal) = self.terminal.take() {
            terminal.close();
        }
        let input_result = self.input_writer.take().map_or(Ok(()), |mut writer| writer.finish());
        join_output_readers(&mut self.readers);
        result.and_then(|status| input_result.map(|()| status))
    }
}

impl Drop for ProcessOwner {
    fn drop(&mut self) {
        let _ = self.finish();
    }
}

fn supervisor_thread_main(
    blocking: &BlockingWorkSender,
    spec: ProcessSpec,
    input: Option<ProcessInput>,
    events: ThreadEventSink,
    commands: Arc<SegQueue<ThreadCommand>>,
    wake: CommandWake,
) {
    let size = input.as_ref().and_then(|input| input.terminal_size);
    let mut owner = match ProcessOwner::spawn(&spec, input.is_some(), size) {
        Ok(owner) => owner,
        Err(error) => {
            drop(input);
            events.push(ThreadEvent::SpawnFailed { error: error.to_string() });
            events.push(ThreadEvent::ThreadFinished);
            return;
        }
    };
    events.push(ThreadEvent::Started { pid: owner.child.id() });
    let result = supervise(blocking, &mut owner, input, &events, &commands, &wake);
    // Cleanup and output completion precede the terminal event, including errors.
    drop(owner);
    match result {
        Ok(status) => events.push(ThreadEvent::Exited { status }),
        Err(error) => events.push(ThreadEvent::Error { error: error.to_string() }),
    }
    events.push(ThreadEvent::ThreadFinished);
}

fn supervise(
    blocking: &BlockingWorkSender,
    owner: &mut ProcessOwner,
    input: Option<ProcessInput>,
    events: &ThreadEventSink,
    commands: &SegQueue<ThreadCommand>,
    wake: &CommandWake,
) -> io::Result<ExitStatus> {
    if let Some(terminal) = &mut owner.terminal {
        owner.readers.push(spawn_output_reader(blocking, terminal.take_reader(), events.clone(), false)?);
    }
    if let OwnedChild::Piped(child) = &mut owner.child {
        if let Some(stdout) = child.stdout.take() {
            owner.readers.push(spawn_output_reader(blocking, stdout, events.clone(), false)?);
        }
        if let Some(stderr) = child.stderr.take() {
            owner.readers.push(spawn_output_reader(blocking, stderr, events.clone(), true)?);
        }
    }
    if let Some(input) = input {
        let writer = if let Some(terminal) = &mut owner.terminal {
            input.spawn_terminal_writer(blocking, terminal.take_writer())
        } else {
            let OwnedChild::Piped(child) = &mut owner.child else {
                unreachable!("terminal child owns a terminal input endpoint");
            };
            let stdin = child.stdin.take().expect("supervisor configures piped stdin");
            input.spawn_writer(blocking, stdin)
        };
        owner.input_writer = Some(writer.map_err(|error| {
            io::Error::new(error.kind(), format!("failed to start process input writer: {error}"))
        })?);
    }
    let mut stopping = false;
    let mut deadline: Option<Instant> = None;
    loop {
        if owner.child.try_wait()?.is_some() {
            return owner.finish();
        }
        while let Some(command) = commands.pop() {
            match command {
                ThreadCommand::ShutdownNow => return owner.finish(),
                ThreadCommand::Stop { kill_after } if !stopping => {
                    // A pseudo console belongs to a different console session:
                    // GenerateConsoleCtrlEvent cannot target it from this host.
                    // Stopping a terminal hangs it up and closes its owned job.
                    if owner.terminal.is_some() {
                        return owner.finish();
                    }
                    if owner.child.try_wait()?.is_some() {
                        return owner.finish();
                    }
                    // CTRL_BREAK targets only this process group. Unlike CTRL_C,
                    // it is supported for CREATE_NEW_PROCESS_GROUP children.
                    if unsafe { GenerateConsoleCtrlEvent(CTRL_BREAK_EVENT, owner.child.id()) } == 0 {
                        return Err(io::Error::last_os_error());
                    }
                    stopping = true;
                    deadline = kill_after
                        .map(|duration| Instant::now().checked_add(duration).ok_or_else(|| {
                            io::Error::new(io::ErrorKind::InvalidInput, "process stop deadline overflow")
                        }))
                        .transpose()?;
                }
                ThreadCommand::Stop { .. } => {}
            }
        }
        if deadline.is_some_and(|deadline| deadline <= Instant::now()) {
            return owner.finish();
        }
        let timeout = deadline.map_or(INFINITE, |deadline| {
            let remaining = deadline.saturating_duration_since(Instant::now());
            remaining.as_millis()
                .saturating_add(u128::from(remaining.subsec_nanos() % 1_000_000 != 0))
                .min(u128::from(INFINITE - 1)) as u32
        });
        let handles = [wake.0.as_raw_handle(), owner.child.as_raw_handle()];
        // SAFETY: both owned handles remain valid throughout this blocking wait.
        let result = unsafe { WaitForMultipleObjects(2, handles.as_ptr(), 0, timeout) };
        if result == WAIT_FAILED {
            return Err(io::Error::last_os_error());
        }
        if result != WAIT_OBJECT_0 && result != WAIT_OBJECT_0 + 1 && result != WAIT_TIMEOUT {
            return Err(io::Error::other(format!("unexpected process wait result: {result}")));
        }
    }
}
