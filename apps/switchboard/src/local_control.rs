//! Private, per-user transport between the foreground shell client and operator daemon.

#[cfg(unix)]
mod platform {
    use std::fs::{self, File, OpenOptions};
    use std::io;
    use std::os::fd::AsRawFd;
    use std::os::unix::fs::{DirBuilderExt, FileTypeExt, MetadataExt, OpenOptionsExt, PermissionsExt};
    use std::path::{Path, PathBuf};

    use anyhow::{Context, Result, bail, ensure};
    #[cfg(not(test))]
    use iroh::PublicKey;
    use tokio::net::{UnixListener, UnixStream};

    #[cfg(not(test))]
    pub type LocalStream = UnixStream;

    pub struct Listener {
        // Retain the live socket even in tests that only exercise endpoint ownership.
        _listener: UnixListener,
        path: PathBuf,
        device: u64,
        inode: u64,
        // Keep the lock file itself: unlinking a lock permits two independent locks.
        _lock: File,
    }

    #[cfg(not(test))]
    impl Listener {
        pub async fn accept(&mut self) -> Result<LocalStream> {
            let (stream, _) = self._listener.accept().await.context("accept local terminal client")?;
            check_peer(&stream)?;
            Ok(stream)
        }
    }

    impl Drop for Listener {
        fn drop(&mut self) {
            if let Ok(metadata) = fs::symlink_metadata(&self.path) {
                if metadata.dev() == self.device && metadata.ino() == self.inode {
                    let _ = fs::remove_file(&self.path);
                }
            }
        }
    }

    fn user_id() -> u32 {
        // SAFETY: geteuid has no arguments or failure mode.
        unsafe { libc::geteuid() }
    }

    fn check_directory(path: &Path, private: bool) -> Result<()> {
        let metadata = fs::symlink_metadata(path)
            .with_context(|| format!("inspect local control directory {}", path.display()))?;
        ensure!(metadata.is_dir() && !metadata.file_type().is_symlink(),
            "local control directory {} must be a real directory", path.display());
        ensure!(metadata.uid() == user_id(),
            "local control directory {} is not owned by the current user", path.display());
        let forbidden = if private { 0o077 } else { 0o022 };
        ensure!(metadata.mode() & forbidden == 0,
            "local control directory {} has unsafe permissions (expected {})",
            path.display(), if private { "0700" } else { "no group/other write access" });
        ensure!(metadata.mode() & 0o700 == 0o700,
            "local control directory {} requires owner read/write/search access", path.display());
        Ok(())
    }

    #[cfg(not(test))]
    fn endpoint(operator: PublicKey, create: bool) -> Result<PathBuf> {
        let state = crate::default_state()?;
        if create {
            fs::create_dir_all(&state)
                .with_context(|| format!("create application data directory {}", state.display()))?;
        }
        check_directory(&state, false)?;
        let directory = state.join("control");
        if create {
            match fs::DirBuilder::new().mode(0o700).create(&directory) {
                Ok(()) => {}
                Err(error) if error.kind() == io::ErrorKind::AlreadyExists => {}
                Err(error) => return Err(error).context("create private local control directory"),
            }
        }
        check_directory(&directory, true)?;
        Ok(directory.join(format!("terminal-{}.sock", &operator.to_string()[..16])))
    }

    fn check_socket(path: &Path) -> Result<fs::Metadata> {
        let metadata = fs::symlink_metadata(path).context("inspect local terminal socket")?;
        ensure!(metadata.file_type().is_socket() && metadata.uid() == user_id(),
            "refusing non-socket or foreign-owned local endpoint {}", path.display());
        ensure!(metadata.mode() & 0o777 == 0o600,
            "local terminal socket {} must have permissions 0600", path.display());
        Ok(metadata)
    }

    #[cfg(not(test))]
    fn check_peer(stream: &UnixStream) -> Result<()> {
        ensure!(stream.peer_cred().context("authenticate local terminal peer")?.uid() == user_id(),
            "local terminal peer is not the current user");
        Ok(())
    }

    #[cfg(not(test))]
    pub async fn bind(operator: PublicKey) -> Result<Listener> {
        bind_at(endpoint(operator, true)?).await
    }

    async fn bind_at(path: PathBuf) -> Result<Listener> {
        let lock = OpenOptions::new().read(true).write(true).create(true)
            .truncate(false).mode(0o600).custom_flags(libc::O_NOFOLLOW | libc::O_CLOEXEC)
            .open(path.with_extension("lock")).context("open local terminal endpoint lock")?;
        let metadata = lock.metadata().context("inspect local terminal endpoint lock")?;
        ensure!(metadata.is_file() && metadata.uid() == user_id()
                && metadata.mode() & 0o777 == 0o600 && metadata.nlink() == 1,
            "local terminal endpoint lock must be a private, user-owned regular file");
        // SAFETY: lock owns a valid descriptor for the entire Listener lifetime.
        if unsafe { libc::flock(lock.as_raw_fd(), libc::LOCK_EX | libc::LOCK_NB) } != 0 {
            return Err(io::Error::last_os_error()).context("operator terminal daemon is already running (endpoint locked)");
        }
        match fs::symlink_metadata(&path) {
            Ok(_) => {
                let stale = check_socket(&path)?;
                match UnixStream::connect(&path).await {
                    Ok(_) => bail!("operator terminal daemon is already listening at {}", path.display()),
                    Err(error) if matches!(error.raw_os_error(), Some(libc::ECONNREFUSED) | Some(libc::ENOENT)) => {}
                    Err(error) => return Err(error).context("check existing local terminal daemon"),
                }
                match fs::symlink_metadata(&path) {
                    Ok(current) => {
                        ensure!(current.dev() == stale.dev() && current.ino() == stale.ino(),
                            "local terminal endpoint changed while checking for a stale socket");
                        fs::remove_file(&path).context("remove stale local terminal socket")?;
                    }
                    Err(error) if error.kind() == io::ErrorKind::NotFound => {}
                    Err(error) => return Err(error).context("recheck stale local terminal socket"),
                }
            }
            Err(error) if error.kind() == io::ErrorKind::NotFound => {}
            Err(error) => return Err(error).context("inspect local terminal endpoint"),
        }
        // The containing directory is already 0700, so the socket is inaccessible
        // to other users even between bind and chmod.
        let listener = UnixListener::bind(&path)
            .with_context(|| format!("bind local terminal socket {}", path.display()))?;
        fs::set_permissions(&path, fs::Permissions::from_mode(0o600))
            .context("make local terminal socket private")?;
        let metadata = check_socket(&path)?;
        Ok(Listener { _listener: listener, path, device: metadata.dev(), inode: metadata.ino(), _lock: lock })
    }

    #[cfg(not(test))]
    pub async fn connect(operator: PublicKey) -> Result<LocalStream> {
        let path = endpoint(operator, false)
            .context("locate operator daemon; start the operator node before connecting")?;
        check_socket(&path)?;
        let stream = UnixStream::connect(&path).await
            .context("connect to operator daemon; start the operator node before connecting")?;
        check_peer(&stream)?;
        Ok(stream)
    }

    #[cfg(test)]
    mod tests {
        use super::*;
        use std::sync::atomic::{AtomicU64, Ordering};

        struct Directory(PathBuf);
        impl Directory {
            fn new() -> Self {
                static SEQUENCE: AtomicU64 = AtomicU64::new(0);
                let path = std::env::temp_dir().join(format!("switchboard-control-{}-{}",
                    std::process::id(), SEQUENCE.fetch_add(1, Ordering::Relaxed)));
                fs::DirBuilder::new().mode(0o700).create(&path).unwrap();
                Self(path)
            }
            fn socket(&self) -> PathBuf { self.0.join("terminal.sock") }
        }
        impl Drop for Directory {
            fn drop(&mut self) { let _ = fs::remove_dir_all(&self.0); }
        }

        #[test]
        fn live_socket_is_never_unlinked() {
            engine_test!({
            let directory = Directory::new();
            let path = directory.socket();
            let listener = UnixListener::bind(&path).unwrap();
            fs::set_permissions(&path, fs::Permissions::from_mode(0o600)).unwrap();
            let inode = fs::symlink_metadata(&path).unwrap().ino();
            assert!(bind_at(path.clone()).await.is_err());
            assert_eq!(fs::symlink_metadata(&path).unwrap().ino(), inode);
            assert!(UnixStream::connect(&path).await.is_ok());
            drop(listener);
            });
        }

        #[test]
        fn stale_socket_is_replaced_and_owned_socket_is_removed() {
            engine_test!({
            let directory = Directory::new();
            let path = directory.socket();
            drop(std::os::unix::net::UnixListener::bind(&path).unwrap());
            fs::set_permissions(&path, fs::Permissions::from_mode(0o600)).unwrap();
            let listener = bind_at(path.clone()).await.unwrap();
            assert_eq!(fs::symlink_metadata(&path).unwrap().mode() & 0o777, 0o600);
            assert!(bind_at(path.clone()).await.is_err());
            drop(listener);
            assert!(!path.exists());
            assert!(path.with_extension("lock").exists());
            });
        }

        #[test]
        fn unsafe_paths_are_rejected_without_deleting_them() {
            engine_test!({
            let directory = Directory::new();
            let path = directory.socket();
            let file = directory.0.join("valuable");
            fs::write(&file, b"keep").unwrap();
            std::os::unix::fs::symlink(&file, &path).unwrap();
            assert!(bind_at(path.clone()).await.is_err());
            assert!(fs::symlink_metadata(&path).unwrap().file_type().is_symlink());
            assert_eq!(fs::read(&file).unwrap(), b"keep");
            fs::set_permissions(&directory.0, fs::Permissions::from_mode(0o770)).unwrap();
            assert!(check_directory(&directory.0, true).is_err());
            });
        }
    }
}

#[cfg(windows)]
mod platform {
    use std::ffi::c_void;
    use std::io;
    use std::os::windows::io::{AsRawHandle, FromRawHandle, OwnedHandle};
    use std::pin::Pin;
    use std::ptr;
    use std::task::{Context as TaskContext, Poll};
    use std::time::Duration;

    use anyhow::{Context, Result, ensure};
    use iroh::PublicKey;
    use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};
    use tokio::net::windows::named_pipe::{ClientOptions, NamedPipeClient, NamedPipeServer, ServerOptions};
    use windows_sys::Win32::Foundation::{ERROR_INSUFFICIENT_BUFFER, ERROR_PIPE_BUSY, HANDLE, LocalFree};
    use windows_sys::Win32::Security::{GetTokenInformation, SECURITY_ATTRIBUTES, TOKEN_QUERY, TOKEN_USER, TokenUser};
    use windows_sys::Win32::Security::Authorization::{ConvertSidToStringSidW, ConvertStringSecurityDescriptorToSecurityDescriptorW};
    use windows_sys::Win32::System::Pipes::GetNamedPipeServerProcessId;
    use windows_sys::Win32::System::Threading::{GetCurrentProcess, OpenProcess, OpenProcessToken, PROCESS_QUERY_LIMITED_INFORMATION};

    struct LocalAllocation(*mut c_void);
    // SAFETY: the allocation is exclusively owned and only read after construction.
    unsafe impl Send for LocalAllocation {}
    impl Drop for LocalAllocation {
        fn drop(&mut self) {
            // SAFETY: both SID conversion APIs return LocalAlloc-owned storage.
            unsafe { LocalFree(self.0); }
        }
    }

    fn process_user(process: HANDLE) -> Result<String> {
        let mut token = ptr::null_mut();
        // SAFETY: process is a valid borrowed process handle, token is an out pointer.
        ensure!(unsafe { OpenProcessToken(process, TOKEN_QUERY, &mut token) } != 0,
            "open local terminal peer token: {}", io::Error::last_os_error());
        // SAFETY: OpenProcessToken returned a new owned handle.
        let token = unsafe { OwnedHandle::from_raw_handle(token) };
        let mut length = 0;
        // SAFETY: null buffer queries the required token information size.
        unsafe { GetTokenInformation(token.as_raw_handle(), TokenUser, ptr::null_mut(), 0, &mut length); }
        ensure!(io::Error::last_os_error().raw_os_error() == Some(ERROR_INSUFFICIENT_BUFFER as i32)
                && length as usize >= std::mem::size_of::<TOKEN_USER>(),
            "query local terminal peer token size: {}", io::Error::last_os_error());
        // usize storage guarantees the alignment required by TOKEN_USER.
        let mut buffer = vec![0usize; (length as usize).div_ceil(std::mem::size_of::<usize>())];
        // SAFETY: buffer is aligned and covers the size returned by the preceding query.
        ensure!(unsafe { GetTokenInformation(token.as_raw_handle(), TokenUser,
            buffer.as_mut_ptr().cast(), length, &mut length) } != 0,
            "read local terminal peer token: {}", io::Error::last_os_error());
        // SAFETY: successful TokenUser query initializes TOKEN_USER and its SID.
        let user = unsafe { &*buffer.as_ptr().cast::<TOKEN_USER>() };
        let mut text = ptr::null_mut();
        // SAFETY: user.User.Sid remains valid while buffer is alive.
        ensure!(unsafe { ConvertSidToStringSidW(user.User.Sid, &mut text) } != 0,
            "format local terminal user SID: {}", io::Error::last_os_error());
        let allocation = LocalAllocation(text.cast());
        let mut length = 0;
        // SAFETY: the conversion API returns a null-terminated UTF-16 string.
        unsafe { while *text.add(length) != 0 { length += 1; } }
        // SAFETY: the terminator search established the initialized slice length.
        let sid = String::from_utf16(unsafe { std::slice::from_raw_parts(text, length) })
            .context("decode local terminal user SID")?;
        drop(allocation);
        Ok(sid)
    }

    fn current_user() -> Result<String> {
        // SAFETY: GetCurrentProcess returns a valid borrowed pseudo-handle.
        process_user(unsafe { GetCurrentProcess() })
    }

    fn endpoint(operator: PublicKey, user: &str) -> Result<String> {
        // Include the same default per-user state location as Unix, but keep the
        // pipe name short and independent of the daemon's --state override.
        let state = crate::default_state()?.to_string_lossy().to_lowercase();
        let hash = state.as_bytes().iter().fold(0xcbf29ce484222325u64,
            |hash, byte| (hash ^ u64::from(*byte)).wrapping_mul(0x100000001b3));
        Ok(format!(r"\\.\pipe\switchboard-{user}-{hash:016x}-{}", &operator.to_string()[..16]))
    }

    fn security(user: &str) -> Result<LocalAllocation> {
        // Protected DACL: only this user, not Everyone/Authenticated Users or
        // even an inherited administrator group, may open a client/server handle.
        let sddl: Vec<u16> = format!("D:P(A;;GA;;;{user})").encode_utf16().chain(Some(0)).collect();
        let mut descriptor = ptr::null_mut();
        // SAFETY: sddl is terminated and descriptor is a valid out pointer.
        ensure!(unsafe { ConvertStringSecurityDescriptorToSecurityDescriptorW(
            sddl.as_ptr(), 1, &mut descriptor, ptr::null_mut()) } != 0,
            "create private local terminal pipe security: {}", io::Error::last_os_error());
        Ok(LocalAllocation(descriptor))
    }

    fn create_pipe(name: &str, descriptor: &LocalAllocation, first: bool) -> Result<NamedPipeServer> {
        let mut attributes = SECURITY_ATTRIBUTES {
            nLength: std::mem::size_of::<SECURITY_ATTRIBUTES>() as u32,
            lpSecurityDescriptor: descriptor.0,
            bInheritHandle: 0,
        };
        let mut options = ServerOptions::new();
        options.first_pipe_instance(first).reject_remote_clients(true);
        // SAFETY: attributes and its descriptor remain valid for the synchronous create call.
        unsafe { options.create_with_security_attributes_raw(name,
            (&mut attributes as *mut SECURITY_ATTRIBUTES).cast()) }
            .context("bind private local terminal pipe (is the operator daemon already running?)")
    }

    pub struct Listener {
        name: String,
        security: LocalAllocation,
        next: NamedPipeServer,
    }

    impl Listener {
        pub async fn accept(&mut self) -> Result<LocalStream> {
            self.next.connect().await.context("accept local terminal pipe client")?;
            // Reserve the next instance before returning this one, so an empty
            // namespace can never be hijacked between two accepted clients.
            let next = create_pipe(&self.name, &self.security, false)?;
            Ok(LocalStream::Server(std::mem::replace(&mut self.next, next)))
        }
    }

    pub async fn bind(operator: PublicKey) -> Result<Listener> {
        let user = current_user()?;
        let name = endpoint(operator, &user)?;
        let security = security(&user)?;
        let next = create_pipe(&name, &security, true)?;
        Ok(Listener { name, security, next })
    }

    pub async fn connect(operator: PublicKey) -> Result<LocalStream> {
        let user = current_user()?;
        let name = endpoint(operator, &user)?;
        let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
        let client = loop {
            match ClientOptions::new().open(&name) {
                Ok(client) => break client,
                Err(error) if error.raw_os_error() == Some(ERROR_PIPE_BUSY as i32)
                    && tokio::time::Instant::now() < deadline => {
                    // Named pipes briefly have no unconnected instance while
                    // accept installs its replacement; no worker blocks on stdin.
                    tokio::time::sleep(Duration::from_millis(20)).await;
                }
                Err(error) => return Err(error).context(
                    "connect to operator daemon; start the operator node before connecting"),
            }
        };
        // Pipe names are public. Do not trust a different user who pre-created
        // our name, even if that server offers a permissive DACL.
        let mut process_id = 0;
        // SAFETY: client owns a valid local pipe handle and process_id is writable.
        ensure!(unsafe { GetNamedPipeServerProcessId(client.as_raw_handle(), &mut process_id) } != 0,
            "identify local terminal daemon: {}", io::Error::last_os_error());
        // SAFETY: process_id came from the connected pipe, and the returned handle is owned.
        let process = unsafe { OpenProcess(PROCESS_QUERY_LIMITED_INFORMATION, 0, process_id) };
        ensure!(!process.is_null(), "open local terminal daemon process: {}", io::Error::last_os_error());
        // SAFETY: successful OpenProcess returns a new owned handle.
        let process = unsafe { OwnedHandle::from_raw_handle(process) };
        ensure!(process_user(process.as_raw_handle())? == user,
            "local terminal daemon is not owned by the current user");
        Ok(LocalStream::Client(client))
    }

    pub enum LocalStream {
        Client(NamedPipeClient),
        Server(NamedPipeServer),
    }

    impl AsyncRead for LocalStream {
        fn poll_read(self: Pin<&mut Self>, cx: &mut TaskContext<'_>, buf: &mut ReadBuf<'_>) -> Poll<io::Result<()>> {
            match self.get_mut() {
                Self::Client(stream) => Pin::new(stream).poll_read(cx, buf),
                Self::Server(stream) => Pin::new(stream).poll_read(cx, buf),
            }
        }
    }
    impl AsyncWrite for LocalStream {
        fn poll_write(self: Pin<&mut Self>, cx: &mut TaskContext<'_>, buf: &[u8]) -> Poll<io::Result<usize>> {
            match self.get_mut() {
                Self::Client(stream) => Pin::new(stream).poll_write(cx, buf),
                Self::Server(stream) => Pin::new(stream).poll_write(cx, buf),
            }
        }
        fn poll_flush(self: Pin<&mut Self>, cx: &mut TaskContext<'_>) -> Poll<io::Result<()>> {
            match self.get_mut() {
                Self::Client(stream) => Pin::new(stream).poll_flush(cx),
                Self::Server(stream) => Pin::new(stream).poll_flush(cx),
            }
        }
        fn poll_shutdown(self: Pin<&mut Self>, cx: &mut TaskContext<'_>) -> Poll<io::Result<()>> {
            match self.get_mut() {
                Self::Client(stream) => Pin::new(stream).poll_shutdown(cx),
                Self::Server(stream) => Pin::new(stream).poll_shutdown(cx),
            }
        }
    }
}

#[cfg(not(test))]
pub use platform::{Listener, LocalStream, bind, connect};
