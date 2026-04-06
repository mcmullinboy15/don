//! Process management — spawning, signaling, and lifecycle management
//! for child processes in their own process groups.

pub mod cleanup;
pub mod env;
pub mod identity;
pub mod pid_file;
pub(crate) mod socket;
#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
pub(crate) mod test_util;

pub use identity::ProcessIdentity;

use nix::sys::signal::{Signal, killpg};
use nix::unistd::Pid;
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::pin::Pin;
use std::process::ExitStatus;
use std::task::{Context, Poll};
use tokio::io::{AsyncRead, ReadBuf};

/// The output stream from a child process — either a PTY or piped stdout.
///
/// Both variants implement [`AsyncRead`], allowing the caller to read
/// output uniformly regardless of how the child was spawned.
pub enum ChildOutput {
    /// Output from a PTY-spawned process (master read half).
    Pty(pty_process::OwnedReadPty),
    /// Piped stdout from a non-PTY process (stderr merged via dup2).
    Pipe(tokio::process::ChildStdout),
    /// Log stream from a Docker container (via bollard).
    DockerLogs(crate::docker::stream::DockerLogReader),
}

impl AsyncRead for ChildOutput {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<std::io::Result<()>> {
        match self.get_mut() {
            ChildOutput::Pty(pty) => Pin::new(pty).poll_read(cx, buf),
            ChildOutput::Pipe(stdout) => Pin::new(stdout).poll_read(cx, buf),
            ChildOutput::DockerLogs(reader) => Pin::new(reader).poll_read(cx, buf),
        }
    }
}

/// A handle to a spawned child process in its own process group.
///
/// Holds the child process and optionally a PGID file path.
/// The PGID file is written on spawn and deleted when the handle is dropped.
pub struct ProcessHandle {
    /// The process group ID. Equal to the child's PID since we use setpgid/setsid.
    pgid: i32,
    /// The child process (for waiting on exit).
    child: tokio::process::Child,
    /// The PTY write half, if PTY mode. Used for interactive attach (Phase 17).
    pty_write: Option<pty_process::OwnedWritePty>,
    /// Path to the PGID file. Cleaned up on drop.
    pgid_file_path: Option<PathBuf>,
}

/// Configuration for spawning a process.
pub struct SpawnConfig<'a> {
    /// The executable to run.
    pub cmd: &'a str,
    /// Arguments to the executable.
    pub args: &'a [String],
    /// Working directory (None = inherit don's cwd).
    pub dir: Option<&'a Path>,
    /// Environment variables (complete set for the child).
    /// Must include PATH and other essentials — the child's env is fully replaced.
    pub env: HashMap<String, String>,
    /// Path for the PGID file. None = no PGID file (e.g., for tasks).
    pub pgid_file_path: Option<PathBuf>,
    /// Force pipe-based spawning instead of PTY (for testing fallback).
    pub force_pipe: bool,
    /// Raw fds to pass to the child at fd 3, 4, 5... (LISTEN_FDS protocol).
    /// Empty means no socket passing. Works in both PTY and pipe modes —
    /// fd placement happens in a pre_exec hook either way.
    pub listen_fds: Vec<std::os::unix::io::RawFd>,
}

/// Errors from process spawning and management.
#[derive(Debug, thiserror::Error)]
pub enum ProcessError {
    /// PTY allocation failed.
    #[error("failed to allocate PTY: {0}")]
    PtyAlloc(#[source] pty_process::Error),
    /// Failed to spawn child process.
    #[error("failed to spawn process '{cmd}': {source}")]
    Spawn {
        cmd: String,
        #[source]
        source: std::io::Error,
    },
    /// Child exited before we could read its PID.
    #[error("child process '{cmd}' exited immediately, could not determine PGID")]
    ChildExitedEarly { cmd: String },
    /// PGID file error.
    #[error("pgid file error: {0}")]
    PgidFile(String),
    /// Failed to send signal to process group.
    #[error("failed to send {signal} to pgid {pgid}: {source}")]
    Signal {
        pgid: i32,
        signal: &'static str,
        #[source]
        source: nix::Error,
    },
    /// Process did not exit even after SIGKILL (e.g., stuck in uninterruptible sleep).
    #[error("process pgid {pgid} did not exit after SIGKILL (possibly in uninterruptible sleep)")]
    Unkillable { pgid: i32 },
    /// I/O error during process management.
    #[error("process I/O error: {0}")]
    Io(#[from] std::io::Error),
}

impl ProcessHandle {
    /// The process group ID of this child.
    pub fn pgid(&self) -> i32 {
        self.pgid
    }

    /// Take the PTY write half for interactive attach.
    pub fn take_pty_write(&mut self) -> Option<pty_process::OwnedWritePty> {
        self.pty_write.take()
    }

    /// Return the PTY write half after an attach session ends.
    pub fn set_pty_write(&mut self, pty: pty_process::OwnedWritePty) {
        self.pty_write = Some(pty);
    }

    /// Wait for the child to exit, returning the exit status.
    pub async fn wait(&mut self) -> Result<ExitStatus, ProcessError> {
        self.child.wait().await.map_err(ProcessError::Io)
    }

    /// Send a signal to the entire process group.
    pub fn signal(&self, sig: Signal) -> Result<(), ProcessError> {
        killpg(Pid::from_raw(self.pgid), sig).map_err(|source| ProcessError::Signal {
            pgid: self.pgid,
            signal: signal_name(sig),
            source,
        })
    }

    /// Send a signal to the process group, wait up to `timeout` for exit,
    /// then send SIGKILL if the process hasn't exited.
    pub async fn terminate(
        &mut self,
        sig: Signal,
        timeout: std::time::Duration,
    ) -> Result<ExitStatus, ProcessError> {
        // Send the requested signal. Ignore ESRCH (process already gone).
        if let Err(e) = self.signal(sig)
            && !matches!(
                e,
                ProcessError::Signal {
                    source: nix::Error::ESRCH,
                    ..
                }
            )
        {
            return Err(e);
        }

        // Wait with timeout
        match tokio::time::timeout(timeout, self.child.wait()).await {
            Ok(result) => result.map_err(ProcessError::Io),
            Err(_elapsed) => {
                // Timeout — escalate to SIGKILL
                if let Err(e) = self.signal(Signal::SIGKILL)
                    && !matches!(
                        e,
                        ProcessError::Signal {
                            source: nix::Error::ESRCH,
                            ..
                        }
                    )
                {
                    return Err(e);
                }
                // Wait again with a generous timeout. SIGKILL is normally instant,
                // but a process in uninterruptible sleep (D state, e.g. stuck NFS)
                // cannot be killed and wait() would block forever.
                match tokio::time::timeout(std::time::Duration::from_millis(500), self.child.wait())
                    .await
                {
                    Ok(result) => result.map_err(ProcessError::Io),
                    Err(_) => Err(ProcessError::Unkillable { pgid: self.pgid }),
                }
            }
        }
    }
}

impl Drop for ProcessHandle {
    fn drop(&mut self) {
        if let Some(path) = self.pgid_file_path.take() {
            let _ = std::fs::remove_file(path);
        }
    }
}

/// Spawn a child process in its own process group.
///
/// 1. Tries PTY allocation first (for terminal-like behavior).
/// 2. Falls back to pipe-based spawning if PTY fails or `force_pipe` is set.
/// 3. In PTY mode, the child gets its own session via `setsid()` (handled by pty-process).
///    In pipe mode, the child gets its own process group via `setpgid(0, 0)`.
/// 4. If `pgid_file_path` is set, writes the PGID to the file after spawn.
pub async fn spawn_process(
    config: SpawnConfig<'_>,
) -> Result<(ProcessHandle, ChildOutput), ProcessError> {
    // Default to PTY for all services — this gives children a real TTY, which
    // flips libc stdio from block-buffered back to line-buffered, so logs from
    // Python/C/C++/Java network services appear as they're written rather than
    // stalling in a 4KB pipe buffer. PTY allocation can fail in headless/CI
    // environments, in which case we fall back to pipe mode.
    if !config.force_pipe {
        match spawn_pty(&config) {
            Ok((child, read_pty, write_pty)) => {
                let pgid = child_pgid(&child, config.cmd)?;
                write_pgid_file(config.pgid_file_path.as_deref(), pgid).await?;
                let output = ChildOutput::Pty(read_pty);
                let handle = ProcessHandle {
                    pgid,
                    child,
                    pty_write: Some(write_pty),
                    pgid_file_path: config.pgid_file_path,
                };
                Ok((handle, output))
            }
            Err(_pty_err) => spawn_pipe_handle(&config).await,
        }
    } else {
        spawn_pipe_handle(&config).await
    }
}

/// Build a ProcessHandle + ChildOutput from a pipe-mode spawn.
async fn spawn_pipe_handle(
    config: &SpawnConfig<'_>,
) -> Result<(ProcessHandle, ChildOutput), ProcessError> {
    let mut child = spawn_pipe(config)?;
    let pgid = child_pgid(&child, config.cmd)?;
    write_pgid_file(config.pgid_file_path.as_deref(), pgid).await?;
    let stdout = child.stdout.take().ok_or_else(|| ProcessError::Spawn {
        cmd: config.cmd.to_string(),
        source: std::io::Error::other("child process has no stdout"),
    })?;

    let output = ChildOutput::Pipe(stdout);
    let handle = ProcessHandle {
        pgid,
        child,
        pty_write: None,
        pgid_file_path: config.pgid_file_path.clone(),
    };
    Ok((handle, output))
}

fn spawn_pty(
    config: &SpawnConfig<'_>,
) -> Result<
    (
        tokio::process::Child,
        pty_process::OwnedReadPty,
        pty_process::OwnedWritePty,
    ),
    ProcessError,
> {
    let (pty, pts) = pty_process::open().map_err(ProcessError::PtyAlloc)?;

    // Set a reasonable default size so programs that query terminal
    // dimensions at startup don't stall on a 0x0 PTY.
    let _ = pty.resize(pty_process::Size::new(24, 80));

    let mut cmd = pty_process::Command::new(config.cmd);
    cmd = cmd.args(config.args);

    // Set environment: overlay merged env onto inherited env.
    // merge_env() starts from std::env::vars() so the full set is in config.env,
    // but we use envs() rather than env_clear() to be safe.
    cmd = cmd.envs(&config.env);

    if let Some(dir) = config.dir {
        cmd = cmd.current_dir(dir);
    }

    // Note: pty-process calls setsid() in its session_leader pre_exec hook,
    // which creates a new session AND process group (PGID = PID).
    // No additional setpgid needed — setsid handles it.
    //
    // If we're passing listener fds, register a pre_exec hook that runs after
    // session_leader (pty-process chains them in order). It places the fds at
    // 3, 4, 5... and sets LISTEN_PID to the child's own PID.
    if !config.listen_fds.is_empty() {
        let listen_fds = config.listen_fds.clone();
        // Safety: place_fds_for_exec calls dup/dup2/fcntl/close and setenv
        // is async-signal-safe on Linux/macOS. All operations happen between
        // fork and exec in the child process only.
        cmd = unsafe {
            cmd.pre_exec(move || {
                socket::place_fds_for_exec(&listen_fds)?;
                set_listen_pid_env();
                Ok(())
            })
        };
    }

    let child = cmd.spawn(pts).map_err(|e| ProcessError::Spawn {
        cmd: config.cmd.to_string(),
        source: std::io::Error::other(e),
    })?;

    let (read_pty, write_pty) = pty.into_split();
    Ok((child, read_pty, write_pty))
}

fn spawn_pipe(config: &SpawnConfig<'_>) -> Result<tokio::process::Child, ProcessError> {
    let mut cmd = tokio::process::Command::new(config.cmd);
    cmd.args(config.args);
    cmd.stdin(std::process::Stdio::null());
    cmd.stdout(std::process::Stdio::piped());

    // Overlay merged env onto inherited env.
    cmd.envs(&config.env);

    if let Some(dir) = config.dir {
        cmd.current_dir(dir);
    }

    // Clone listen fds for the pre_exec closure.
    let listen_fds = config.listen_fds.clone();

    // Safety: setpgid, dup2, dup, fcntl, and close are async-signal-safe.
    // dup2(1, 2) merges stderr into stdout. This works because tokio has
    // already set up fd 1 as the pipe's write end before pre_exec runs.
    unsafe {
        cmd.pre_exec(move || {
            // Place listen fds at fd 3, 4, 5... and clear CLOEXEC.
            socket::place_fds_for_exec(&listen_fds)?;

            // Set LISTEN_PID to the child's (our) PID after fork, before exec.
            if !listen_fds.is_empty() {
                set_listen_pid_env();
            }

            nix::unistd::setpgid(Pid::from_raw(0), Pid::from_raw(0))
                .map_err(std::io::Error::other)?;
            nix::unistd::dup2(1, 2).map_err(std::io::Error::other)?;
            Ok(())
        });
    }

    cmd.spawn().map_err(|source| ProcessError::Spawn {
        cmd: config.cmd.to_string(),
        source,
    })
}

/// Get the child's PGID from its PID. With setpgid(0,0) or setsid(), PGID == PID.
fn child_pgid(child: &tokio::process::Child, cmd: &str) -> Result<i32, ProcessError> {
    child
        .id()
        .map(|id| id as i32)
        .ok_or_else(|| ProcessError::ChildExitedEarly {
            cmd: cmd.to_string(),
        })
}

/// Write the PGID (and start_time if available) to a file. Creates parent
/// directories if needed. Format: `<pgid>\n<start_time>` or just `<pgid>`
/// if the child exited before we could capture its start_time.
async fn write_pgid_file(path: Option<&Path>, pgid: i32) -> Result<(), ProcessError> {
    let Some(path) = path else { return Ok(()) };
    if let Some(parent) = path.parent() {
        tokio::fs::create_dir_all(parent)
            .await
            .map_err(ProcessError::Io)?;
    }
    // Capture start_time synchronously — this reads /proc/<pgid>/stat (Linux)
    // or calls sysctl (macOS). If the child already exited (unlikely race),
    // we fall back to writing just the PGID.
    let content = match identity::capture(pgid) {
        Ok(Some(ident)) => format!("{}\n{}", ident.pgid, ident.start_time),
        _ => pgid.to_string(),
    };
    tokio::fs::write(path, content).await.map_err(|e| {
        ProcessError::PgidFile(format!("failed to write pgid to '{}': {e}", path.display()))
    })?;
    Ok(())
}

/// Read the PGID from a file. Returns `None` if the file does not exist.
pub async fn read_pgid_file(path: &Path) -> Result<Option<i32>, ProcessError> {
    match tokio::fs::read_to_string(path).await {
        Ok(content) => {
            let content = content.trim();
            if content.is_empty() {
                return Ok(None);
            }
            // First line is the PGID (second line, if present, is start_time).
            let first_line = content.lines().next().unwrap_or("").trim();
            let pgid: i32 = first_line.parse().map_err(|_| {
                ProcessError::PgidFile(format!("invalid pgid in '{}': '{first_line}'", path.display()))
            })?;
            Ok(Some(pgid))
        }
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(None),
        Err(e) => Err(ProcessError::PgidFile(format!(
            "failed to read pgid from '{}': {e}",
            path.display()
        ))),
    }
}

/// Read a pid file as a full `ProcessIdentity`. Returns `None` if the file
/// does not exist. If the file has the old single-line format, returns
/// `ProcessIdentity { pgid, start_time: 0 }`.
pub async fn read_pid_file_identity(
    path: &Path,
) -> Result<Option<ProcessIdentity>, ProcessError> {
    match tokio::fs::read_to_string(path).await {
        Ok(content) => {
            let content = content.trim();
            if content.is_empty() {
                return Ok(None);
            }
            let mut lines = content.lines();
            let pgid_str = lines.next().unwrap_or("");
            let pgid: i32 = pgid_str.trim().parse().map_err(|_| {
                ProcessError::PgidFile(format!(
                    "invalid pgid in '{}': '{pgid_str}'",
                    path.display()
                ))
            })?;
            let start_time: u64 = match lines.next() {
                Some(s) => s.trim().parse().unwrap_or_else(|_| {
                    eprintln!(
                        "[don] warning: invalid start_time in '{}', treating as unknown",
                        path.display()
                    );
                    0
                }),
                None => 0, // old format — no start_time line
            };
            Ok(Some(ProcessIdentity { pgid, start_time }))
        }
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(None),
        Err(e) => Err(ProcessError::PgidFile(format!(
            "failed to read identity from '{}': {e}",
            path.display()
        ))),
    }
}

/// Delete a PGID file from disk. Idempotent — does not error if already gone.
pub async fn cleanup_pgid_file(path: &Path) -> Result<(), std::io::Error> {
    match tokio::fs::remove_file(path).await {
        Ok(()) => Ok(()),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(e) => Err(e),
    }
}

/// Set the `LISTEN_PID` environment variable to the current process's PID.
///
/// Called from pre_exec hooks in the child after fork, before exec. The
/// value must be the reader's own PID — systemd's socket-activation protocol
/// requires it as a guard against fd inheritance leaking to nested children.
///
/// Uses a fixed-size stack buffer so there are no heap allocations after
/// fork (allocator locks from other tokio threads in the parent are frozen
/// post-fork and can deadlock the child).
///
/// # Safety
///
/// Must only be called between `fork` and `exec`. `libc::getpid` and
/// `libc::setenv` are async-signal-safe on Linux and macOS.
fn set_listen_pid_env() {
    // PID_MAX_LIMIT on Linux is 2^22 = 4194304 (7 digits); macOS caps at
    // 99999. 20 bytes holds any plausible PID plus sign plus NUL.
    let mut buf = [0u8; 20];
    let pid = unsafe { libc::getpid() };
    let len = write_i32_nul(&mut buf, pid);
    unsafe {
        libc::setenv(c"LISTEN_PID".as_ptr(), buf.as_ptr().cast(), 1);
    }
    // Silence unused warning — len is useful for tests/debugging, not for setenv.
    let _ = len;
}

/// Write a signed integer as a null-terminated ASCII string into `buf`.
/// Returns the number of bytes written (not counting the NUL). Panics (in
/// debug) if `buf` is too small.
fn write_i32_nul(buf: &mut [u8], value: i32) -> usize {
    debug_assert!(buf.len() >= 12, "buffer too small for i32");
    // Handle sign.
    let (mut n, negative) = if value < 0 {
        // -i32::MIN overflows; use wrapping_neg + cast to u32 for the magnitude.
        ((value as i64).unsigned_abs() as u32, true)
    } else {
        (value as u32, false)
    };
    // Write digits in reverse into a scratch area.
    let mut digits = [0u8; 10];
    let mut i = 0;
    if n == 0 {
        digits[0] = b'0';
        i = 1;
    } else {
        while n > 0 {
            digits[i] = b'0' + (n % 10) as u8;
            n /= 10;
            i += 1;
        }
    }
    // Emit into buf, prepending '-' if negative, reversed.
    let mut j = 0;
    if negative {
        buf[j] = b'-';
        j += 1;
    }
    while i > 0 {
        i -= 1;
        buf[j] = digits[i];
        j += 1;
    }
    buf[j] = 0;
    j
}

fn signal_name(sig: Signal) -> &'static str {
    match sig {
        Signal::SIGTERM => "SIGTERM",
        Signal::SIGKILL => "SIGKILL",
        Signal::SIGINT => "SIGINT",
        Signal::SIGQUIT => "SIGQUIT",
        Signal::SIGHUP => "SIGHUP",
        Signal::SIGUSR1 => "SIGUSR1",
        Signal::SIGUSR2 => "SIGUSR2",
        _ => "unknown",
    }
}
