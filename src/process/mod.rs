//! Process management — spawning, signaling, and lifecycle management
//! for child processes in their own process groups.

pub mod env;
pub mod pid_file;
pub(crate) mod socket;
#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
pub(crate) mod test_util;

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
    /// Empty means no socket passing. When non-empty, pipe mode is forced
    /// (pty-process doesn't expose pre_exec for fd placement).
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

    /// Take the PTY write half (for interactive attach in Phase 17).
    pub fn take_pty_write(&mut self) -> Option<pty_process::OwnedWritePty> {
        self.pty_write.take()
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
    // Force pipe mode when passing listen fds — pty-process doesn't expose
    // pre_exec for fd placement. Network services don't need a PTY anyway.
    if !config.force_pipe && config.listen_fds.is_empty() {
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

            // Set LISTEN_PID to our (child's) PID. getpid() returns the
            // child's PID after fork, before exec.
            if !listen_fds.is_empty() {
                let pid = libc::getpid();
                let pid_str = format!("{pid}\0");
                libc::setenv(
                    c"LISTEN_PID".as_ptr(),
                    pid_str.as_ptr().cast(),
                    1,
                );
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

/// Write the PGID to a file. Creates parent directories if needed.
async fn write_pgid_file(path: Option<&Path>, pgid: i32) -> Result<(), ProcessError> {
    let Some(path) = path else { return Ok(()) };
    if let Some(parent) = path.parent() {
        tokio::fs::create_dir_all(parent)
            .await
            .map_err(ProcessError::Io)?;
    }
    tokio::fs::write(path, pgid.to_string())
        .await
        .map_err(|e| {
            ProcessError::PgidFile(format!("failed to write pgid to '{}': {e}", path.display()))
        })?;
    Ok(())
}

/// Read the PGID from a file. Returns `None` if the file does not exist.
pub async fn read_pgid_file(path: &Path) -> Result<Option<i32>, ProcessError> {
    match tokio::fs::read_to_string(path).await {
        Ok(content) => {
            let content = content.trim().to_string();
            if content.is_empty() {
                return Ok(None);
            }
            let pgid: i32 = content.parse().map_err(|_| {
                ProcessError::PgidFile(format!("invalid pgid in '{}': '{content}'", path.display()))
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

/// Delete a PGID file from disk. Idempotent — does not error if already gone.
pub async fn cleanup_pgid_file(path: &Path) -> Result<(), std::io::Error> {
    match tokio::fs::remove_file(path).await {
        Ok(()) => Ok(()),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(e) => Err(e),
    }
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
