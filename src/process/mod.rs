//! Process management — spawning, signaling, and lifecycle management
//! for child processes in their own process groups.

pub mod env;
pub mod pid_file;
#[cfg(test)]
pub(crate) mod test_util;

use nix::sys::signal::{killpg, Signal};
use nix::unistd::Pid;
use pid_file::{PidFile, PidFileError};
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
        }
    }
}

/// A handle to a spawned child process in its own process group.
///
/// Holds the child process, its output stream, and optionally a PID file lock.
/// The PID file lock is released when this handle is dropped.
pub struct ProcessHandle {
    /// The process group ID. Equal to the child's PID since we use setpgid/setsid.
    pgid: i32,
    /// The async-readable output stream (PTY or pipe). Taken once via `take_output()`.
    output: Option<ChildOutput>,
    /// The child process (for waiting on exit).
    child: tokio::process::Child,
    /// The PTY write half, if PTY mode. Used for interactive attach (Phase 17).
    pty_write: Option<pty_process::OwnedWritePty>,
    /// The PID file lock. Held for the process lifetime, released on drop.
    _pid_file: Option<PidFile>,
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
    /// Path for the PID file. None = no PID file (e.g., for tasks).
    pub pid_file_path: Option<PathBuf>,
    /// Force pipe-based spawning instead of PTY (for testing fallback).
    pub force_pipe: bool,
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
    /// PID file error.
    #[error("pid file error: {0}")]
    PidFile(#[from] PidFileError),
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

    /// Take the output stream. Can only be called once.
    pub fn take_output(&mut self) -> Option<ChildOutput> {
        self.output.take()
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
            && !matches!(e, ProcessError::Signal { source: nix::Error::ESRCH, .. })
        {
            return Err(e);
        }

        // Wait with timeout
        match tokio::time::timeout(timeout, self.child.wait()).await {
            Ok(result) => result.map_err(ProcessError::Io),
            Err(_elapsed) => {
                // Timeout — escalate to SIGKILL
                if let Err(e) = self.signal(Signal::SIGKILL)
                    && !matches!(e, ProcessError::Signal { source: nix::Error::ESRCH, .. })
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

/// Spawn a child process in its own process group.
///
/// 1. If `pid_file_path` is set, acquires an flock BEFORE spawning.
/// 2. Tries PTY allocation first (for terminal-like behavior).
/// 3. Falls back to pipe-based spawning if PTY fails or `force_pipe` is set.
/// 4. In PTY mode, the child gets its own session via `setsid()` (handled by pty-process).
///    In pipe mode, the child gets its own process group via `setpgid(0, 0)`.
pub fn spawn_process(config: SpawnConfig<'_>) -> Result<ProcessHandle, ProcessError> {
    // Acquire PID file lock before spawning (if requested).
    // We write a placeholder PGID of 0, then update after spawn.
    let pid_file = config
        .pid_file_path
        .as_ref()
        .map(|path| PidFile::acquire(path.clone(), 0))
        .transpose()?;

    // Try PTY first, fall back to pipe.
    // If spawn fails and we acquired a PID file, it's dropped here
    // (releasing the lock). This is correct — no process to track.
    if !config.force_pipe {
        match spawn_pty(&config) {
            Ok((child, read_pty, write_pty)) => {
                let pgid = child_pgid(&child, config.cmd)?;
                let pid_file = maybe_update_pid_file(pid_file, pgid)?;
                Ok(ProcessHandle {
                    pgid,
                    output: Some(ChildOutput::Pty(read_pty)),
                    child,
                    pty_write: Some(write_pty),
                    _pid_file: pid_file,
                })
            }
            Err(_pty_err) => spawn_pipe_handle(&config, pid_file),
        }
    } else {
        spawn_pipe_handle(&config, pid_file)
    }
}

/// Build a ProcessHandle from a pipe-mode spawn.
fn spawn_pipe_handle(
    config: &SpawnConfig<'_>,
    pid_file: Option<PidFile>,
) -> Result<ProcessHandle, ProcessError> {
    let mut child = spawn_pipe(config)?;
    let pgid = child_pgid(&child, config.cmd)?;
    let pid_file = maybe_update_pid_file(pid_file, pgid)?;
    let stdout = child.stdout.take();

    Ok(ProcessHandle {
        pgid,
        output: stdout.map(ChildOutput::Pipe),
        child,
        pty_write: None,
        _pid_file: pid_file,
    })
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

    // Safety: setpgid and dup2 are async-signal-safe.
    // dup2(1, 2) merges stderr into stdout. This works because tokio has
    // already set up fd 1 as the pipe's write end before pre_exec runs.
    unsafe {
        cmd.pre_exec(|| {
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
        .ok_or_else(|| ProcessError::ChildExitedEarly { cmd: cmd.to_string() })
}

fn maybe_update_pid_file(
    pid_file: Option<PidFile>,
    pgid: i32,
) -> Result<Option<PidFile>, ProcessError> {
    match pid_file {
        Some(mut pf) => {
            pf.update_pgid(pgid)?;
            Ok(Some(pf))
        }
        None => Ok(None),
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
