//! Fd placement for the systemd `LISTEN_FDS` protocol.
//!
//! The owning `TcpListener`s live in each service's [`crate::proxy::ServiceProxy`].
//! At spawn time, the runner collects their raw fds and passes them here so
//! the `pre_exec` hook can move them to fd 3, 4, … in the child and clear
//! `CLOEXEC` so they survive `exec`.
//!
//! `LISTEN_FDS` / `LISTEN_FDNAMES` are provided via the child's environment;
//! `LISTEN_PID` is set by the `sh`-wrapper emitted by
//! [`crate::sys::listen_pid_shim`] (setenv from `pre_exec` doesn't
//! survive `execve` with an explicit envp).

use std::os::unix::io::RawFd;

/// The first fd number for passed sockets (per systemd convention).
const SD_LISTEN_FDS_START: i32 = 3;

/// Hard cap for socket activation fds. This keeps fd placement
/// allocation-free in the child-side `pre_exec` hook.
const MAX_LISTEN_FDS: usize = 128;

/// Place file descriptors at fd 3, 4, 5... for LISTEN_FDS.
///
/// Must only be called in a `pre_exec` hook (async-signal-safe context).
/// Uses a two-pass approach to avoid fd collisions: first dup all source fds
/// to high temporary fds, then dup2 from temps to the target positions.
///
/// Also clears `FD_CLOEXEC` on each target fd so they survive `exec`.
///
/// # Safety
///
/// This function calls `dup`, `dup2`, `fcntl`, and `close` which are
/// async-signal-safe per POSIX. It must only be called between `fork`
/// and `exec` (i.e., inside a `pre_exec` hook).
pub(crate) fn place_fds_for_exec(source_fds: &[RawFd]) -> std::io::Result<()> {
    if source_fds.is_empty() {
        return Ok(());
    }

    if source_fds.len() > MAX_LISTEN_FDS {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            "too many listen fds",
        ));
    }

    let mut temp_fds = [-1; MAX_LISTEN_FDS];
    let temp_start = SD_LISTEN_FDS_START + source_fds.len() as i32;

    // Pass 1: dup source fds to high temp fds to avoid collisions.
    // (A source fd might be 3 or 4, which we need as target positions.)
    for (i, &src) in source_fds.iter().enumerate() {
        let temp = unsafe { libc::fcntl(src, libc::F_DUPFD, temp_start) };
        if temp < 0 {
            close_temp_fds(&temp_fds[..i]);
            return Err(std::io::Error::last_os_error());
        }
        temp_fds[i] = temp;
    }

    // Pass 2: dup2 temp fds to target positions (3, 4, 5...) and clear CLOEXEC.
    for (i, &temp) in temp_fds[..source_fds.len()].iter().enumerate() {
        let target = SD_LISTEN_FDS_START + i as i32;
        let result = unsafe { libc::dup2(temp, target) };
        if result < 0 {
            close_temp_fds(&temp_fds[..source_fds.len()]);
            return Err(std::io::Error::last_os_error());
        }
        if let Err(err) = clear_cloexec(target) {
            close_temp_fds(&temp_fds[..source_fds.len()]);
            return Err(err);
        }
    }

    close_temp_fds(&temp_fds[..source_fds.len()]);
    Ok(())
}

fn close_temp_fds(fds: &[RawFd]) {
    for &fd in fds {
        if fd >= 0 {
            let _ = unsafe { libc::close(fd) };
        }
    }
}

/// Clear the FD_CLOEXEC flag on a file descriptor.
fn clear_cloexec(fd: RawFd) -> std::io::Result<()> {
    let flags = unsafe { libc::fcntl(fd, libc::F_GETFD) };
    if flags < 0 {
        return Err(std::io::Error::last_os_error());
    }

    let result = unsafe { libc::fcntl(fd, libc::F_SETFD, flags & !libc::FD_CLOEXEC) };
    if result < 0 {
        return Err(std::io::Error::last_os_error());
    }
    Ok(())
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
mod tests {
    use super::*;

    // `place_fds_for_exec` with real fds manipulates fd 3+ globally and
    // would race with other tests. The empty case is safe to check.
    #[test]
    fn test_place_fds_empty() {
        place_fds_for_exec(&[]).unwrap();
    }
}
