//! Socket binding and fd passing for the LISTEN_FDS protocol.
//!
//! Don binds TCP sockets declared in a service's `listen` config, holds them
//! across restarts, and passes them to child processes as inherited file
//! descriptors. The child receives `LISTEN_FDS`, `LISTEN_PID`, and
//! `LISTEN_FDNAMES` environment variables following the systemd socket
//! activation protocol.

use std::collections::HashMap;
use std::net::TcpListener;
use std::os::unix::io::{AsRawFd, RawFd};

/// The first fd number for passed sockets (per systemd convention).
const SD_LISTEN_FDS_START: i32 = 3;

/// Errors from socket operations.
#[derive(Debug, thiserror::Error)]
pub enum SocketError {
    #[error("failed to bind '{addr}': {source}")]
    Bind {
        addr: String,
        #[source]
        source: std::io::Error,
    },
}

/// A set of bound TCP listeners owned by don for a single service.
///
/// These outlive individual service restarts — don holds the sockets open
/// so the port is never released. During a restart, incoming connections
/// queue in the kernel's listen backlog until the new process starts accepting.
#[derive(Debug)]
pub(crate) struct BoundSockets {
    /// Listeners in declaration order: (address_string, listener).
    listeners: Vec<(String, TcpListener)>,
}

/// Bind TCP sockets for each address in `listen`.
///
/// Sets `SO_REUSEADDR` on each socket. Returns an error if any address
/// fails to bind (e.g., port already in use).
pub(crate) fn bind_sockets(listen: &[String]) -> Result<BoundSockets, SocketError> {
    let mut listeners = Vec::with_capacity(listen.len());
    for addr in listen {
        let listener = TcpListener::bind(addr).map_err(|e| SocketError::Bind {
            addr: addr.clone(),
            source: e,
        })?;
        // SO_REUSEADDR is set by default by std::net::TcpListener::bind on Unix.
        listeners.push((addr.clone(), listener));
    }
    Ok(BoundSockets { listeners })
}

impl BoundSockets {
    /// Number of bound sockets.
    pub(crate) fn len(&self) -> usize {
        self.listeners.len()
    }

    /// Compute the LISTEN_FDS and LISTEN_FDNAMES environment variables.
    ///
    /// `LISTEN_FDS` is the count of fds as a string.
    /// `LISTEN_FDNAMES` is the address strings joined by `:`.
    /// `LISTEN_PID` is NOT included here — it must be set in pre_exec
    /// where the child's PID is known.
    pub(crate) fn listen_env(&self) -> HashMap<String, String> {
        let mut env = HashMap::new();
        env.insert("LISTEN_FDS".to_string(), self.listeners.len().to_string());
        let names: Vec<&str> = self.listeners.iter().map(|(addr, _)| addr.as_str()).collect();
        env.insert("LISTEN_FDNAMES".to_string(), names.join(":"));
        env
    }

    /// Get the raw file descriptors in declaration order.
    ///
    /// These are the fds that need to be placed at fd 3, 4, 5... in the child.
    pub(crate) fn raw_fds(&self) -> Vec<RawFd> {
        self.listeners.iter().map(|(_, l)| l.as_raw_fd()).collect()
    }
}

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

    // Pass 1: dup source fds to high temp fds to avoid collisions.
    // (A source fd might be 3 or 4, which we need as target positions.)
    let mut temp_fds = Vec::with_capacity(source_fds.len());
    for &src in source_fds {
        // dup() gives us a new fd (kernel picks the lowest available).
        // Since we're about to use 3..3+N, dup should give us higher numbers.
        let temp = nix::unistd::dup(src).map_err(std::io::Error::other)?;
        temp_fds.push(temp);
    }

    // Pass 2: dup2 temp fds to target positions (3, 4, 5...) and clear CLOEXEC.
    for (i, &temp) in temp_fds.iter().enumerate() {
        let target = SD_LISTEN_FDS_START + i as i32;
        nix::unistd::dup2(temp, target).map_err(std::io::Error::other)?;
        // Close the temp fd (dup2 doesn't close the source).
        let _ = nix::unistd::close(temp);

        // Clear FD_CLOEXEC so the fd survives exec.
        clear_cloexec(target)?;
    }

    Ok(())
}

/// Clear the FD_CLOEXEC flag on a file descriptor.
fn clear_cloexec(fd: RawFd) -> std::io::Result<()> {
    use nix::fcntl::{FdFlag, FcntlArg, fcntl};
    let flags = fcntl(fd, FcntlArg::F_GETFD).map_err(std::io::Error::other)?;
    let mut fd_flags = FdFlag::from_bits_truncate(flags);
    fd_flags.remove(FdFlag::FD_CLOEXEC);
    fcntl(fd, FcntlArg::F_SETFD(fd_flags)).map_err(std::io::Error::other)?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_bind_sockets() {
        struct Case {
            name: &'static str,
            addrs: Vec<&'static str>,
            expect_ok: bool,
        }

        let cases = vec![
            Case {
                name: "single address",
                addrs: vec!["127.0.0.1:0"],
                expect_ok: true,
            },
            Case {
                name: "multiple addresses",
                addrs: vec!["127.0.0.1:0", "127.0.0.1:0"],
                expect_ok: true,
            },
            Case {
                name: "invalid address",
                addrs: vec!["not-an-address"],
                expect_ok: false,
            },
        ];

        for case in cases {
            let addrs: Vec<String> = case.addrs.iter().map(|s| s.to_string()).collect();
            let result = bind_sockets(&addrs);
            if case.expect_ok {
                let sockets = result.unwrap_or_else(|e| panic!("{}: {e}", case.name));
                assert_eq!(sockets.len(), case.addrs.len(), "{}", case.name);
                assert!(sockets.len() > 0, "{}", case.name);
            } else {
                assert!(result.is_err(), "{}: expected error", case.name);
            }
        }
    }

    #[test]
    fn test_listen_env() {
        struct Case {
            name: &'static str,
            addrs: Vec<&'static str>,
            expected_fds: &'static str,
            expected_names: &'static str,
        }

        let cases = vec![
            Case {
                name: "single socket",
                addrs: vec!["127.0.0.1:0"],
                expected_fds: "1",
                expected_names: "127.0.0.1:0",
            },
            Case {
                name: "two sockets",
                addrs: vec!["127.0.0.1:0", "127.0.0.1:0"],
                expected_fds: "2",
                expected_names: "127.0.0.1:0:127.0.0.1:0",
            },
            Case {
                name: "three sockets",
                addrs: vec!["127.0.0.1:0", "127.0.0.1:0", "127.0.0.1:0"],
                expected_fds: "3",
                expected_names: "127.0.0.1:0:127.0.0.1:0:127.0.0.1:0",
            },
        ];

        for case in cases {
            let addrs: Vec<String> = case.addrs.iter().map(|s| s.to_string()).collect();
            let sockets = bind_sockets(&addrs).unwrap();
            let env = sockets.listen_env();
            assert_eq!(
                env.get("LISTEN_FDS").map(|s| s.as_str()),
                Some(case.expected_fds),
                "{}: LISTEN_FDS",
                case.name
            );
            assert_eq!(
                env.get("LISTEN_FDNAMES").map(|s| s.as_str()),
                Some(case.expected_names),
                "{}: LISTEN_FDNAMES",
                case.name
            );
            // LISTEN_PID should NOT be set by listen_env
            assert!(
                !env.contains_key("LISTEN_PID"),
                "{}: LISTEN_PID should not be set",
                case.name
            );
        }
    }

    #[test]
    fn test_raw_fds_returns_valid_fds() {
        let sockets = bind_sockets(&["127.0.0.1:0".to_string(), "127.0.0.1:0".to_string()]).unwrap();
        let fds = sockets.raw_fds();
        assert_eq!(fds.len(), 2);
        // All fds should be positive (valid)
        for fd in &fds {
            assert!(*fd > 0, "expected positive fd, got {fd}");
        }
        // Fds should be distinct
        assert_ne!(fds[0], fds[1], "expected distinct fds");
    }

    // Note: place_fds_for_exec is not unit-tested because it manipulates
    // global fd state (fds 3, 4, 5...) which conflicts with other tests
    // running in parallel. It's covered by integration tests that spawn
    // real child processes with listen fds.

    #[test]
    fn test_place_fds_empty() {
        // No-op for empty list.
        place_fds_for_exec(&[]).unwrap();
    }

    #[test]
    fn test_bind_error_includes_address() {
        // Bind to an invalid address to trigger an error.
        let result = bind_sockets(&["999.999.999.999:80".to_string()]);
        let err = result.unwrap_err();
        let msg = err.to_string();
        assert!(
            msg.contains("999.999.999.999:80"),
            "error should include the address: {msg}"
        );
    }
}
