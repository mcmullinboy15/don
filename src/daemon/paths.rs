//! Filesystem layout for the system-wide don daemon.
//!
//! Everything don writes for a *project* lives under that project's `.don/`.
//! The daemon is the one documented exception: it is system-wide by
//! definition, so it has nowhere project-local to put its socket and its
//! registry of running projects. Those live under a single directory
//! resolved here.
//!
//! Resolution order (first match wins):
//!
//! 1. `DON_STATE_DIR` — explicit override, used by tests and by anyone who
//!    wants the daemon's footprint somewhere specific.
//! 2. `XDG_STATE_HOME/don` — honoured on every platform when the variable is
//!    set to an absolute path, because a user who sets it means it.
//! 3. `~/Library/Application Support/don` on macOS,
//!    `~/.local/state/don` everywhere else.

use std::ffi::OsString;
use std::path::{Path, PathBuf};

/// Longest socket path we'll accept before failing with a useful message.
///
/// `sockaddr_un.sun_path` is 108 bytes on Linux and 104 on macOS, including
/// the NUL terminator. Binding a longer path fails deep inside `bind(2)` with
/// an errno that tells the user nothing, so we check up front and point them
/// at `DON_STATE_DIR`.
const SOCKET_PATH_MAX: usize = 100;

/// Errors resolving the daemon state directory.
#[derive(Debug, thiserror::Error)]
pub enum PathError {
    /// Neither an override nor a home directory was available.
    #[error(
        "cannot determine where to put don daemon state: \
         $HOME is not set and neither $DON_STATE_DIR nor $XDG_STATE_HOME is an absolute path — \
         set DON_STATE_DIR to a writable directory"
    )]
    NoHome,
    /// The resolved socket path is too long for `sockaddr_un`.
    #[error(
        "daemon socket path '{}' is {len} bytes, over the {max}-byte limit for unix sockets — \
         set DON_STATE_DIR to a shorter path (e.g. /tmp/don)",
        path.display()
    )]
    SocketPathTooLong {
        path: PathBuf,
        len: usize,
        max: usize,
    },
    /// Creating the state directory failed.
    #[error("failed to create don daemon state directory '{}': {source}", path.display())]
    CreateDir {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },
}

/// The environment inputs that decide where daemon state lives.
///
/// Captured as a plain struct rather than read from the process environment
/// inline so the resolution rules can be tested exhaustively — `set_var` is
/// `unsafe` in edition 2024 and racy across parallel tests either way.
#[derive(Debug, Clone, Default)]
pub struct DaemonEnv {
    /// `$DON_STATE_DIR`
    pub don_state_dir: Option<OsString>,
    /// `$XDG_STATE_HOME`
    pub xdg_state_home: Option<OsString>,
    /// `$HOME`
    pub home: Option<OsString>,
    /// Whether the platform default should be macOS-flavoured.
    pub macos: bool,
}

impl DaemonEnv {
    /// Capture the current process environment.
    pub fn from_process() -> Self {
        Self {
            don_state_dir: std::env::var_os("DON_STATE_DIR"),
            xdg_state_home: std::env::var_os("XDG_STATE_HOME"),
            home: std::env::var_os("HOME"),
            macos: cfg!(target_os = "macos"),
        }
    }
}

/// Resolved locations of every file the daemon owns.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DaemonPaths {
    root: PathBuf,
}

impl DaemonPaths {
    /// Resolve the daemon state directory from the process environment.
    pub fn from_process_env() -> Result<Self, PathError> {
        Self::resolve(&DaemonEnv::from_process())
    }

    /// Resolve the daemon state directory from explicit environment inputs.
    pub fn resolve(env: &DaemonEnv) -> Result<Self, PathError> {
        let paths = Self {
            root: resolve_root(env)?,
        };
        let socket = paths.socket();
        let len = socket.as_os_str().len();
        if len > SOCKET_PATH_MAX {
            return Err(PathError::SocketPathTooLong {
                path: socket,
                len,
                max: SOCKET_PATH_MAX,
            });
        }
        Ok(paths)
    }

    /// Use an explicit directory, bypassing environment resolution.
    ///
    /// Intended for tests and for callers that already know the location
    /// (for example a service unit that pins `DON_STATE_DIR`).
    pub fn with_root(root: PathBuf) -> Self {
        Self { root }
    }

    /// The state directory itself.
    pub fn root(&self) -> &Path {
        &self.root
    }

    /// Unix socket the daemon listens on for control requests (register,
    /// deregister, list). Chmod 0600 — same posture as a project's
    /// `.don/don.sock`.
    pub fn socket(&self) -> PathBuf {
        self.root.join("daemon.sock")
    }

    /// Flock'd PID file guarding against two daemons running at once.
    pub fn pid_file(&self) -> PathBuf {
        self.root.join("daemon.pid")
    }

    /// On-disk copy of the project registry, so a restarted daemon can pick
    /// up stacks that were already running.
    pub fn registry(&self) -> PathBuf {
        self.root.join("registry.json")
    }

    /// Directory for daemon logs.
    pub fn log_dir(&self) -> PathBuf {
        self.root.join("logs")
    }

    /// Daemon's own log file, used when it runs under systemd/launchd.
    pub fn log_file(&self) -> PathBuf {
        self.log_dir().join("daemon.log")
    }

    /// Create the state directory (and the log directory) if missing.
    pub fn ensure(&self) -> Result<(), PathError> {
        for dir in [self.root.clone(), self.log_dir()] {
            std::fs::create_dir_all(&dir).map_err(|source| PathError::CreateDir {
                path: dir,
                source,
            })?;
        }
        Ok(())
    }
}

/// Apply the documented resolution order. Relative paths are ignored rather
/// than resolved against the cwd — a daemon's state location must not depend
/// on where it happened to be launched from.
fn resolve_root(env: &DaemonEnv) -> Result<PathBuf, PathError> {
    if let Some(dir) = absolute(env.don_state_dir.as_ref()) {
        return Ok(dir);
    }
    if let Some(dir) = absolute(env.xdg_state_home.as_ref()) {
        return Ok(dir.join("don"));
    }
    let home = absolute(env.home.as_ref()).ok_or(PathError::NoHome)?;
    if env.macos {
        Ok(home.join("Library").join("Application Support").join("don"))
    } else {
        Ok(home.join(".local").join("state").join("don"))
    }
}

/// Interpret an env var as an absolute path, discarding empty and relative values.
fn absolute(value: Option<&OsString>) -> Option<PathBuf> {
    let path = PathBuf::from(value?);
    (path.is_absolute()).then_some(path)
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;

    fn env(
        don_state_dir: Option<&str>,
        xdg_state_home: Option<&str>,
        home: Option<&str>,
        macos: bool,
    ) -> DaemonEnv {
        DaemonEnv {
            don_state_dir: don_state_dir.map(OsString::from),
            xdg_state_home: xdg_state_home.map(OsString::from),
            home: home.map(OsString::from),
            macos,
        }
    }

    #[test]
    fn resolves_root_by_precedence() {
        struct Case {
            name: &'static str,
            env: DaemonEnv,
            expected: Option<&'static str>,
        }

        let cases = vec![
            Case {
                name: "DON_STATE_DIR wins over everything",
                env: env(Some("/srv/don"), Some("/xdg"), Some("/home/u"), false),
                expected: Some("/srv/don"),
            },
            Case {
                name: "DON_STATE_DIR wins on macOS too",
                env: env(Some("/srv/don"), None, Some("/Users/u"), true),
                expected: Some("/srv/don"),
            },
            Case {
                name: "XDG_STATE_HOME gets a 'don' suffix",
                env: env(None, Some("/xdg"), Some("/home/u"), false),
                expected: Some("/xdg/don"),
            },
            Case {
                name: "explicit XDG_STATE_HOME is honoured on macOS",
                env: env(None, Some("/xdg"), Some("/Users/u"), true),
                expected: Some("/xdg/don"),
            },
            Case {
                name: "linux default is ~/.local/state/don",
                env: env(None, None, Some("/home/u"), false),
                expected: Some("/home/u/.local/state/don"),
            },
            Case {
                name: "macos default is ~/Library/Application Support/don",
                env: env(None, None, Some("/Users/u"), true),
                expected: Some("/Users/u/Library/Application Support/don"),
            },
            Case {
                name: "relative DON_STATE_DIR is ignored",
                env: env(Some("relative/dir"), None, Some("/home/u"), false),
                expected: Some("/home/u/.local/state/don"),
            },
            Case {
                name: "relative XDG_STATE_HOME is ignored",
                env: env(None, Some("relative"), Some("/home/u"), false),
                expected: Some("/home/u/.local/state/don"),
            },
            Case {
                name: "empty values are ignored",
                env: env(Some(""), Some(""), Some("/home/u"), false),
                expected: Some("/home/u/.local/state/don"),
            },
            Case {
                name: "no home and no override is an error",
                env: env(None, None, None, false),
                expected: None,
            },
            Case {
                name: "relative home is not usable",
                env: env(None, None, Some("home"), true),
                expected: None,
            },
        ];

        for case in cases {
            let actual = resolve_root(&case.env);
            match case.expected {
                Some(expected) => assert_eq!(
                    actual.unwrap(),
                    PathBuf::from(expected),
                    "case: {}",
                    case.name
                ),
                None => assert!(
                    matches!(actual, Err(PathError::NoHome)),
                    "case: {} — expected NoHome, got {actual:?}",
                    case.name
                ),
            }
        }
    }

    #[test]
    fn file_names_hang_off_the_root() {
        let paths = DaemonPaths::with_root(PathBuf::from("/state/don"));
        assert_eq!(paths.root(), Path::new("/state/don"));
        assert_eq!(paths.socket(), PathBuf::from("/state/don/daemon.sock"));
        assert_eq!(paths.pid_file(), PathBuf::from("/state/don/daemon.pid"));
        assert_eq!(paths.registry(), PathBuf::from("/state/don/registry.json"));
        assert_eq!(paths.log_dir(), PathBuf::from("/state/don/logs"));
        assert_eq!(paths.log_file(), PathBuf::from("/state/don/logs/daemon.log"));
    }

    #[test]
    fn rejects_socket_paths_too_long_to_bind() {
        let long = format!("/{}", "d".repeat(SOCKET_PATH_MAX));
        let result = DaemonPaths::resolve(&env(Some(&long), None, None, false));
        assert!(
            matches!(result, Err(PathError::SocketPathTooLong { .. })),
            "expected SocketPathTooLong, got {result:?}"
        );

        // A root just short enough that appending "/daemon.sock" still fits.
        let ok = format!("/{}", "d".repeat(SOCKET_PATH_MAX - "/daemon.sock".len() - 1));
        assert!(DaemonPaths::resolve(&env(Some(&ok), None, None, false)).is_ok());
    }

    #[test]
    fn ensure_creates_root_and_log_dir() {
        let tmp = tempfile::tempdir().unwrap();
        let paths = DaemonPaths::with_root(tmp.path().join("nested").join("don"));
        paths.ensure().unwrap();
        assert!(paths.root().is_dir());
        assert!(paths.log_dir().is_dir());
        // Idempotent.
        paths.ensure().unwrap();
    }
}
