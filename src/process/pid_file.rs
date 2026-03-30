//! Flock-based PID file locking for process management.
//!
//! Each service gets a PID file at `.don/pids/<name>` that stores the PGID
//! and holds an `flock` for the process lifetime. The lock provides a
//! definitive answer to "is this process still alive?" without worrying
//! about PID recycling.

use nix::errno::Errno;
use nix::fcntl::{Flock, FlockArg};
use std::fs::{File, OpenOptions};
use std::io::Write;
use std::os::unix::fs::OpenOptionsExt;
use std::path::{Path, PathBuf};

/// An flock-held PID file that stores a process group ID.
///
/// The `Flock<File>` is held for the lifetime of this struct.
/// When dropped, the lock is released automatically.
/// The file is NOT deleted on drop — stale detection relies on the
/// file existing after the process dies.
pub struct PidFile {
    _lock: Flock<File>,
    path: PathBuf,
    pgid: i32,
}

/// Errors from PID file operations.
#[derive(Debug, thiserror::Error)]
pub enum PidFileError {
    /// Failed to create parent directories.
    #[error("failed to create pid file directory '{}': {source}", path.display())]
    CreateDir {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },
    /// Failed to open or create the PID file.
    #[error("failed to open pid file '{}': {source}", path.display())]
    Open {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },
    /// Failed to acquire flock — another process holds it.
    #[error("lock held by another process (already running)")]
    AlreadyLocked,
    /// Failed to acquire flock for a non-contention reason.
    #[error("flock failed on '{}': {source}", path.display())]
    Flock {
        path: PathBuf,
        #[source]
        source: Errno,
    },
    /// Failed to write PGID to the file.
    #[error("failed to write pgid to '{}': {source}", path.display())]
    Write {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },
    /// Failed to read PGID from a stale file.
    #[error("failed to read pgid from '{}': {source}", path.display())]
    Read {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },
    /// PGID in the file was not a valid integer.
    #[error("invalid pgid in '{}': '{content}'", path.display())]
    InvalidPgid { path: PathBuf, content: String },
}

impl PidFile {
    /// Acquire an exclusive, non-blocking flock on the given path and write the PGID.
    ///
    /// 1. Creates parent directories if needed.
    /// 2. Opens the file with `O_CREAT | O_CLOEXEC`.
    /// 3. Attempts `flock(LOCK_EX | LOCK_NB)`.
    /// 4. If the lock fails with EWOULDBLOCK, returns `PidFileError::AlreadyLocked`.
    /// 5. Truncates and writes the PGID.
    ///
    /// The caller must keep the returned `PidFile` alive for the lifetime of the
    /// managed process. Dropping it releases the lock.
    pub fn acquire(path: PathBuf, pgid: i32) -> Result<Self, PidFileError> {
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent).map_err(|source| PidFileError::CreateDir {
                path: parent.to_path_buf(),
                source,
            })?;
        }

        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .custom_flags(nix::libc::O_CLOEXEC)
            .open(&path)
            .map_err(|source| PidFileError::Open {
                path: path.clone(),
                source,
            })?;

        let mut lock = match Flock::lock(file, FlockArg::LockExclusiveNonblock) {
            Ok(lock) => lock,
            Err((_file, Errno::EWOULDBLOCK)) => return Err(PidFileError::AlreadyLocked),
            Err((_file, errno)) => {
                return Err(PidFileError::Flock {
                    path,
                    source: errno,
                })
            }
        };

        // Truncate, write PGID, and fsync to ensure durability
        lock.set_len(0).map_err(|source| PidFileError::Write {
            path: path.clone(),
            source,
        })?;
        write!(*lock, "{pgid}").map_err(|source| PidFileError::Write {
            path: path.clone(),
            source,
        })?;
        lock.sync_all().map_err(|source| PidFileError::Write {
            path: path.clone(),
            source,
        })?;

        Ok(Self {
            _lock: lock,
            path,
            pgid,
        })
    }

    /// Update the PGID stored in this PID file (e.g., after spawn when the real PID is known).
    pub fn update_pgid(&mut self, pgid: i32) -> Result<(), PidFileError> {
        use std::io::Seek;
        self._lock.seek(std::io::SeekFrom::Start(0)).map_err(|source| PidFileError::Write {
            path: self.path.clone(),
            source,
        })?;
        self._lock.set_len(0).map_err(|source| PidFileError::Write {
            path: self.path.clone(),
            source,
        })?;
        write!(*self._lock, "{pgid}").map_err(|source| PidFileError::Write {
            path: self.path.clone(),
            source,
        })?;
        self._lock.sync_all().map_err(|source| PidFileError::Write {
            path: self.path.clone(),
            source,
        })?;
        self.pgid = pgid;
        Ok(())
    }

    /// The PGID stored in this PID file.
    pub fn pgid(&self) -> i32 {
        self.pgid
    }

    /// The path to the PID file.
    pub fn path(&self) -> &Path {
        &self.path
    }

    /// Try to acquire a lock on an existing PID file for stale detection.
    ///
    /// Returns:
    /// - `Ok(Some(pgid))` if the lock succeeded — the process is dead (stale).
    ///   The caller should `killpg(pgid)` and then call `cleanup()`.
    /// - `Ok(None)` if the lock failed with EWOULDBLOCK — the process is alive.
    /// - `Ok(None)` if the file does not exist.
    /// - `Err(...)` on unexpected errors.
    pub fn try_lock_stale(path: &Path) -> Result<Option<i32>, PidFileError> {
        let file = match OpenOptions::new()
            .read(true)
            .custom_flags(nix::libc::O_CLOEXEC)
            .open(path)
        {
            Ok(f) => f,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(None),
            Err(source) => {
                return Err(PidFileError::Open {
                    path: path.to_path_buf(),
                    source,
                })
            }
        };

        match Flock::lock(file, FlockArg::LockExclusiveNonblock) {
            Ok(mut lock) => {
                // Lock succeeded — process is dead. Read the PGID from the locked fd.
                use std::io::Read;
                let mut content = String::new();
                lock.read_to_string(&mut content).map_err(|source| PidFileError::Read {
                    path: path.to_path_buf(),
                    source,
                })?;
                let content = content.trim();
                if content.is_empty() {
                    return Ok(None);
                }
                let pgid: i32 = content.parse().map_err(|_| PidFileError::InvalidPgid {
                    path: path.to_path_buf(),
                    content: content.to_string(),
                })?;
                Ok(Some(pgid))
                // lock drops here, releasing the flock
            }
            Err((_file, Errno::EWOULDBLOCK)) => Ok(None),
            Err((_file, errno)) => Err(PidFileError::Flock {
                path: path.to_path_buf(),
                source: errno,
            }),
        }
    }

    /// Delete a PID file from disk. Idempotent — does not error if already gone.
    pub fn cleanup(path: &Path) -> Result<(), std::io::Error> {
        match std::fs::remove_file(path) {
            Ok(()) => Ok(()),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(()),
            Err(e) => Err(e),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::process::test_util::TempDir;
    use std::fs;

    #[test]
    fn test_acquire_and_read_pgid() {
        let dir = TempDir::new("acquire");
        let path = dir.path().join("test.pid");

        let pf = PidFile::acquire(path.clone(), 12345).unwrap();
        assert_eq!(pf.pgid(), 12345);
        assert!(path.exists());

        let content = fs::read_to_string(&path).unwrap();
        assert_eq!(content, "12345");

        drop(pf);
    }

    #[test]
    fn test_acquire_twice_fails() {
        let dir = TempDir::new("acquire-twice");
        let path = dir.path().join("test.pid");

        let _pf1 = PidFile::acquire(path.clone(), 12345).unwrap();
        let result = PidFile::acquire(path, 99999);
        assert!(matches!(result, Err(PidFileError::AlreadyLocked)));
    }

    #[test]
    fn test_drop_releases_lock() {
        let dir = TempDir::new("drop-release");
        let path = dir.path().join("test.pid");

        let pf = PidFile::acquire(path.clone(), 12345).unwrap();
        drop(pf);

        let pf2 = PidFile::acquire(path, 67890).unwrap();
        assert_eq!(pf2.pgid(), 67890);
    }

    #[test]
    fn test_try_lock_stale_nonexistent() {
        let dir = TempDir::new("stale-nonexistent");
        let path = dir.path().join("nope.pid");

        let result = PidFile::try_lock_stale(&path).unwrap();
        assert_eq!(result, None);
    }

    #[test]
    fn test_try_lock_stale_unlocked() {
        let dir = TempDir::new("stale-unlocked");
        let path = dir.path().join("test.pid");

        let pf = PidFile::acquire(path.clone(), 42).unwrap();
        drop(pf);

        let result = PidFile::try_lock_stale(&path).unwrap();
        assert_eq!(result, Some(42));
    }

    #[test]
    fn test_try_lock_stale_while_held() {
        let dir = TempDir::new("stale-held");
        let path = dir.path().join("test.pid");

        let _pf = PidFile::acquire(path.clone(), 42).unwrap();

        let result = PidFile::try_lock_stale(&path).unwrap();
        assert_eq!(result, None);
    }

    #[test]
    fn test_cleanup_removes_file() {
        let dir = TempDir::new("cleanup");
        let path = dir.path().join("test.pid");

        fs::write(&path, "12345").unwrap();
        assert!(path.exists());

        PidFile::cleanup(&path).unwrap();
        assert!(!path.exists());
    }

    #[test]
    fn test_cleanup_idempotent() {
        let dir = TempDir::new("cleanup-idempotent");
        let path = dir.path().join("nope.pid");

        PidFile::cleanup(&path).unwrap();
    }

    #[test]
    fn test_creates_parent_dirs() {
        let dir = TempDir::new("parent-dirs");
        let path = dir.path().join("nested").join("dir").join("test.pid");

        let pf = PidFile::acquire(path.clone(), 42).unwrap();
        assert!(path.exists());
        drop(pf);
    }
}
